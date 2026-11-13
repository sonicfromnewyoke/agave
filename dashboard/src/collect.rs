//! Samples slot state five times a second and publishes what changed. The
//! once-a-second readings run on their own thread in [`crate::meters`].

use {
    crate::{
        metrics_tap::{MetricsTap, ShredFill, SlotCost, StageTimes, WorkerSum},
        produced::{CostDetail, Execution, ProducedDetail, ProducedStore},
        search::LeaderMap,
        slots::{ShredArrival, SlotLevel},
    },
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_cluster_type::ClusterType,
    solana_pubkey::Pubkey,
    solana_rpc_client_types::request::DELINQUENT_VALIDATOR_SLOT_DISTANCE,
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet, VecDeque},
        hash::Hash,
        sync::{
            Arc, RwLock,
            atomic::{self, AtomicU64},
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

pub mod block_life;
pub mod cert_formation;
pub mod certificates;
pub mod cluster_charts;
pub mod earnings;
pub mod fee_payers;
pub mod finalization;
pub mod gossip_peers;
pub mod handoff;
pub mod skip_rate;
pub mod slot_clock;

use self::certificates::LastVotes;
pub(crate) use self::slot_clock::CATCH_UP_SLOTS_PER_SECOND;
pub use self::{certificates::MissReplies, handoff::LeaderFigures};

/// How long after the last request an on-demand reply is still gathered.
pub const WANTED_FOR_MILLIS: u64 = 30_000;

/// A reply encoded for the server to hold.
pub fn json_or_null<T: Serialize>(value: &T) -> Arc<str> {
    match serde_json::to_string(value) {
        Ok(json) => Arc::from(json),
        Err(err) => {
            log::error!("dashboard: failed to encode a reply: {err}");
            Arc::from("null")
        }
    }
}

/// The tip counts the slot still arriving, so a node in step reads a slot or two behind it.
pub const IN_STEP_SLOTS: u64 = 4;
/// Parents walked down from the tip before the rest of the distance is counted in slots.
pub const BEHIND_WALK_BLOCKS: u64 = 32;

/// Replies to requests, encoded by the slot feed and handed out by the server as they stand.
#[derive(Default)]
pub struct Replies {
    /// The miss and written lists, rebuilt every slow tick while asked for.
    pub misses: OnDemand<MissReplies>,
    /// Written by the slot feed; the server pages through it for the slot details page.
    pub produced: RwLock<ProducedStore>,
    /// For the waterfall and costs of a slot asked about.
    pub tap: Arc<MetricsTap>,
    pub gossip_peers: OnDemand<Encoded>,
    /// Every settled leader's handoff figures, by median, for the leaders in a bin asked about.
    pub handoff_leaders: RwLock<Vec<LeaderFigures>>,
    /// The Cluster page's charts.
    pub cluster_charts: OnDemand<Encoded>,
    /// The client, country and stake of staked nodes and scheduled leaders, for the schedule's leader
    /// conditions.
    pub leader_traits: RwLock<LeaderMap>,
}

/// A reply gathered only while someone asks for it, and the time of the last ask.
#[derive(Default)]
pub struct OnDemand<T> {
    pub held: RwLock<T>,
    /// Unix milliseconds.
    asked_at: AtomicU64,
}

impl<T> OnDemand<T> {
    /// Notes an ask; true where the one before it had lapsed.
    pub fn ask(&self, now_millis: u64) -> bool {
        let before = self.asked_at.swap(now_millis, atomic::Ordering::Relaxed);
        now_millis.saturating_sub(before) > WANTED_FOR_MILLIS
    }

    pub fn wanted(&self, now_millis: u64) -> bool {
        let asked_at = self.asked_at.load(atomic::Ordering::Relaxed);
        now_millis.saturating_sub(asked_at) <= WANTED_FOR_MILLIS
    }

    #[cfg(test)]
    pub fn asked_at(&self) -> u64 {
        self.asked_at.load(atomic::Ordering::Relaxed)
    }
}

/// An encoded reply, null until first gathered.
#[derive(Clone)]
pub struct Encoded(pub Arc<str>);

impl Default for Encoded {
    fn default() -> Self {
        Self(Arc::from("null"))
    }
}

impl Replies {
    /// The settled leaders whose median gap is at least `from` and under `to`, each end open
    /// where it is none.
    pub fn handoff_leaders_between(
        &self,
        from: Option<i32>,
        to: Option<i32>,
    ) -> Vec<LeaderFigures> {
        let Ok(leaders) = self.handoff_leaders.read() else {
            return Vec::new();
        };
        leaders
            .iter()
            .filter(|leader| from.is_none_or(|from| leader.p50 >= from))
            .filter(|leader| to.is_none_or(|to| leader.p50 < to))
            .cloned()
            .collect()
    }

    /// The blocks, turns, waterfalls and costs in `first..=last`, clamped as the store clamps.
    pub fn produced_detail(&self, first: Slot, last: Slot) -> Option<ProducedDetail> {
        let (blocks, turns, fees) = {
            let produced = self.produced.read().ok()?;
            let (blocks, turns) = produced.detail(first, last);
            let fees = (first == last).then(|| produced.fees(first)).flatten();
            (blocks, turns, fees)
        };
        // A turn's drawer sums the waterfalls of every slot in it.
        let from = turns.first().map_or(first, |turn| turn.first.min(first));
        let to = turns
            .iter()
            .map(|turn| turn.last)
            .chain(blocks.last().map(|block| block.slot))
            .max()
            .unwrap_or(first);
        let costs: Vec<SlotCost> = blocks
            .iter()
            .filter_map(|block| self.tap.cost(block.slot))
            .collect();
        let accounts: Vec<&str> = costs
            .iter()
            .map(|cost| cost.costliest_account.as_str())
            .collect();
        let recurrences = self.tap.costliest_recurrences(&accounts);
        let costs = costs
            .into_iter()
            .map(|cost| {
                let recurrence = recurrences.get(&cost.costliest_account).copied();
                CostDetail { cost, recurrence }
            })
            .collect();
        Some(ProducedDetail {
            waterfalls: self.tap.waterfalls_in(from, to),
            blocks,
            turns,
            costs,
            fees,
        })
    }
}

/// Only a connecting client reads the overview, a few hundred kilobytes.
pub const OVERVIEW_INTERVAL: Duration = Duration::from_secs(1);

pub const NANOS_PER_DAY: u128 = 86_400_000_000_000;

const MINUTE_MILLIS: u64 = 60_000;

pub const REPLAY_RATE_WINDOW: Duration = Duration::from_secs(30);

/// Matches the slot time beside it in the strip's head.
pub const FINALITY_WINDOW_MILLIS: u64 = 60_000;
const REPLAY_RATE_MIN_SPAN: Duration = Duration::from_secs(5);

pub const SLOT_OVERVIEW_LEN: usize = 512;

/// How far back a slot's duration looks for an earlier block that filled.
pub const FULL_REACH_SLOTS: u64 = 64;

/// A slot arriving this far behind the cluster's tip was caught up on, not received live.
pub const HANDOFF_BEHIND_SLOTS: u64 = 32;

/// How far back a leader's run of consecutive turns is followed to its first slot.
pub const HANDOFF_RUN_TURNS: usize = 8;

pub const SLOT_HISTORY: usize = 4096;

const MAX_VERSIONS_REPORTED: usize = 5;

pub const NEXT_LEADER_LOOKAHEAD: u64 = 20_000;

pub const UPCOMING_SLOTS: u64 = 32;

/// Sent to a connecting client; older blocks are asked for.
pub const PRODUCED_BLOCKS: usize = 500;

pub const WORKER_REPORT_MILLIS: u64 = 20;

pub const EXECUTION_WAIT_SLOTS: u64 = 64;

pub const LEADER_TURNS: usize = 128;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StakeSummary {
    /// In lamports.
    pub activated_stake: u64,
    pub total_stake: u64,
    /// In `[0, 1]`.
    pub share: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ValidatorCounts {
    /// Distinct staked identities this epoch, so a validator with several vote accounts counts
    /// once.
    pub total: usize,
    pub delinquent: usize,
    pub rpc_nodes: usize,
    pub non_delinquent_stake: u64,
    pub delinquent_stake: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VersionShare {
    pub version: Option<String>,
    pub validators: usize,
    pub stake: u64,
    /// The folded tail, which sorts beside a genuine no-version group.
    pub other: bool,
}

/// Only the leaders on screen, so the table is bounded by the page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Peer {
    pub identity: String,
    pub version: Option<String>,
    pub client: Option<String>,
    pub stake: u64,
    pub ip: Option<String>,
    pub country: Option<&'static str>,
    pub name: Option<String>,
    pub icon: Option<String>,
}

/// Where the cluster reaches this node, shown in the header.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct GossipAddress {
    pub ip: String,
    pub country: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EpochInfo {
    pub epoch: Epoch,
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub slots_in_epoch: u64,
    pub my_leader_slots: Vec<Slot>,

    /// In order of first turn.
    pub leaders: Vec<String>,
    /// One index into `leaders` per run of `NUM_CONSECUTIVE_LEADER_SLOTS` slots from `start_slot`.
    /// Empty, never partial, until the schedule is derived.
    pub turns: Vec<u16>,

    pub block_cost_limit: u64,
    pub account_cost_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Health {
    pub replay: ReplayHealth,
    pub vote: VoteHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayHealth {
    NotStarted,
    Running,
    Stalled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VoteHealth {
    NotVoting,
    NotStarted,
    Voting,
    Delinquent,
}

/// Alpenglow votes outside blocks, so several figures mean nothing under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Consensus {
    Tower,
    Alpenglow,
}

/// What voting costs: fees from the identity per vote under TowerBFT, the admission ticket from the
/// vote account each epoch under alpenglow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VoteCost {
    Fees {
        per_day: u64,
    },
    Ticket {
        lamports: u64,
        /// What the vote account must hold at the turn: rent exemption plus
        /// the ticket.
        minimum: u64,
        /// Rewards our vote account holds for its delegators, which it must keep on top of the minimum.
        pending: u64,
    },
}

/// This validator's vote credits in the epoch, against the most any staked validator has earned.
/// Under alpenglow the field holds lamports of reward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VoteCredits {
    pub epoch: Epoch,
    pub credits: u64,
    pub cluster_max: Option<u64>,
    /// Over the staked validators with credits this epoch.
    pub cluster_median: Option<u64>,
}

/// Whether this vote account holds a seat in alpenglow's admitted set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Admission {
    pub seat: bool,
    pub next_seat: Option<bool>,
    /// Lamports the vote account is short of the ticket for the epoch after
    /// the next, absent where it covers it or cannot be read.
    pub ticket_short: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SkipRate {
    pub epoch: Epoch,
    /// Share of this validator's leader slots that produced no block, over the part of the epoch
    /// the blockstore covers. `None` until the root has passed one.
    pub rate: Option<f64>,
}

/// The shared failed count until RPC's slot updates first give one.
pub const FAILED_UNREAD: u64 = u64::MAX;

pub fn measure_replay_rate(window: &VecDeque<(Instant, Slot)>) -> Option<f64> {
    let (oldest_at, oldest) = window.front()?;
    let (newest_at, newest) = window.back()?;
    let span = newest_at.duration_since(*oldest_at);
    if span < REPLAY_RATE_MIN_SPAN {
        return None;
    }
    Some(newest.saturating_sub(*oldest) as f64 / span.as_secs_f64())
}

pub fn classify_slot(slot: Slot, root: Slot, confirmed: Slot, finalized: Slot) -> SlotLevel {
    if slot <= finalized {
        SlotLevel::Finalized
    } else if slot <= root {
        SlotLevel::Rooted
    } else if slot <= confirmed {
        SlotLevel::OptimisticallyConfirmed
    } else {
        SlotLevel::Completed
    }
}

/// Keyed by identity: several staked vote accounts are one validator.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StakeTally {
    pub staked: HashMap<Pubkey, u64>,
    pub delinquent: HashSet<Pubkey>,
    /// The oldest last vote among each validator's staked accounts, `None` where one never voted.
    pub stalest_votes: LastVotes,
    pub delinquent_stake: u64,
    pub non_delinquent_stake: u64,
}

/// The cluster's rule: no vote yet, or the last one too far behind the tip.
pub fn is_delinquent(last_vote: Option<Slot>, tip: Slot) -> bool {
    last_vote
        .map(|vote| tip.saturating_sub(vote) >= DELINQUENT_VALIDATOR_SLOT_DISTANCE)
        .unwrap_or(true)
}

pub fn tally_stake(
    accounts: impl Iterator<Item = (Pubkey, u64, Option<Slot>)>,
    tip: Slot,
) -> StakeTally {
    let mut tally = StakeTally::default();
    for (identity, stake, last_vote) in accounts {
        // The bank holds every vote account ever created; a validator is one with stake this epoch.
        if stake == 0 {
            continue;
        }
        if is_delinquent(last_vote, tip) {
            tally.delinquent.insert(identity);
            tally.delinquent_stake = tally.delinquent_stake.saturating_add(stake);
        } else {
            tally.non_delinquent_stake = tally.non_delinquent_stake.saturating_add(stake);
        }
        let total = tally.staked.entry(identity).or_insert(0);
        *total = total.saturating_add(stake);
        // `None` orders first, so an account that never voted stays the stalest.
        let stalest = tally.stalest_votes.entry(identity).or_insert(last_vote);
        *stalest = (*stalest).min(last_vote);
    }
    tally
}

/// Looser than the cluster's threshold, since a brief lag on our own node is normal.
const VOTE_BEHIND_LIMIT: u64 = 150;

const REPLAY_STALL_AFTER: Duration = Duration::from_secs(12);

const VOTE_STALL_AFTER: Duration = Duration::from_secs(60);

/// The two durations sit at either end of the argument list because swapping them compiles.
pub fn assess_health(
    since_completed: Duration,
    completed_slot: Slot,
    voting: bool,
    vote_slot: Option<Slot>,
    behind: Option<u64>,
    since_vote_advance: Duration,
) -> Health {
    let replay = if since_completed > REPLAY_STALL_AFTER {
        ReplayHealth::Stalled
    } else if completed_slot == 0 {
        ReplayHealth::NotStarted
    } else {
        ReplayHealth::Running
    };

    // Checked first: a node that is not the voter has no votes of its own, and the rules below
    // would read the other machine's health.
    let vote = if !voting {
        VoteHealth::NotVoting
    } else {
        match (vote_slot, behind) {
            (None, _) => VoteHealth::NotStarted,
            (Some(_), Some(behind)) if behind > VOTE_BEHIND_LIMIT => VoteHealth::Delinquent,
            _ if since_vote_advance > VOTE_STALL_AFTER => VoteHealth::Delinquent,
            _ => VoteHealth::Voting,
        }
    };

    Health { replay, vote }
}

/// Counted over staked identities so the two cards add up.
pub fn version_shares(
    staked: &HashMap<Pubkey, u64>,
    versions: &HashMap<Pubkey, String>,
) -> Vec<VersionShare> {
    let releases = staked.iter().map(|(identity, stake)| {
        let release = versions
            .get(identity)
            .map(|version| strip_prerelease(version));
        (Some(release), *stake)
    });
    // Folded to keep the message a fixed size.
    let (rows, (folded, validators, stake)) =
        by_stake(releases, MAX_VERSIONS_REPORTED, |a, b| b.1.cmp(&a.1));
    rows.into_iter()
        .map(|row| (row, false))
        .chain((folded > 0).then_some(((None, validators, stake), true)))
        .map(|((version, validators, stake), other)| VersionShare {
            version: version.map(str::to_string),
            validators,
            stake,
            other,
        })
        .collect()
}

/// A key, its validators and their stake.
type Staked<K> = (K, usize, u64);

/// Validators and their stake by key, the largest stake first and `tie` ordering equal stakes,
/// cut to the first `shown`. The rest come back summed with those of no key, keyed by how many
/// keys they held.
fn by_stake<K: Eq + Hash>(
    stakes: impl IntoIterator<Item = (Option<K>, u64)>,
    shown: usize,
    tie: impl Fn(&Staked<K>, &Staked<K>) -> Ordering,
) -> (Vec<Staked<K>>, Staked<usize>) {
    let mut totals: HashMap<K, (usize, u64)> = HashMap::new();
    let mut unkeyed = (0usize, 0u64);
    for (key, stake) in stakes {
        let (validators, total) = match key {
            Some(key) => totals.entry(key).or_default(),
            None => &mut unkeyed,
        };
        *validators = validators.saturating_add(1);
        *total = total.saturating_add(stake);
    }
    let mut rows: Vec<Staked<K>> = totals
        .into_iter()
        .map(|(key, (validators, stake))| (key, validators, stake))
        .collect();
    rows.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| tie(a, b)));
    let tail = rows.split_off(shown.min(rows.len()));
    let (validators, stake) = tail.iter().fold(unkeyed, |(count, sum), row| {
        (count.saturating_add(row.1), sum.saturating_add(row.2))
    });
    (rows, (tail.len(), validators, stake))
}

/// The span of worker reports that are a block's own, end excluded. A report closes the time it
/// covers, so inside a turn each goes to the block holding its middle.
pub fn execution_window(
    start: u64,
    follows_ours: bool,
    next_start: Option<u64>,
    settled: u64,
) -> (u64, u64) {
    let middle = WORKER_REPORT_MILLIS / 2;
    let from = if follows_ours {
        start.saturating_add(middle)
    } else {
        start
    };
    let to = next_start.map_or(settled, |next| next.saturating_add(middle));
    (from, to)
}

pub fn build_execution(
    workers: Option<WorkerSum>,
    votes: Option<StageTimes>,
    window_millis: u64,
) -> Option<Execution> {
    if workers.is_none() && votes.is_none() {
        return None;
    }
    let workers = workers.unwrap_or_default();
    Some(Execution {
        non_vote: workers.times,
        workers: workers.workers,
        longest_batch: workers.longest_batch,
        votes,
        window_millis,
    })
}

impl From<ShredFill> for ShredArrival {
    fn from(fill: ShredFill) -> Self {
        Self {
            count: fill.shreds,
            repaired: fill.repaired,
            recovered: fill.recovered,
            full_millis: fill.full_millis,
        }
    }
}

/// As upstream names the cluster in its own output.
pub fn cluster_name(cluster_type: ClusterType) -> &'static str {
    match cluster_type {
        ClusterType::Testnet => "testnet",
        ClusterType::MainnetBeta => "mainnet-beta",
        ClusterType::Devnet => "devnet",
        ClusterType::Development => "development",
    }
}

/// A cluster mid-upgrade reports `4.2.0`, `4.2.0-rc.0` and `4.2.0-rc.1`, which are one release.
pub fn strip_prerelease(version: &str) -> &str {
    version.split(['-', '+']).next().unwrap_or(version)
}

pub fn system_time_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_reads_the_thresholds_most_settled_first() {
        for (slot, level) in [
            (80, SlotLevel::Finalized),
            (90, SlotLevel::Finalized),
            (95, SlotLevel::Rooted),
            (100, SlotLevel::Rooted),
            (105, SlotLevel::OptimisticallyConfirmed),
            (110, SlotLevel::OptimisticallyConfirmed),
            (120, SlotLevel::Completed),
        ] {
            assert_eq!(classify_slot(slot, 100, 110, 90), level, "{slot}");
        }
        // During startup the commitment cache trails the root bank, so `confirmed`
        // can sit below a rooted slot.
        assert_eq!(classify_slot(100, 100, 50, 0), SlotLevel::Rooted);
    }

    fn identity(seed: u8) -> Pubkey {
        Pubkey::new_from_array([seed; 32])
    }

    const TIP: Slot = 1_000;

    #[test]
    fn test_unstaked_vote_accounts_are_not_counted() {
        let tally = tally_stake(
            [(identity(1), 0, Some(TIP)), (identity(2), 100, Some(TIP))].into_iter(),
            TIP,
        );
        assert_eq!(tally.staked.len(), 1);
        assert_eq!(tally.non_delinquent_stake, 100);
    }

    #[test]
    fn test_two_vote_accounts_on_one_identity_are_one_validator() {
        let tally = tally_stake(
            [(identity(1), 100, Some(TIP)), (identity(1), 250, Some(TIP))].into_iter(),
            TIP,
        );
        assert_eq!(tally.staked.len(), 1, "one identity is one validator");
        assert_eq!(tally.staked[&identity(1)], 350, "its stake is the sum");
        // Counted per vote account, an identity with two delinquent accounts read total 1,
        // delinquent 2.
        let tally = tally_stake(
            [(identity(1), 100, Some(0)), (identity(1), 100, Some(0))].into_iter(),
            TIP,
        );
        assert_eq!((tally.staked.len(), tally.delinquent.len()), (1, 1));
    }

    #[test]
    fn test_a_vote_account_that_never_voted_is_delinquent() {
        let tally = tally_stake([(identity(1), 100, None)].into_iter(), TIP);
        assert_eq!(tally.delinquent.len(), 1);
        assert_eq!(tally.delinquent_stake, 100);
        assert_eq!(tally.non_delinquent_stake, 0);
    }

    #[test]
    fn test_delinquency_is_decided_at_the_threshold() {
        // As RPC's getVoteAccounts splits them.
        let at = TIP - DELINQUENT_VALIDATOR_SLOT_DISTANCE;
        assert!(
            tally_stake([(identity(1), 1, Some(at + 1))].into_iter(), TIP)
                .delinquent
                .is_empty(),
            "one slot inside the limit is still voting"
        );
        assert_eq!(
            tally_stake([(identity(1), 1, Some(at))].into_iter(), TIP)
                .delinquent
                .len(),
            1,
            "exactly at it is not"
        );
    }

    #[test]
    fn test_a_validator_is_as_stale_as_its_stalest_account() {
        let tally = tally_stake(
            [
                (identity(1), 100, Some(TIP)),
                (identity(1), 100, Some(40)),
                (identity(2), 100, Some(TIP)),
                (identity(2), 100, None),
                (identity(3), 100, Some(TIP)),
            ]
            .into_iter(),
            TIP,
        );
        assert_eq!(tally.stalest_votes[&identity(1)], Some(40));
        assert_eq!(
            tally.stalest_votes[&identity(2)],
            None,
            "never voting is the stalest"
        );
        assert_eq!(tally.stalest_votes[&identity(3)], Some(TIP));
        for (identity, last_vote) in &tally.stalest_votes {
            assert_eq!(
                is_delinquent(*last_vote, TIP),
                tally.delinquent.contains(identity),
                "the stalest vote decides as the tally does"
            );
        }
    }

    #[test]
    fn test_stake_splits_by_delinquency() {
        let tally = tally_stake(
            [
                (identity(1), 100, Some(TIP)),
                (identity(2), 30, Some(0)),
                (identity(3), 7, Some(TIP)),
            ]
            .into_iter(),
            TIP,
        );
        assert_eq!(tally.non_delinquent_stake, 107);
        assert_eq!(tally.delinquent_stake, 30);
    }

    #[test]
    fn test_blocks_in_a_turn_share_their_boundaries_and_the_last_keeps_its_tail() {
        let first = execution_window(1_000, false, Some(1_150), 1_190);
        let second = execution_window(1_150, true, Some(1_350), 1_390);
        let last = execution_window(1_350, true, None, 1_590);
        assert_eq!(first, (1_000, 1_160));
        assert_eq!(second, (1_160, 1_360));
        assert_eq!(last, (1_360, 1_590));
    }

    fn staked(entries: &[(u8, u64)]) -> HashMap<Pubkey, u64> {
        entries
            .iter()
            .map(|&(seed, stake)| (identity(seed), stake))
            .collect()
    }

    fn gossiped(entries: &[(u8, &str)]) -> HashMap<Pubkey, String> {
        entries
            .iter()
            .map(|&(seed, version)| (identity(seed), version.to_string()))
            .collect()
    }

    #[test]
    fn test_prerelease_tags_fold_into_one_release_row() {
        let shares = version_shares(
            &staked(&[(1, 10), (2, 10), (3, 10)]),
            &gossiped(&[(1, "4.2.0"), (2, "4.2.0-rc.0"), (3, "4.2.0-rc.1")]),
        );
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].version.as_deref(), Some("4.2.0"));
        assert_eq!(shares[0].validators, 3);
        assert_eq!(shares[0].stake, 30);
    }

    #[test]
    fn test_rows_are_ordered_by_stake_not_by_node_count() {
        let shares = version_shares(
            &staked(&[(1, 1_000), (2, 1), (3, 1), (4, 1)]),
            &gossiped(&[(1, "4.2.0"), (2, "4.1.0"), (3, "4.1.0"), (4, "4.1.0")]),
        );
        assert_eq!(shares[0].version.as_deref(), Some("4.2.0"));
        assert_eq!(shares[0].stake, 1_000);
        assert_eq!(shares[1].validators, 3, "more nodes, less stake, second");
    }

    #[test]
    fn test_unstaked_gossip_nodes_are_not_counted() {
        assert!(version_shares(&staked(&[]), &gossiped(&[(1, "4.2.0")])).is_empty());
        let staked = staked(&[(1, 30), (2, 20), (3, 10)]);
        let shares = version_shares(
            &staked,
            &gossiped(&[(1, "4.2.0"), (2, "4.3.0"), (7, "4.3.0"), (8, "4.1.0")]),
        );
        let counted: usize = shares.iter().map(|share| share.validators).sum();
        assert_eq!(counted, staked.len());
        assert_eq!(
            shares,
            version_shares(&staked, &gossiped(&[(1, "4.2.0"), (2, "4.3.0")]))
        );
    }

    #[test]
    fn test_a_staked_validator_gossip_cannot_see_reports_no_version() {
        let shares = version_shares(&staked(&[(9, 500)]), &gossiped(&[]));
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].version, None);
        assert_eq!(shares[0].stake, 500);
        assert!(
            !shares[0].other,
            "no version reported is not the folded tail"
        );
    }

    #[test]
    fn test_the_tail_folds_into_one_flagged_row() {
        let stakes = (1..=9).map(|seed| (identity(seed), u64::from(seed)));
        let versions = (1..=9).map(|seed| (identity(seed), format!("4.{seed}.0")));
        let shares = version_shares(&stakes.collect(), &versions.collect());

        assert_eq!(shares.len(), MAX_VERSIONS_REPORTED + 1);
        let last = shares.last().unwrap();
        assert!(last.other, "the fold is flagged rather than inferred");
        assert_eq!(last.validators, 9 - MAX_VERSIONS_REPORTED);
        assert!(
            shares[..MAX_VERSIONS_REPORTED].iter().all(|s| !s.other),
            "only the tail row is flagged"
        );
    }

    const FRESH: Duration = Duration::from_secs(1);

    #[test]
    fn test_replay_is_stalled_when_no_slot_completes() {
        assert_eq!(
            assess_health(Duration::from_secs(13), 100, true, Some(99), Some(1), FRESH).replay,
            ReplayHealth::Stalled
        );
        assert_eq!(
            assess_health(FRESH, 100, true, Some(99), Some(1), FRESH).replay,
            ReplayHealth::Running
        );
        assert_eq!(
            assess_health(FRESH, 100, false, None, None, FRESH).replay,
            ReplayHealth::Running,
            "whether or not this node votes"
        );
    }

    #[test]
    fn test_the_replay_rate_is_read_across_a_window_worth_reading() {
        let now = Instant::now();
        let at = |secs_ago: u64| now.checked_sub(Duration::from_secs(secs_ago)).unwrap();
        let window: VecDeque<(Instant, Slot)> =
            [(at(20), 1_000), (at(10), 1_400), (at(0), 1_800)].into();
        assert_eq!(measure_replay_rate(&window), Some(40.0));
        let window: VecDeque<(Instant, Slot)> = [(at(2), 1_000), (at(0), 1_010)].into();
        assert_eq!(measure_replay_rate(&window), None);
        assert_eq!(measure_replay_rate(&VecDeque::new()), None);
    }

    #[test]
    fn test_not_voting_outranks_every_other_reading() {
        for (vote_slot, behind, since_vote_advance) in [
            (Some(99), Some(1), FRESH),
            (Some(50), Some(VOTE_BEHIND_LIMIT + 1), FRESH),
            (None, None, Duration::from_secs(3_600)),
        ] {
            assert_eq!(
                assess_health(FRESH, 100, false, vote_slot, behind, since_vote_advance).vote,
                VoteHealth::NotVoting
            );
        }
    }

    #[test]
    fn test_a_vote_far_behind_the_tip_is_delinquent() {
        assert_eq!(
            assess_health(
                FRESH,
                100,
                true,
                Some(50),
                Some(VOTE_BEHIND_LIMIT + 1),
                FRESH
            )
            .vote,
            VoteHealth::Delinquent
        );
        assert_eq!(
            assess_health(FRESH, 100, true, Some(50), Some(VOTE_BEHIND_LIMIT), FRESH).vote,
            VoteHealth::Voting
        );
    }

    #[test]
    fn test_a_vote_that_is_close_but_frozen_is_delinquent() {
        // The case the distance alone misses: near the tip and not moving.
        assert_eq!(
            assess_health(FRESH, 100, true, Some(99), Some(1), Duration::from_secs(61)).vote,
            VoteHealth::Delinquent
        );
    }

    #[test]
    fn test_a_node_that_has_never_voted_is_not_delinquent() {
        // An unstaked node is not a failing one, however long it sits there.
        assert_eq!(
            assess_health(FRESH, 100, true, None, None, Duration::from_secs(3_600)).vote,
            VoteHealth::NotStarted
        );
    }

    #[test]
    fn test_cluster_names_are_the_ones_upstream_parses() {
        for cluster_type in ClusterType::STRINGS.map(|name| name.parse::<ClusterType>().unwrap()) {
            assert_eq!(cluster_name(cluster_type).parse(), Ok(cluster_type));
        }
    }

    #[test]
    fn test_releases_fold_their_prerelease_tags() {
        assert_eq!(strip_prerelease("4.2.0-rc.1"), "4.2.0");
        assert_eq!(strip_prerelease("0.1102.0-beta.40201"), "0.1102.0");
        assert_eq!(strip_prerelease("4.2.0"), "4.2.0");
        assert_eq!(strip_prerelease("1.18.23+build7"), "1.18.23");
        assert_eq!(strip_prerelease(""), "");
        assert_eq!(strip_prerelease("unknown"), "unknown");
        assert_eq!(strip_prerelease("-leading"), "");
    }
}
