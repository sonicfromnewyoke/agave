//! Turns in the held history that match a leader's name or key, a slot number, this validator, or
//! conditions on their slots and leaders, answered a page at a time for the schedule page's search,
//! and the same matches summed up, overall and by client. Turns to come are matched on what is
//! known before they happen.

use {
    crate::{
        collect::EpochInfo,
        history::{
            HAS_BLOCK, HAS_CLOCK, HAS_REPLAYED, HAS_SHREDS, HAS_TIPS, PackedSlot, REWARD_MASK,
            REWARD_NONE, REWARD_PAID, REWARD_UNPAID, SlotHistory, SlotRange,
        },
        slots::SlotLevel,
        validator_info::ValidatorInfoCache,
    },
    agave_votor_messages::reward_certificate::NUM_SLOTS_FOR_REWARD,
    serde::{Deserialize, Serialize},
    solana_clock::Slot,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::{
        cmp::Reverse,
        collections::{BTreeSet, HashMap, HashSet},
        fmt::Write,
        str::FromStr,
        sync::RwLock,
    },
};

const SLOTS_PER_TURN: u64 = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;

/// A page of turns with their lead-in rows stays under half the message ceiling; a test holds it.
pub const MAX_SEARCH_TURNS: usize = 128;

/// Turn numbers and their leaders stay well under the message ceiling; a test holds it.
pub const MAX_FUTURE_TURNS: usize = 256;

/// Longer than any name, and it bounds the cost of each comparison.
const MAX_QUERY_CHARS: usize = 64;

/// Turns read per hold of the history lock: the span a range reply reads, so the slot feed's
/// writes wait no longer than they do behind a range reply.
const TURNS_PER_LOCK: usize = 1024;

const SLOTS_PER_LOCK: u64 = TURNS_PER_LOCK as u64 * SLOTS_PER_TURN;

/// Fewer non-vote transactions than this is few.
pub const FEW_TRANSACTIONS: u32 = 100;

/// How far back a slot's duration looks for an earlier block that filled, as the page does.
const FULL_REACH: u64 = 64;

/// Slower than this share of the slots at the same place in the turn is slow.
const SLOW_PERCENT: u64 = 95;

/// Fewer durations than this at a place gives that place no cut-off.
const SLOW_SAMPLES: u64 = 20;

/// Longer durations share the last bucket.
const MAX_DURATION_MILLIS: usize = 10_000;

/// A condition on a slot, one per filter on the page; a turn is listed when one of its slots meets
/// every one asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Only {
    Skipped,
    /// Slower than the cut-off for its place in the turn.
    Slow,
    /// The reward certificate was written without our vote.
    LeftOut,
    FewTransactions,
    /// Tips were read, and were nought.
    NoTips,
}

/// What gossip and the bank say of a staked node or scheduled leader, for conditions on a turn's
/// leader. Current, not as of the turn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LeaderTraits {
    pub client: Option<String>,
    pub version: Option<String>,
    pub ip: Option<String>,
    pub country: Option<&'static str>,
    /// In lamports.
    pub stake: u64,
    /// Not voting: never, or too far behind the tip.
    #[serde(skip)]
    pub delinquent: bool,
    /// The oldest last vote among its staked vote accounts.
    #[serde(skip)]
    pub last_vote: Option<Slot>,
}

/// The traits of every staked node and of every held epoch's leaders in gossip, written by the
/// collector.
pub type LeaderMap = HashMap<Pubkey, LeaderTraits>;

/// A column a condition reads: the leader's, or the slot's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Field {
    /// Whether the leader is voting now: `delinquent` or `voting`.
    Leader,
    /// Part of the leader's name or identity key, any case.
    Name,
    Client,
    /// A version, or its start at a dot: `4.4` holds `4.4.0`.
    Version,
    Country,
    Stake,
    /// Slots from the end of a turn to its leader's next turn, or to the epoch's end for its last.
    Gap,
    Voted,
    LeftOut,
    Transactions,
    Failed,
    /// Failed as a percentage of the block's transactions.
    FailedShare,
    BaseFee,
    Priority,
    Tips,
    Duration,
    Shreds,
    Repaired,
    Received,
    Replay,
    Compute,
    BlockFill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Is,
    IsNot,
}

/// One row of the page's conditions. Missing figures never match.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Condition {
    pub field: Field,
    pub op: Op,
    /// For a comparison, in the field's own unit: lamports, milliseconds, compute units, percent.
    #[serde(default)]
    pub value: f64,
    /// For `is` and `is_not`: names, clients, country codes, or `paid`, `unpaid` and `none` for
    /// voted.
    #[serde(default)]
    pub values: Vec<String>,
}

/// What the page asks for: text matched as the page matches it, and whether only ours count.
#[derive(Debug, Default, Deserialize)]
pub struct SearchParams {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub ours: bool,
    /// Turns starting below this slot are searched, newest first; with `forward`, turns starting at
    /// or above it, oldest first.
    pub before: Slot,
    /// Searches up from `before`, for scrolling up from a jump.
    #[serde(default)]
    pub forward: bool,
    #[serde(default)]
    pub only: Vec<Only>,
    /// Turns that began after this are passed over, for a jump to a time.
    pub until_millis: Option<u64>,
    /// The slow cut-offs a first page answered with, so later pages keep them.
    pub slow: Option<Vec<Option<u32>>>,
    #[serde(default, rename = "where")]
    pub conditions: Vec<Condition>,
    /// A range: turns outside it are passed over, and a scan stops on leaving it.
    pub from_slot: Option<Slot>,
    pub to_slot: Option<Slot>,
    pub from_millis: Option<u64>,
    pub to_millis: Option<u64>,
}

/// A page of matching turns, newest first.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct SearchReply {
    /// Each matching turn's rows, from the slot whose certificate its first row shows.
    pub turns: Vec<SlotRange>,
    /// Where to search below next; `None` once the history is read to its end.
    pub next: Option<Slot>,
    /// Each place in the turn's slow cut-off in milliseconds, first slot first, where slow was
    /// asked for.
    pub slow: Option<Vec<Option<u32>>>,
}

/// The name conditions as matched: whether each is `is`, and its values in lower case.
type NameWants = [(bool, Vec<String>)];

/// One epoch's leaders, each marked for whether its name or key matches the search and the name
/// conditions, with its traits.
struct EpochLeaders<'a> {
    epoch: &'a EpochInfo,
    named: Vec<bool>,
    /// Empty where no name condition is asked.
    names_held: Vec<bool>,
    ours: Option<u16>,
    traits: Vec<Option<LeaderTraits>>,
    /// Each turn's gap in slots; empty where no gap condition is asked.
    gaps: Vec<u64>,
}

/// A turn's leader as a search sees it.
#[derive(Clone, Copy)]
struct LeaderAt<'a> {
    key: Option<&'a str>,
    named: bool,
    names_held: bool,
    ours: bool,
    traits: Option<&'a LeaderTraits>,
    gap: Option<u64>,
    block_cost_limit: u64,
}

impl<'a> EpochLeaders<'a> {
    fn new(
        epoch: &'a EpochInfo,
        info: &ValidatorInfoCache,
        map: &LeaderMap,
        needle: &str,
        names: &NameWants,
        with_gaps: bool,
    ) -> Self {
        let traits = epoch
            .leaders
            .iter()
            .map(|key| map.get(&Pubkey::from_str(key).ok()?).cloned())
            .collect();
        let (named, names_held) = if needle.is_empty() && names.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            epoch
                .leaders
                .iter()
                .map(|key| {
                    let name = Pubkey::from_str(key).ok().and_then(|identity| {
                        info.get(&identity)?.name.as_deref().map(str::to_lowercase)
                    });
                    let key = key.to_lowercase();
                    let has = |part: &str| {
                        key.contains(part)
                            || name.as_deref().is_some_and(|name| name.contains(part))
                    };
                    let held = names
                        .iter()
                        .all(|(is, values)| values.iter().any(|value| has(value)) == *is);
                    (!needle.is_empty() && has(needle), names.is_empty() || held)
                })
                .unzip()
        };
        let ours = epoch
            .my_leader_slots
            .first()
            .and_then(|slot| leader_index(epoch, *slot));
        Self {
            epoch,
            named,
            names_held,
            ours,
            traits,
            gaps: if with_gaps {
                turn_gaps(epoch)
            } else {
                Vec::new()
            },
        }
    }

    /// The one leader the name conditions hold for, if there is exactly one.
    fn only_named(&self) -> Option<&'a str> {
        let mut held = self
            .names_held
            .iter()
            .zip(&self.epoch.leaders)
            .filter(|(held, _)| **held);
        let (_, key) = held.next()?;
        held.next().is_none().then_some(key.as_str())
    }

    /// The turn's leader: whether it matches by name or key, whether it is ours, and its traits;
    /// `None` outside this epoch.
    fn at(&self, slot: Slot) -> Option<LeaderAt<'_>> {
        let index = usize::from(leader_index(self.epoch, slot)?);
        Some(LeaderAt {
            key: self.epoch.leaders.get(index).map(String::as_str),
            named: self.named.get(index).copied().unwrap_or(false),
            names_held: self
                .names_held
                .get(index)
                .copied()
                .unwrap_or(self.names_held.is_empty()),
            ours: self.ours.map(usize::from) == Some(index),
            traits: self.traits.get(index).and_then(Option::as_ref),
            gap: turn_index(self.epoch, slot).and_then(|turn| self.gaps.get(turn).copied()),
            block_cost_limit: self.epoch.block_cost_limit,
        })
    }
}

fn turn_index(epoch: &EpochInfo, slot: Slot) -> Option<usize> {
    if slot < epoch.start_slot || slot > epoch.end_slot {
        return None;
    }
    let turn = slot
        .checked_sub(epoch.start_slot)?
        .checked_div(SLOTS_PER_TURN)?;
    usize::try_from(turn).ok()
}

/// The index into the epoch's leaders of the turn `slot` is in; `None` outside the epoch.
pub(crate) fn leader_index(epoch: &EpochInfo, slot: Slot) -> Option<u16> {
    epoch.turns.get(turn_index(epoch, slot)?).copied()
}

/// Each turn's gap in slots, read back from the epoch's end.
fn turn_gaps(epoch: &EpochInfo) -> Vec<u64> {
    let mut next = HashMap::new();
    let mut gaps = vec![0; epoch.turns.len()];
    for (at, leader) in epoch.turns.iter().enumerate().rev() {
        let turn = u64::try_from(at).unwrap_or(u64::MAX);
        let end = turn
            .saturating_add(1)
            .saturating_mul(SLOTS_PER_TURN)
            .saturating_add(epoch.start_slot);
        let next_start =
            next.insert(*leader, turn)
                .map_or(epoch.end_slot.saturating_add(1), |later: u64| {
                    later
                        .saturating_mul(SLOTS_PER_TURN)
                        .saturating_add(epoch.start_slot)
                });
        if let Some(gap) = gaps.get_mut(at) {
            *gap = next_start.saturating_sub(end);
        }
    }
    gaps
}

/// When a row's block became full here, as the page reads it.
fn full_at(row: &PackedSlot) -> Option<u64> {
    (row.flags & HAS_CLOCK != 0 && row.flags & HAS_SHREDS != 0)
        .then(|| row.time_millis.saturating_add(u64::from(row.full_millis)))
}

/// From the nearest earlier block to fill to this one filling, as the page measures it.
fn duration_millis(history: &SlotHistory, slot: Slot) -> Option<u64> {
    let full = full_at(history.get(slot)?)?;
    let previous =
        (1..=FULL_REACH).find_map(|back| full_at(history.get(slot.checked_sub(back)?)?))?;
    full.checked_sub(previous)
}

fn place(slot: Slot) -> usize {
    usize::try_from(slot.checked_rem(SLOTS_PER_TURN).unwrap_or(0)).unwrap_or(0)
}

/// The duration at or under which `SLOW_PERCENT` of the counted slots fall.
fn cut_off(counts: &[u32]) -> Option<u32> {
    let total = counts.iter().map(|count| u64::from(*count)).sum::<u64>();
    if total < SLOW_SAMPLES {
        return None;
    }
    let wanted = total.saturating_mul(SLOW_PERCENT).div_ceil(100);
    let mut seen = 0u64;
    counts.iter().enumerate().find_map(|(millis, count)| {
        seen = seen.saturating_add(u64::from(*count));
        (seen >= wanted)
            .then(|| u32::try_from(millis).ok())
            .flatten()
    })
}

/// Each place in the turn's cut-off, from every slot held between `floor` and `top`.
fn slow_cut_offs(history: &RwLock<SlotHistory>, floor: Slot, top: Slot) -> Vec<Option<u32>> {
    let places = usize::try_from(SLOTS_PER_TURN).unwrap_or(1);
    let mut counts = vec![vec![0u32; MAX_DURATION_MILLIS.saturating_add(1)]; places];
    // The last block to fill, carried across the slots so each is read once.
    let mut last: Option<(Slot, u64)> = None;
    let mut slot = floor;
    while slot < top {
        let Ok(history) = history.read() else {
            break;
        };
        let stop = slot.saturating_add(SLOTS_PER_LOCK).min(top);
        for at in slot..stop {
            let Some(full) = history.get(at).and_then(full_at) else {
                continue;
            };
            let gap = last
                .filter(|(was, _)| at.saturating_sub(*was) <= FULL_REACH)
                .and_then(|(_, millis)| full.checked_sub(millis));
            if let Some(gap) = gap {
                let bucket = usize::try_from(gap)
                    .unwrap_or(MAX_DURATION_MILLIS)
                    .min(MAX_DURATION_MILLIS);
                if let Some(count) = counts
                    .get_mut(place(at))
                    .and_then(|at_place| at_place.get_mut(bucket))
                {
                    *count = count.saturating_add(1);
                }
            }
            last = Some((at, full));
        }
        slot = stop;
    }
    counts.iter().map(|counted| cut_off(counted)).collect()
}

/// What a search or a summary is for: text as the page matches it, lower-cased, whether only ours
/// count, and the filters and conditions a slot must meet; with the ring's length and floor.
struct Wanted<'a> {
    needle: String,
    digits: bool,
    ours: bool,
    leaders: Vec<EpochLeaders<'a>>,
    only: &'a [Only],
    /// The slow cut-offs, where slow was asked for.
    slow: Option<Vec<Option<u32>>>,
    conditions: &'a [Condition],
    capacity: u64,
    floor: Slot,
}

impl Wanted<'_> {
    fn leader(&self, start: Slot) -> Option<LeaderAt<'_>> {
        self.leaders.iter().find_map(|leaders| leaders.at(start))
    }

    /// Whether the turn is listed: matched as a turn, with one held slot meeting everything.
    /// `text` is scratch for writing slot numbers, kept across calls.
    fn turn(&self, history: &SlotHistory, start: Slot, text: &mut String) -> bool {
        let end = start.saturating_add(SLOTS_PER_TURN.saturating_sub(1));
        // A turn with no row held is not listed, as the page draws none.
        let Some(first) = (start..=end).find(|slot| history.get(*slot).is_some()) else {
            return false;
        };
        let leader = self.leader(start);
        self.turn_matches(first, end, leader, text)
            && (first..=end).any(|slot| self.meets(history, slot, leader))
    }

    /// The parts of a match that are the turn's rather than a slot's: name, key, slot number, ours.
    fn turn_matches(
        &self,
        first: Slot,
        end: Slot,
        leader: Option<LeaderAt<'_>>,
        text: &mut String,
    ) -> bool {
        if self.ours && !leader.is_some_and(|leader| leader.ours) {
            return false;
        }
        self.needle.is_empty()
            || leader.is_some_and(|leader| leader.named)
            // Only digits can be part of a slot number, so other text skips the formatting.
            || (self.digits
                && (first..=end).any(|slot| {
                    text.clear();
                    write!(text, "{slot}").is_ok() && text.contains(&self.needle)
                }))
    }

    /// Whether a held slot meets every filter and condition; with none asked, any held slot does.
    fn meets(&self, history: &SlotHistory, slot: Slot, leader: Option<LeaderAt<'_>>) -> bool {
        let Some(row) = history.get(slot) else {
            return false;
        };
        self.only.iter().all(|only| match only {
            Only::Skipped => row.level == SlotLevel::Skipped as u8,
            Only::Slow => self
                .slow
                .as_deref()
                .and_then(|slow| slow.get(place(slot)).copied().flatten())
                .zip(duration_millis(history, slot))
                .is_some_and(|(limit, millis)| millis > u64::from(limit)),
            Only::LeftOut => row.flags & REWARD_MASK == REWARD_UNPAID,
            Only::FewTransactions => row.flags & HAS_BLOCK != 0 && row.non_votes < FEW_TRANSACTIONS,
            Only::NoTips => row.flags & HAS_TIPS != 0 && row.tips == 0,
        }) && self
            .conditions
            .iter()
            .all(|condition| holds(condition, history, slot, row, leader))
    }
}

fn holds(
    condition: &Condition,
    history: &SlotHistory,
    slot: Slot,
    row: &PackedSlot,
    leader: Option<LeaderAt<'_>>,
) -> bool {
    if let Some(held) = leader_holds(condition, leader) {
        return held;
    }
    match condition.field {
        Field::Voted => listed(condition, voted(row)),
        field => figure(field, history, slot, row, leader)
            .is_some_and(|figure| compared(condition, figure)),
    }
}

/// Whether a turn meets a condition known before it happens, on its leader or its gap; `None` for a
/// condition on a slot.
fn leader_holds(condition: &Condition, leader: Option<LeaderAt<'_>>) -> Option<bool> {
    let traits = leader.and_then(|leader| leader.traits);
    Some(match condition.field {
        Field::Client => listed(
            condition,
            traits.and_then(|traits| traits.client.as_deref()),
        ),
        Field::Version => version_listed(
            condition,
            traits.and_then(|traits| traits.version.as_deref()),
        ),
        Field::Country => listed(condition, traits.and_then(|traits| traits.country)),
        // Every name condition, matched once per leader.
        Field::Name => leader.is_some_and(|leader| leader.names_held),
        Field::Leader => listed(
            condition,
            traits.map(|traits| {
                if traits.delinquent {
                    "delinquent"
                } else {
                    "voting"
                }
            }),
        ),
        Field::Stake => traits.is_some_and(|traits| compared(condition, traits.stake as f64)),
        Field::Gap => leader
            .and_then(|leader| leader.gap)
            .is_some_and(|gap| compared(condition, gap as f64)),
        Field::Voted
        | Field::LeftOut
        | Field::Transactions
        | Field::Failed
        | Field::FailedShare
        | Field::BaseFee
        | Field::Priority
        | Field::Tips
        | Field::Duration
        | Field::Shreds
        | Field::Repaired
        | Field::Received
        | Field::Replay
        | Field::Compute
        | Field::BlockFill => return None,
    })
}

fn compared(condition: &Condition, figure: f64) -> bool {
    let value = condition.value;
    match condition.op {
        Op::Gt => figure > value,
        Op::Ge => figure >= value,
        Op::Lt => figure < value,
        Op::Le => figure <= value,
        Op::Eq => (figure - value).abs() < f64::EPSILON,
        Op::Is | Op::IsNot => false,
    }
}

/// `is` and `is_not` against versions, each naming one whole or its start at a dot or dash.
fn version_listed(condition: &Condition, version: Option<&str>) -> bool {
    listed_by(condition, version, |listed, version| {
        version
            .strip_prefix(listed)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['.', '-']))
    })
}

/// `is` and `is_not` against a list; a value not known matches neither.
fn listed(condition: &Condition, value: Option<&str>) -> bool {
    listed_by(condition, value, |listed, value| listed == value)
}

/// `is` and `is_not` against a list, each entry naming the value where `names(entry, value)`; a
/// value not known matches neither.
fn listed_by(
    condition: &Condition,
    value: Option<&str>,
    names: impl Fn(&str, &str) -> bool,
) -> bool {
    let Some(value) = value else {
        return false;
    };
    let named = condition.values.iter().any(|listed| names(listed, value));
    match condition.op {
        Op::Is => named,
        Op::IsNot => !named,
        _ => false,
    }
}

fn voted(row: &PackedSlot) -> Option<&'static str> {
    match row.flags & REWARD_MASK {
        REWARD_PAID => Some("paid"),
        REWARD_UNPAID => Some("unpaid"),
        REWARD_NONE => Some("none"),
        _ => None,
    }
}

/// Replay's own span, from the block filling to replay finishing, as the page draws it.
fn replay_millis(row: &PackedSlot) -> Option<u64> {
    (row.flags & HAS_REPLAYED != 0 && row.flags & HAS_SHREDS != 0)
        .then(|| u64::from(row.replayed_millis.saturating_sub(row.full_millis)))
}

/// A slot's or its leader's figure in the field's own unit; `None` where not measured.
fn figure(
    field: Field,
    history: &SlotHistory,
    slot: Slot,
    row: &PackedSlot,
    leader: Option<LeaderAt<'_>>,
) -> Option<f64> {
    let block = row.flags & HAS_BLOCK != 0;
    let shreds = row.flags & HAS_SHREDS != 0;
    let whole = match field {
        Field::LeftOut => matches!(row.flags & REWARD_MASK, REWARD_PAID | REWARD_UNPAID)
            .then_some(u64::from(row.left_out)),
        Field::Transactions => block.then_some(u64::from(row.non_votes)),
        Field::Failed => block.then_some(u64::from(row.failed)),
        Field::FailedShare => {
            return (block && row.non_votes > 0)
                .then(|| f64::from(row.failed) / f64::from(row.non_votes) * 100.0);
        }
        Field::BaseFee => block.then(|| row.fees.saturating_sub(row.priority_fees)),
        Field::Priority => block.then_some(row.priority_fees),
        Field::Tips => (row.flags & HAS_TIPS != 0).then_some(row.tips),
        Field::Duration => duration_millis(history, slot),
        Field::Shreds => shreds.then_some(u64::from(row.shreds)),
        Field::Repaired => shreds.then_some(u64::from(row.repaired)),
        Field::Received => shreds.then_some(u64::from(row.full_millis)),
        Field::Replay => replay_millis(row),
        Field::Compute => block.then_some(u64::from(row.compute)),
        Field::BlockFill => {
            let limit = leader
                .map(|leader| leader.block_cost_limit)
                .filter(|limit| *limit > 0)?;
            return block.then(|| f64::from(row.compute) / limit as f64 * 100.0);
        }
        Field::Leader
        | Field::Name
        | Field::Client
        | Field::Version
        | Field::Country
        | Field::Stake
        | Field::Gap
        | Field::Voted => None,
    };
    whole.map(|value| value as f64)
}

/// Where a turn lies against the range asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Below,
    Within,
    Above,
}

fn range_side(params: &SearchParams, history: &SlotHistory, start: Slot) -> Side {
    let end = start.saturating_add(SLOTS_PER_TURN.saturating_sub(1));
    if params.from_slot.is_some_and(|from| end < from) {
        return Side::Below;
    }
    if params.to_slot.is_some_and(|to| start > to) {
        return Side::Above;
    }
    if params.from_millis.is_none() && params.to_millis.is_none() {
        return Side::Within;
    }
    // A turn with nothing timed in or near it is taken as inside.
    match began_millis(history, start) {
        Some(began) if params.from_millis.is_some_and(|from| began < from) => Side::Below,
        Some(began) if params.to_millis.is_some_and(|to| began > to) => Side::Above,
        _ => Side::Within,
    }
}

/// When the turn's first held slot with a clock began; for a turn with none, when the last slot
/// timed before it did.
fn began_millis(history: &SlotHistory, start: Slot) -> Option<u64> {
    let clock = |slot: Slot| {
        history
            .get(slot)
            .filter(|row| row.flags & HAS_CLOCK != 0)
            .map(|row| row.time_millis)
    };
    (start..start.saturating_add(SLOTS_PER_TURN))
        .find_map(clock)
        .or_else(|| (1..=FULL_REACH).find_map(|back| clock(start.checked_sub(back)?)))
}

/// Text as it is matched: trimmed, cut to `MAX_QUERY_CHARS`, lower case.
fn normalize_text(text: &str) -> String {
    let cut: String = text.trim().chars().take(MAX_QUERY_CHARS).collect();
    cut.to_lowercase()
}

fn prepare<'a>(
    history: &RwLock<SlotHistory>,
    info: &RwLock<ValidatorInfoCache>,
    map: &LeaderMap,
    epochs: &'a [EpochInfo],
    params: &'a SearchParams,
) -> Option<Wanted<'a>> {
    let needle = normalize_text(&params.query);
    let names: Vec<(bool, Vec<String>)> = params
        .conditions
        .iter()
        .filter(|condition| condition.field == Field::Name)
        .map(|condition| {
            let values = condition
                .values
                .iter()
                .map(|value| normalize_text(value))
                .filter(|value| !value.is_empty())
                .collect();
            (condition.op != Op::IsNot, values)
        })
        .collect();
    let with_gaps = params
        .conditions
        .iter()
        .any(|condition| condition.field == Field::Gap);
    let leaders = {
        let info = info.read().ok()?;
        epochs
            .iter()
            .map(|epoch| EpochLeaders::new(epoch, &info, map, &needle, &names, with_gaps))
            .collect()
    };
    let capacity = u64::try_from(history.read().ok()?.capacity()).unwrap_or(u64::MAX);
    // Nothing below this can still be in the ring.
    let floor = params.before.saturating_sub(capacity);
    // Measured over the whole ring, which reaches no further than its length past the start, so a
    // jump or the live window above it does not move them.
    let slow = params.only.contains(&Only::Slow).then(|| {
        params.slow.clone().unwrap_or_else(|| {
            slow_cut_offs(history, floor, params.before.saturating_add(capacity))
        })
    });
    Some(Wanted {
        digits: !needle.is_empty() && needle.bytes().all(|byte| byte.is_ascii_digit()),
        needle,
        ours: params.ours,
        leaders,
        only: &params.only,
        slow,
        conditions: &params.conditions,
        capacity,
        floor,
    })
}

pub fn search(
    history: &RwLock<SlotHistory>,
    info: &RwLock<ValidatorInfoCache>,
    leaders: &RwLock<LeaderMap>,
    epochs: &[EpochInfo],
    params: &SearchParams,
) -> SearchReply {
    let Ok(map) = leaders.read() else {
        return SearchReply::default();
    };
    let Some(wanted) = prepare(history, info, &map, epochs, params) else {
        return SearchReply::default();
    };
    drop(map);
    let lead = NUM_SLOTS_FOR_REWARD;
    let rows = usize::try_from(lead.saturating_add(SLOTS_PER_TURN)).unwrap_or(usize::MAX);

    // Passed once a turn at or before the time is reached; older turns are older still.
    let mut reached = params.until_millis.is_none() || params.forward;
    // Each turn's first slot in the order searched, as far as the ring can reach either way.
    let ceiling = params.before.saturating_add(wanted.capacity);
    let floor = wanted.floor;
    let mut turn = params.before.checked_div(SLOTS_PER_TURN).unwrap_or(0);
    let mut next_start = || -> Option<Slot> {
        if params.forward {
            let start = turn.saturating_mul(SLOTS_PER_TURN);
            turn = turn.saturating_add(1);
            (start < ceiling).then_some(start)
        } else {
            turn = turn.checked_sub(1)?;
            let start = turn.saturating_mul(SLOTS_PER_TURN);
            (start >= floor).then_some(start)
        }
    };
    let mut found = Vec::new();
    let mut text = String::new();
    let next = 'scan: loop {
        let Ok(history) = history.read() else {
            break 'scan None;
        };
        for _ in 0..TURNS_PER_LOCK {
            let Some(start) = next_start() else {
                break 'scan None;
            };
            match range_side(params, &history, start) {
                Side::Within => {}
                // Every turn further on lies outside too.
                Side::Below if !params.forward => break 'scan None,
                Side::Above if params.forward => break 'scan None,
                Side::Below | Side::Above => continue,
            }
            if !reached {
                match (began_millis(&history, start), params.until_millis) {
                    (Some(began), Some(until)) if began <= until => reached = true,
                    _ => continue,
                }
            }
            if !wanted.turn(&history, start, &mut text) {
                continue;
            }
            found.push(history.range(start.saturating_sub(lead), rows));
            if found.len() >= MAX_SEARCH_TURNS {
                break 'scan Some(if params.forward {
                    start.saturating_add(SLOTS_PER_TURN)
                } else {
                    start
                });
            }
        }
    };
    SearchReply {
        turns: found,
        next,
        slow: wanted.slow,
    }
}

/// What a turn's card shows of its leader; whether it votes goes with the summary.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct LeaderRow {
    pub identity: String,
    #[serde(flatten)]
    pub traits: LeaderTraits,
}

/// Every staked node and scheduled leader in gossip. The peer table holds only those near the tip.
/// By identity, so the reply does not change with the map's order.
pub fn leader_rows(leaders: &LeaderMap) -> Vec<LeaderRow> {
    let mut rows: Vec<LeaderRow> = leaders
        .iter()
        .map(|(identity, traits)| LeaderRow {
            identity: identity.to_string(),
            traits: traits.clone(),
        })
        .collect();
    rows.sort_by(|a, b| a.identity.cmp(&b.identity));
    rows
}

/// A page of the turns to come that match, oldest first.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct FutureReply {
    /// Turn numbers: a turn's first slot over the slots in a turn.
    pub turns: Vec<u64>,
    /// Each leader of those turns once.
    pub leaders: Vec<LeaderRow>,
    /// Where the next page starts; `None` once the schedule is read to its end.
    pub next: Option<Slot>,
    /// Matches after this page.
    pub rest: u64,
    /// The validator name conditions choose, where they hold for exactly one this epoch.
    pub named: Option<String>,
}

/// The turns to come from `before` on that a search would list, a page at a time. Only what is
/// known of a turn before it happens is matched, so a filter on a slot's figures matches none. Time
/// bounds are ignored: a turn to come has no clock, and the page sends them as slots.
pub fn future(
    history: &RwLock<SlotHistory>,
    info: &RwLock<ValidatorInfoCache>,
    leaders: &RwLock<LeaderMap>,
    epochs: &[EpochInfo],
    params: &SearchParams,
) -> FutureReply {
    let knowable = params.only.is_empty()
        && params
            .conditions
            .iter()
            .all(|condition| leader_holds(condition, None).is_some());
    if !knowable {
        return FutureReply::default();
    }
    let Ok(map) = leaders.read() else {
        return FutureReply::default();
    };
    let Some(wanted) = prepare(history, info, &map, epochs, params) else {
        return FutureReply::default();
    };
    drop(map);
    let last = epochs.iter().map(|epoch| epoch.end_slot).max().unwrap_or(0);
    let last = params.to_slot.map_or(last, |to| to.min(last));

    let chooses = params
        .conditions
        .iter()
        .any(|condition| condition.field == Field::Name)
        && params
            .conditions
            .iter()
            .all(|condition| condition.field != Field::Name || condition.op == Op::Is);
    let named = chooses
        .then(|| {
            wanted
                .leaders
                .iter()
                .find(|held| {
                    held.at(params.before)
                        .is_some_and(|leader| leader.key.is_some())
                })?
                .only_named()
        })
        .flatten();
    let mut reply = FutureReply {
        named: named.map(str::to_string),
        ..FutureReply::default()
    };
    let mut seen = BTreeSet::new();
    let mut text = String::new();
    // Nothing to come lies below the held epochs, and the request names where the walk starts.
    let first = epochs.iter().map(|epoch| epoch.start_slot).min();
    let mut turn = params
        .before
        .max(first.unwrap_or(0))
        .div_ceil(SLOTS_PER_TURN);
    loop {
        let start = turn.saturating_mul(SLOTS_PER_TURN);
        if start > last {
            return reply;
        }
        let end = start.saturating_add(SLOTS_PER_TURN.saturating_sub(1));
        let leader = wanted.leader(start);
        let matches = params.from_slot.is_none_or(|from| end >= from)
            && wanted.turn_matches(start, end, leader, &mut text)
            && params
                .conditions
                .iter()
                .all(|condition| leader_holds(condition, leader) == Some(true));
        if matches {
            if reply.turns.len() < MAX_FUTURE_TURNS {
                reply.turns.push(turn);
                let key = leader.and_then(|leader| leader.key);
                if let Some(key) = key.filter(|key| seen.insert(*key)) {
                    let traits = leader.and_then(|leader| leader.traits);
                    reply.leaders.push(LeaderRow {
                        identity: key.to_string(),
                        traits: traits.cloned().unwrap_or_default(),
                    });
                }
            } else {
                reply.next.get_or_insert(start);
                reply.rest = reply.rest.saturating_add(1);
            }
        }
        turn = turn.saturating_add(1);
    }
}

/// A mean built a slot at a time, sent as the mean, or null with nothing added.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct Mean {
    sum: f64,
    count: u64,
}

impl Mean {
    fn add(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.sum += value as f64;
            self.count = self.count.saturating_add(1);
        }
    }

    fn get(self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }
}

impl Serialize for Mean {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.get().serialize(serializer)
    }
}

/// The matching slots summed up; means are per slot, over the slots that have the figure, fees in
/// lamports and tips before jito's cut.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Sums {
    slots: u64,
    skipped: u64,
    transactions: u64,
    duration_ms: Mean,
    /// Duration of the first slot of each turn.
    first_slot_ms: Mean,
    /// Duration of each turn's other slots.
    rest_of_turn_ms: Mean,
    replay_ms: Mean,
    base_fee: Mean,
    priority_fee: Mean,
    tips: Mean,
    compute: Mean,
}

impl Sums {
    fn add(&mut self, history: &SlotHistory, slot: Slot, row: &PackedSlot) {
        let block = row.flags & HAS_BLOCK != 0;
        self.slots = self.slots.saturating_add(1);
        if row.level == SlotLevel::Skipped as u8 {
            self.skipped = self.skipped.saturating_add(1);
        }
        if block {
            self.transactions = self.transactions.saturating_add(u64::from(row.non_votes));
        }
        let duration = duration_millis(history, slot);
        self.duration_ms.add(duration);
        if place(slot) == 0 {
            self.first_slot_ms.add(duration);
        } else {
            self.rest_of_turn_ms.add(duration);
        }
        // A span from catching up measures the backlog, not replay.
        self.replay_ms
            .add(replay_millis(row).filter(|_| slot >= history.live_from));
        self.base_fee
            .add(block.then(|| row.fees.saturating_sub(row.priority_fees)));
        self.priority_fee.add(block.then_some(row.priority_fees));
        self.tips
            .add((row.flags & HAS_TIPS != 0).then_some(row.tips));
        self.compute.add(block.then_some(u64::from(row.compute)));
    }
}

/// One client's share of the matches; `None` for leaders gossip gives no client for.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClientFigures {
    pub client: Option<String>,
    /// The leaders these slots came from.
    pub validators: usize,
    #[serde(flatten)]
    pub figures: Sums,
}

/// One country's share of the matches; `None` for leaders with no country known.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CountryFigures {
    pub country: Option<&'static str>,
    /// The leaders these slots came from.
    pub validators: usize,
    #[serde(flatten)]
    pub figures: Sums,
}

/// The matching slots of one client or country.
#[derive(Default)]
struct Group<'a> {
    sums: Sums,
    leaders: HashSet<&'a str>,
}

/// The groups with a matching slot, most slots first, each made a row of.
fn ranked<K: Ord, R>(
    groups: HashMap<Option<K>, Group>,
    row: impl Fn(Option<K>, usize, Sums) -> R,
) -> Vec<R> {
    let mut groups: Vec<_> = groups
        .into_iter()
        .filter(|(_, group)| group.sums.slots > 0)
        .collect();
    groups.sort_by(|(a, x), (b, y)| y.sums.slots.cmp(&x.sums.slots).then_with(|| a.cmp(b)));
    groups
        .into_iter()
        .map(|(key, group)| row(key, group.leaders.len(), group.sums))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct HeldFrom {
    pub slot: Slot,
    pub millis: Option<u64>,
}

/// A leader not voting now, which the page tags wherever its turns show.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DelinquentLeader {
    pub identity: String,
    pub last_vote: Option<Slot>,
}

/// What the page offers in its client, country and version lists: those among staked nodes and
/// scheduled leaders.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct Choices {
    pub clients: BTreeSet<String>,
    pub countries: BTreeSet<&'static str>,
    /// Highest first.
    pub versions: BTreeSet<Reverse<String>>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct SummaryReply {
    /// Turns with a slot that matched.
    pub turns: u64,
    pub figures: Sums,
    /// Most slots first.
    pub clients: Vec<ClientFigures>,
    /// Most slots first.
    pub countries: Vec<CountryFigures>,
    /// The oldest slot held, so the page can say how far back it reaches.
    pub held_from: Option<HeldFrom>,
    pub choices: Choices,
    pub slow: Option<Vec<Option<u32>>>,
    /// Every staked node not voting now.
    pub delinquent: Vec<DelinquentLeader>,
}

/// The oldest held slot between `floor` and `top`.
fn held_from(history: &RwLock<SlotHistory>, floor: Slot, top: Slot) -> Option<HeldFrom> {
    let mut slot = floor;
    while slot < top {
        let history = history.read().ok()?;
        let stop = slot.saturating_add(SLOTS_PER_LOCK).min(top);
        if let Some((held, row)) = (slot..stop).find_map(|at| history.get(at).map(|row| (at, *row)))
        {
            return Some(HeldFrom {
                slot: held,
                millis: (row.flags & HAS_CLOCK != 0).then_some(row.time_millis),
            });
        }
        slot = stop;
    }
    None
}

fn asks_nothing(params: &SearchParams) -> bool {
    params.query.trim().is_empty()
        && !params.ours
        && params.only.is_empty()
        && params.conditions.is_empty()
        && params.from_slot.is_none()
        && params.to_slot.is_none()
        && params.from_millis.is_none()
        && params.to_millis.is_none()
}

/// Where a summary stops: with nothing asked, at the start of the newest turn's epoch.
fn summary_floor(params: &SearchParams, epochs: &[EpochInfo], floor: Slot) -> Slot {
    let newest = params.before.saturating_sub(1);
    epochs
        .iter()
        .find(|epoch| (epoch.start_slot..=epoch.end_slot).contains(&newest))
        .filter(|_| asks_nothing(params))
        .map_or(floor, |epoch| floor.max(epoch.start_slot))
}

/// Every turn below `before` that a search would list, summed up overall, by client and by country. With
/// nothing asked, that is the newest turn's epoch.
pub fn summarize(
    history: &RwLock<SlotHistory>,
    info: &RwLock<ValidatorInfoCache>,
    leaders: &RwLock<LeaderMap>,
    epochs: &[EpochInfo],
    params: &SearchParams,
) -> SummaryReply {
    let Ok(map) = leaders.read() else {
        return SummaryReply::default();
    };
    let choices = Choices {
        clients: map
            .values()
            .filter_map(|traits| traits.client.clone())
            .collect(),
        countries: map.values().filter_map(|traits| traits.country).collect(),
        versions: map
            .values()
            .filter_map(|traits| traits.version.clone().map(Reverse))
            .collect(),
    };
    let mut delinquent: Vec<DelinquentLeader> = map
        .iter()
        .filter(|(_, traits)| traits.delinquent)
        .map(|(identity, traits)| DelinquentLeader {
            identity: identity.to_string(),
            last_vote: traits.last_vote,
        })
        .collect();
    delinquent.sort_by(|a, b| a.identity.cmp(&b.identity));
    let Some(wanted) = prepare(history, info, &map, epochs, params) else {
        return SummaryReply::default();
    };
    drop(map);

    let stop_at = summary_floor(params, epochs, wanted.floor);
    let mut overall = Sums::default();
    let mut by_client: HashMap<Option<&str>, Group> = HashMap::new();
    let mut by_country: HashMap<Option<&'static str>, Group> = HashMap::new();
    let mut turns = 0u64;
    let mut text = String::new();
    let mut turn = params.before.checked_div(SLOTS_PER_TURN).unwrap_or(0);
    'scan: loop {
        let Ok(history) = history.read() else {
            break;
        };
        for _ in 0..TURNS_PER_LOCK {
            let Some(below) = turn.checked_sub(1) else {
                break 'scan;
            };
            turn = below;
            let start = turn.saturating_mul(SLOTS_PER_TURN);
            if start < stop_at {
                break 'scan;
            }
            match range_side(params, &history, start) {
                Side::Within => {}
                Side::Below => break 'scan,
                Side::Above => continue,
            }
            let end = start.saturating_add(SLOTS_PER_TURN.saturating_sub(1));
            let Some(first) = (start..=end).find(|slot| history.get(*slot).is_some()) else {
                continue;
            };
            let leader = wanted.leader(start);
            if !wanted.turn_matches(first, end, leader, &mut text) {
                continue;
            }
            let traits = leader.and_then(|leader| leader.traits);
            let client = by_client
                .entry(traits.and_then(|traits| traits.client.as_deref()))
                .or_default();
            let country = by_country
                .entry(traits.and_then(|traits| traits.country))
                .or_default();
            let mut matched = false;
            for slot in first..=end {
                let Some(row) = history.get(slot) else {
                    continue;
                };
                if !wanted.meets(&history, slot, leader) {
                    continue;
                }
                matched = true;
                overall.add(&history, slot, row);
                client.sums.add(&history, slot, row);
                country.sums.add(&history, slot, row);
            }
            if matched {
                turns = turns.saturating_add(1);
                if let Some(key) = leader.and_then(|leader| leader.key) {
                    client.leaders.insert(key);
                    country.leaders.insert(key);
                }
            }
        }
    }

    SummaryReply {
        turns,
        figures: overall,
        clients: ranked(by_client, |client, validators, figures| ClientFigures {
            client: client.map(str::to_string),
            validators,
            figures,
        }),
        countries: ranked(by_country, |country, validators, figures| CountryFigures {
            country,
            validators,
            figures,
        }),
        held_from: held_from(history, wanted.floor, params.before),
        choices,
        slow: wanted.slow,
        delinquent,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{proto, validator_info::ValidatorInfo},
    };

    const START: Slot = 1_000;

    /// An epoch from `START` whose turns cycle through `leaders`, with ours the first.
    fn epoch(leaders: &[impl ToString]) -> EpochInfo {
        let turns: Vec<u16> = (0..1_000u16)
            .map(|turn| turn.checked_rem(leaders.len() as u16).unwrap_or(0))
            .collect();
        EpochInfo {
            epoch: 3,
            start_slot: START,
            end_slot: START + 3_999,
            slots_in_epoch: 4_000,
            my_leader_slots: vec![START, START + 1, START + 2, START + 3],
            leaders: leaders.iter().map(|key| key.to_string()).collect(),
            turns,
            block_cost_limit: 0,
            account_cost_limit: 0,
        }
    }

    fn history(from: Slot, to: Slot) -> SlotHistory {
        let mut history = SlotHistory::new(4_096);
        for slot in from..=to {
            history.record_time(slot, slot);
        }
        history
    }

    fn named(key: &Pubkey, name: &str) -> RwLock<ValidatorInfoCache> {
        let mut info = ValidatorInfoCache::default();
        info.replace(
            [(
                *key,
                ValidatorInfo {
                    name: Some(name.into()),
                    icon_url: None,
                },
            )]
            .into(),
        );
        RwLock::new(info)
    }

    fn params(query: &str, ours: bool, before: Slot) -> SearchParams {
        SearchParams {
            query: query.into(),
            ours,
            before,
            ..SearchParams::default()
        }
    }

    fn is(field: Field, op: Op, values: &[&str]) -> Condition {
        Condition {
            field,
            op,
            value: 0.0,
            values: values.iter().map(|value| value.to_string()).collect(),
        }
    }

    fn cmp(field: Field, op: Op, value: f64) -> Condition {
        Condition {
            field,
            op,
            value,
            values: Vec::new(),
        }
    }

    /// What the searches read, with no names or traits unless set.
    struct Scan {
        history: RwLock<SlotHistory>,
        info: RwLock<ValidatorInfoCache>,
        map: RwLock<LeaderMap>,
        epochs: [EpochInfo; 1],
    }

    impl Scan {
        fn search(&self, params: SearchParams) -> SearchReply {
            search(&self.history, &self.info, &self.map, &self.epochs, &params)
        }

        fn future(&self, params: SearchParams) -> FutureReply {
            future(&self.history, &self.info, &self.map, &self.epochs, &params)
        }

        fn summarize(&self, params: SearchParams) -> SummaryReply {
            summarize(&self.history, &self.info, &self.map, &self.epochs, &params)
        }
    }

    fn scan(history: SlotHistory, leaders: &[impl ToString]) -> Scan {
        Scan {
            history: RwLock::new(history),
            info: RwLock::default(),
            map: RwLock::default(),
            epochs: [epoch(leaders)],
        }
    }

    /// Two leaders taking turns, the first ours, with traits for the second only.
    fn two_leaders(history: SlotHistory) -> Scan {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let traits = LeaderTraits {
            client: Some("AgaveBam".into()),
            version: Some("4.4.1".into()),
            ip: Some("192.0.2.1".into()),
            country: Some("DE"),
            stake: 5_000,
            delinquent: true,
            last_vote: Some(7),
        };
        Scan {
            map: RwLock::new(LeaderMap::from([(other, traits)])),
            ..scan(history, &[ours, other])
        }
    }

    fn only(conditions: &[Only], before: Slot) -> SearchParams {
        SearchParams {
            only: conditions.to_vec(),
            ..params("", false, before)
        }
    }

    fn where_(conditions: Vec<Condition>, before: Slot) -> SearchParams {
        SearchParams {
            conditions,
            ..params("", false, before)
        }
    }

    /// Every slot an ordinary block with tips and our vote paid, then `fill` on each.
    fn rows(from: Slot, to: Slot, fill: impl Fn(Slot, &mut PackedSlot)) -> SlotHistory {
        let mut history = SlotHistory::new(4_096);
        for slot in from..=to {
            history.record_test_row(slot, |row| {
                row.level = SlotLevel::Finalized as u8;
                row.flags = HAS_BLOCK | HAS_TIPS | crate::history::REWARD_PAID;
                row.non_votes = 500;
                row.tips = 10;
                fill(slot, row);
            });
        }
        history
    }

    fn starts(reply: &SearchReply) -> Vec<Slot> {
        reply
            .turns
            .iter()
            .map(|turn| turn.first_slot.saturating_add(NUM_SLOTS_FOR_REWARD))
            .collect()
    }

    #[test]
    fn test_a_name_matches_whatever_its_case() {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let scan = Scan {
            info: named(&other, "Hamsa Labs"),
            ..scan(history(START, START + 15), &[ours, other])
        };
        let reply = scan.search(params("hamsa", false, START + 16));
        assert_eq!(starts(&reply), vec![START + 12, START + 4], "newest first");
        assert_eq!(reply.next, None);
    }

    #[test]
    fn test_a_key_and_a_slot_number_match() {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let scan = scan(history(START, START + 15), &[ours, other]);
        let key = other.to_string().to_lowercase();
        let by_key = scan.search(params(&key[..6], false, START + 16));
        assert_eq!(starts(&by_key), vec![START + 12, START + 4]);
        let by_slot = scan.search(params("1009", false, START + 16));
        assert_eq!(starts(&by_slot), vec![START + 8]);
    }

    #[test]
    fn test_ours_keeps_only_our_turns() {
        let scan = scan(history(START, START + 15), &["a", "b"]);
        let reply = scan.search(params("", true, START + 16));
        assert_eq!(starts(&reply), vec![START + 8, START]);
    }

    #[test]
    fn test_a_turn_with_nothing_held_is_left_out() {
        let scan = scan(history(START + 8, START + 11), &["a"]);
        let reply = scan.search(params("", true, START + 16));
        assert_eq!(starts(&reply), vec![START + 8]);
    }

    #[test]
    fn test_pages_continue_where_the_last_stopped() {
        // A full page and two turns more.
        let top = START + (MAX_SEARCH_TURNS as u64 + 2) * SLOTS_PER_TURN;
        let scan = scan(history(START, top - 1), &["a"]);
        let first = scan.search(params("", true, top));
        let newest_first: Vec<Slot> = (2..MAX_SEARCH_TURNS as u64 + 2)
            .rev()
            .map(|turn| START + turn * SLOTS_PER_TURN)
            .collect();
        assert_eq!(starts(&first), newest_first);
        assert_eq!(first.turns.len(), MAX_SEARCH_TURNS);
        assert_eq!(first.next, Some(START + 8));
        let rest = scan.search(params("", true, START + 8));
        assert_eq!(starts(&rest), vec![START + 4, START]);
        assert_eq!(rest.next, None);
    }

    #[test]
    fn test_each_turn_carries_the_rows_its_certificates_are_read_from() {
        let scan = scan(history(START, START + 15), &["a"]);
        let reply = scan.search(params("", true, START + 16));
        let turn = &reply.turns[0];
        assert_eq!(turn.first_slot, START + 4);
        assert_eq!(turn.rows.len(), 12);
        assert!(turn.rows.iter().all(Option::is_some));
    }

    #[test]
    fn test_a_search_for_nothing_lists_every_turn_for_a_jump() {
        let scan = scan(history(START, START + 15), &["a"]);
        let reply = scan.search(params("  ", false, START + 12));
        assert_eq!(starts(&reply), vec![START + 8, START + 4, START]);
        assert_eq!(reply.slow, None, "slow was not asked for");
    }

    #[test]
    fn test_each_condition_keeps_the_turns_with_a_slot_meeting_it() {
        let held = rows(START, START + 15, |slot, row| {
            match slot.saturating_sub(START) {
                1 => row.tips = 0,
                5 => {
                    row.level = SlotLevel::Skipped as u8;
                    row.flags = 0;
                }
                9 => row.non_votes = 99,
                13 => row.flags = HAS_BLOCK | HAS_TIPS | REWARD_UNPAID,
                _ => {}
            }
        });
        let scan = scan(held, &["a"]);
        let found = |conditions: &[Only]| starts(&scan.search(only(conditions, START + 16)));
        assert_eq!(found(&[Only::NoTips]), vec![START]);
        assert_eq!(found(&[Only::Skipped]), vec![START + 4]);
        assert_eq!(found(&[Only::FewTransactions]), vec![START + 8]);
        assert_eq!(found(&[Only::LeftOut]), vec![START + 12]);
    }

    #[test]
    fn test_conditions_together_must_meet_in_one_slot() {
        let held = rows(START, START + 7, |slot, row| {
            match slot.saturating_sub(START) {
                1 => row.non_votes = 5,
                2 => row.flags = HAS_BLOCK | HAS_TIPS | REWARD_UNPAID,
                5 => {
                    row.non_votes = 5;
                    row.flags = HAS_BLOCK | HAS_TIPS | REWARD_UNPAID;
                }
                _ => {}
            }
        });
        let scan = scan(held, &["a"]);
        let reply = scan.search(only(&[Only::FewTransactions, Only::LeftOut], START + 8));
        assert_eq!(starts(&reply), vec![START + 4]);
    }

    #[test]
    fn test_slow_is_measured_against_the_same_place_in_the_turn() {
        // First slots take 300 ms and the rest 200, but for one of each.
        let duration = |slot: Slot| match (slot.saturating_sub(START), place(slot)) {
            (41, _) => 260,
            (200, _) => 500,
            (_, 0) => 300,
            _ => 200,
        };
        let mut history = SlotHistory::new(4_096);
        let mut time = 0;
        for slot in START..START + 400 {
            time = u64::saturating_add(time, duration(slot));
            history.record_test_row(slot, |row| {
                row.flags = HAS_CLOCK | HAS_SHREDS;
                row.time_millis = time;
            });
        }
        let scan = scan(history, &["a"]);
        let first = scan.search(only(&[Only::Slow], START + 400));
        assert_eq!(
            first.slow,
            Some(vec![Some(300), Some(200), Some(200), Some(200)])
        );
        assert_eq!(starts(&first), vec![START + 200, START + 40]);
        // Starting lower, as a jump does, measures the same slots.
        let jumped = scan.search(only(&[Only::Slow], START + 100));
        assert_eq!(jumped.slow, first.slow);
        assert_eq!(starts(&jumped), vec![START + 40]);
        // A later page keeps the first page's cut-offs rather than measuring again.
        let later = scan.search(SearchParams {
            slow: Some(vec![Some(400), Some(300), Some(300), Some(300)]),
            ..only(&[Only::Slow], START + 200)
        });
        assert_eq!(starts(&later), Vec::<Slot>::new());
        assert_eq!(
            later.slow,
            Some(vec![Some(400), Some(300), Some(300), Some(300)])
        );
    }

    #[test]
    fn test_a_place_with_few_durations_has_no_cut_off() {
        assert_eq!(cut_off(&[0, 19]), None);
        assert_eq!(cut_off(&[0, 19, 1]), Some(1));
        assert_eq!(cut_off(&[0, 18, 2]), Some(2));
    }

    #[test]
    fn test_a_jump_to_a_time_starts_at_the_turn_begun_by_then() {
        let scan = scan(history(START, START + 15), &["a"]);
        let reply = scan.search(SearchParams {
            until_millis: Some(START + 9),
            ..params("", false, START + 16)
        });
        assert_eq!(starts(&reply), vec![START + 8, START + 4, START]);
    }

    #[test]
    fn test_a_forward_search_reads_up_from_its_start_a_page_at_a_time() {
        // From the second turn, a full page and two turns more.
        let top = START + (MAX_SEARCH_TURNS as u64 + 3) * SLOTS_PER_TURN;
        let scan = scan(history(START, top - 1), &["a"]);
        let forward = |before: Slot| SearchParams {
            forward: true,
            ..params("", false, before)
        };
        let first = scan.search(forward(START + 4));
        assert_eq!(starts(&first)[..2], [START + 4, START + 8], "oldest first");
        assert_eq!(first.turns.len(), MAX_SEARCH_TURNS);
        let next = top - 8;
        assert_eq!(first.next, Some(next));
        let rest = scan.search(forward(next));
        assert_eq!(starts(&rest), vec![next, next + 4]);
        let past = scan.search(forward(top));
        assert_eq!(starts(&past), Vec::<Slot>::new());
        assert_eq!(past.next, None, "nothing above the newest held");
    }

    #[test]
    fn test_failed_conditions_read_the_count_and_the_share() {
        let scan = two_leaders(rows(START, START + 15, |slot, row| {
            match slot.saturating_sub(START) {
                9 => {
                    row.non_votes = 1_000;
                    row.failed = 200;
                }
                13 => {
                    row.non_votes = 100;
                    row.failed = 60;
                }
                _ => {}
            }
        }));
        let found = |conditions| starts(&scan.search(where_(conditions, START + 16)));
        assert_eq!(
            found(vec![cmp(Field::Failed, Op::Gt, 100.0)]),
            vec![START + 8],
            "the larger count, in the larger block"
        );
        assert_eq!(
            found(vec![cmp(Field::FailedShare, Op::Ge, 50.0)]),
            vec![START + 12],
            "the larger share, in the smaller block"
        );
    }

    #[test]
    fn test_column_conditions_hold_in_one_slot_with_the_leaders_traits() {
        let scan = two_leaders(rows(START, START + 15, |slot, row| {
            if slot.saturating_sub(START) == 13 || slot.saturating_sub(START) == 1 {
                row.non_votes = 50;
            }
        }));
        let found = |conditions| starts(&scan.search(where_(conditions, START + 16)));
        assert_eq!(
            found(vec![cmp(Field::Transactions, Op::Lt, 100.0)]),
            vec![START + 12, START],
            "a figure in one slot"
        );
        assert_eq!(
            found(vec![is(Field::Client, Op::Is, &["AgaveBam", "Other"])]),
            vec![START + 12, START + 4],
            "any of the clients listed"
        );
        assert_eq!(
            found(vec![
                is(Field::Country, Op::Is, &["DE"]),
                cmp(Field::Transactions, Op::Lt, 100.0),
            ]),
            vec![START + 12],
            "a leader's trait and a slot's figure together"
        );
        assert_eq!(
            found(vec![is(Field::Country, Op::IsNot, &["DE"])]),
            Vec::<Slot>::new(),
            "a leader with no country known matches neither way"
        );
        assert_eq!(
            found(vec![cmp(Field::Stake, Op::Ge, 5_000.0)]),
            vec![START + 12, START + 4]
        );
        assert_eq!(
            found(vec![is(Field::Leader, Op::Is, &["delinquent"])]),
            vec![START + 12, START + 4],
            "a leader not voting now"
        );
    }

    #[test]
    fn test_a_name_condition_keeps_any_leader_named_or_keyed() {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let scan = Scan {
            info: named(&other, "Hamsa Labs"),
            ..scan(history(START, START + 15), &[ours, other])
        };
        let found = |conditions| starts(&scan.search(where_(conditions, START + 16)));
        assert_eq!(
            found(vec![is(Field::Name, Op::Is, &["hamsa", "nobody"])]),
            vec![START + 12, START + 4],
            "any of the names, part of one, any case"
        );
        let key: String = ours.to_string().chars().take(8).collect();
        assert_eq!(
            found(vec![is(Field::Name, Op::Is, &["HAMSA", &key])]),
            vec![START + 12, START + 8, START + 4, START],
            "a key as well as a name"
        );
        assert_eq!(
            found(vec![is(Field::Name, Op::IsNot, &["Hamsa Labs"])]),
            vec![START + 8, START],
            "every leader but those named"
        );
        assert_eq!(
            found(vec![
                is(Field::Name, Op::Is, &["hamsa"]),
                is(Field::Name, Op::IsNot, &["labs"]),
            ]),
            Vec::<Slot>::new(),
            "every name condition at once"
        );
    }

    #[test]
    fn test_a_range_keeps_the_turns_inside_it() {
        let scan = scan(history(START, START + 15), &["a"]);
        let reply = scan.search(SearchParams {
            from_slot: Some(START + 5),
            to_slot: Some(START + 10),
            ..params("", false, START + 16)
        });
        assert_eq!(starts(&reply), vec![START + 8, START + 4]);
        assert_eq!(reply.next, None, "stopped on leaving the range");
        let by_time = scan.search(SearchParams {
            from_millis: Some(START + 4),
            to_millis: Some(START + 8),
            ..params("", false, START + 16)
        });
        assert_eq!(starts(&by_time), vec![START + 8, START + 4]);
    }

    #[test]
    fn test_a_skipped_turn_with_no_clock_is_placed_by_the_slot_timed_before_it() {
        let mut held = SlotHistory::new(4_096);
        for slot in START..=START + 15 {
            if (START + 8..START + 12).contains(&slot) {
                held.record_test_row(slot, |row| {
                    row.level = SlotLevel::Skipped as u8;
                    row.flags = 0;
                });
            } else {
                held.record_time(slot, slot);
            }
        }
        let scan = scan(held, &["a"]);
        let reply = scan.search(SearchParams {
            to_millis: Some(START + 5),
            ..params("", false, START + 16)
        });
        assert_eq!(starts(&reply), vec![START + 4, START]);
    }

    #[test]
    fn test_a_summary_sums_the_matches_overall_and_by_client() {
        let scan = two_leaders(rows(START, START + 7, |slot, row| {
            row.fees = 7_000;
            row.priority_fees = 2_000;
            row.compute = 1_000_000;
            if slot == START + 6 {
                row.level = SlotLevel::Skipped as u8;
                row.flags = 0;
            }
        }));
        let reply = scan.summarize(params("", false, START + 8));
        assert_eq!(reply.turns, 2);
        assert_eq!(reply.figures.slots, 8);
        assert_eq!(reply.figures.skipped, 1);
        assert_eq!(reply.figures.transactions, 7 * 500);
        assert_eq!(reply.figures.base_fee.get(), Some(5_000.0));
        assert_eq!(reply.figures.priority_fee.get(), Some(2_000.0));
        assert_eq!(reply.figures.compute.get(), Some(1_000_000.0));
        assert_eq!(reply.clients.len(), 2);
        let agave = reply
            .clients
            .iter()
            .find(|client| client.client.as_deref() == Some("AgaveBam"))
            .expect("the second leader's client");
        assert_eq!((agave.figures.slots, agave.figures.skipped), (4, 1));
        assert_eq!(
            serde_json::to_value(&reply.figures).unwrap(),
            serde_json::json!({
                "slots": 8,
                "skipped": 1,
                "transactions": 3_500,
                "duration_ms": null,
                "first_slot_ms": null,
                "rest_of_turn_ms": null,
                "replay_ms": null,
                "base_fee": 5_000.0,
                "priority_fee": 2_000.0,
                "tips": 10.0,
                "compute": 1_000_000.0,
            })
        );
        let row = serde_json::to_value(agave).unwrap();
        assert_eq!(
            (
                &row["client"],
                &row["validators"],
                &row["slots"],
                &row["base_fee"]
            ),
            (
                &serde_json::json!("AgaveBam"),
                &serde_json::json!(1),
                &serde_json::json!(4),
                &serde_json::json!(5_000.0)
            ),
            "{row}"
        );
        assert!(
            reply.clients.iter().any(|client| client.client.is_none()),
            "a leader with no traits has its own row"
        );
        assert_eq!(
            reply.held_from,
            Some(HeldFrom {
                slot: START,
                millis: None
            })
        );
        assert_eq!(reply.choices.clients, BTreeSet::from(["AgaveBam".into()]));
        assert_eq!(reply.choices.countries, BTreeSet::from(["DE"]));
        assert_eq!(reply.delinquent.len(), 1);
        assert_eq!(reply.delinquent[0].last_vote, Some(7));
    }

    #[test]
    fn test_a_summary_groups_by_country_and_counts_each_leader_once() {
        let scan = two_leaders(rows(START, START + 23, |_, _| {}));
        let reply = scan.summarize(params("", false, START + 24));
        assert_eq!(reply.countries.len(), 2);
        let germany = reply
            .countries
            .iter()
            .find(|row| row.country == Some("DE"))
            .expect("the second leader's country");
        assert_eq!((germany.figures.slots, germany.validators), (12, 1));
        let unknown = reply
            .countries
            .iter()
            .find(|row| row.country.is_none())
            .expect("a leader with no traits has its own row");
        assert_eq!((unknown.figures.slots, unknown.validators), (12, 1));
        let agave = reply
            .clients
            .iter()
            .find(|row| row.client.as_deref() == Some("AgaveBam"))
            .expect("the second leader's client");
        assert_eq!(agave.validators, 1);
    }

    #[test]
    fn test_a_summary_leaves_out_groups_with_no_matching_slot() {
        let scan = two_leaders(rows(START, START + 7, |slot, row| {
            row.non_votes = if slot < START + 4 { 50 } else { 500 };
        }));
        let few = vec![cmp(Field::Transactions, Op::Lt, 100.0)];
        let reply = scan.summarize(where_(few, START + 8));
        assert_eq!(reply.clients.len(), 1);
        assert_eq!(reply.countries.len(), 1);
        assert_eq!(reply.countries[0].country, None);
        assert_eq!(reply.countries[0].figures.slots, 4);
    }

    #[test]
    fn test_a_summary_splits_duration_by_place_in_the_turn() {
        let held = rows(START, START + 7, |slot, row| {
            row.flags |= HAS_CLOCK | HAS_SHREDS;
            let after = slot - START;
            row.time_millis = if after < 4 {
                1_000 + 200 * after
            } else {
                1_950 + 200 * (after - 4)
            };
            row.full_millis = 100;
        });
        let reply = scan(held, &["a"]).summarize(params("", false, START + 8));
        // The first slot held has no earlier block to measure from.
        assert_eq!(reply.figures.first_slot_ms.get(), Some(350.0));
        assert_eq!(reply.figures.rest_of_turn_ms.get(), Some(200.0));
        assert_eq!(reply.countries[0].figures.first_slot_ms.get(), Some(350.0));
    }

    #[test]
    fn test_a_summary_counts_only_the_slots_that_match() {
        let held = rows(START, START + 7, |slot, row| {
            if slot == START + 1 {
                row.non_votes = 50;
            }
        });
        let few = vec![cmp(Field::Transactions, Op::Lt, 100.0)];
        let reply = scan(held, &["a"]).summarize(where_(few, START + 8));
        assert_eq!((reply.turns, reply.figures.slots), (1, 1));
        assert_eq!(reply.figures.transactions, 50);
    }

    #[test]
    fn test_a_summary_asked_nothing_stops_at_the_epoch_while_a_filter_reads_all_held() {
        let scan = two_leaders(rows(START - 8, START + 7, |_, _| {}));
        let reply = scan.summarize(params("", false, START + 8));
        assert_eq!((reply.turns, reply.figures.slots), (2, 8));
        assert_eq!(
            reply.held_from.map(|held| held.slot),
            Some(START - 8),
            "the oldest slot held still reaches past the epoch"
        );

        let many = vec![cmp(Field::Transactions, Op::Gt, 100.0)];
        let filtered = scan.summarize(where_(many, START + 8));
        assert_eq!((filtered.turns, filtered.figures.slots), (4, 16));
    }

    #[test]
    fn test_a_summary_leaves_slots_replayed_while_catching_up_out_of_the_replay_mean() {
        let scan = two_leaders(rows(START, START + 7, |slot, row| {
            row.flags |= HAS_SHREDS | HAS_REPLAYED;
            row.full_millis = 200;
            row.replayed_millis = if slot < START + 4 { 20_200 } else { 208 };
        }));
        let read = || scan.summarize(params("", false, START + 8));
        assert_eq!(read().figures.replay_ms.get(), Some(10_004.0));

        scan.history.write().unwrap().live_from = Slot::MAX;
        assert_eq!(
            read().figures.replay_ms.get(),
            None,
            "still catching up, nothing is live yet"
        );

        scan.history.write().unwrap().live_from = START + 4;
        let reply = read();
        assert_eq!(reply.figures.replay_ms.get(), Some(8.0));
        assert_eq!(reply.figures.slots, 8, "the slots themselves still count");
    }

    #[test]
    fn test_conditions_are_read_by_their_page_names() {
        let params: SearchParams = serde_json::from_str(
            r#"{"before":8,"only":["skipped","slow","left_out","few_transactions","no_tips"]}"#,
        )
        .expect("parses");
        assert_eq!(
            params.only,
            vec![
                Only::Skipped,
                Only::Slow,
                Only::LeftOut,
                Only::FewTransactions,
                Only::NoTips
            ]
        );
    }

    #[test]
    fn test_a_full_page_fits_the_message_ceiling() {
        let mut held = SlotHistory::new(4_096);
        for slot in START..START + 4_000 {
            held.record_worst_case(slot);
        }
        let reply = scan(held, &["a"]).search(params("", true, START + 4_000));
        assert_eq!(reply.turns.len(), MAX_SEARCH_TURNS);
        let encoded = proto::encode_with_id("slot", "search", Some(1), &reply);
        proto::assert_under("a full page", &encoded, proto::MAX_MESSAGE / 2);
    }

    #[test]
    fn test_turns_to_come_match_on_their_leader_with_no_history_held() {
        let agave = vec![is(Field::Client, Op::Is, &["AgaveBam"])];
        let reply = two_leaders(SlotHistory::new(4_096)).future(where_(agave, START));
        let first = START / SLOTS_PER_TURN;
        assert_eq!(
            reply.turns[..4],
            [first + 1, first + 3, first + 5, first + 7],
            "the second leader's turns"
        );
        assert!(reply.turns.iter().all(|turn| (turn - first) % 2 == 1));
        assert_eq!(reply.leaders.len(), 1, "each leader once");
        let leader = &reply.leaders[0].traits;
        assert_eq!(leader.version.as_deref(), Some("4.4.1"));
        assert_eq!(leader.ip.as_deref(), Some("192.0.2.1"));
        assert_eq!(leader.country, Some("DE"));
        assert_eq!(leader.stake, 5_000);
    }

    #[test]
    fn test_a_filter_on_a_slots_figures_matches_no_turn_to_come() {
        let scan = two_leaders(SlotHistory::new(4_096));
        let leader = is(Field::Country, Op::Is, &["DE"]);
        let figure = cmp(Field::Duration, Op::Gt, 400.0);
        assert_eq!(
            scan.future(where_(vec![leader, figure], START)),
            FutureReply::default()
        );
        assert_eq!(
            scan.future(only(&[Only::Skipped], START)),
            FutureReply::default()
        );
    }

    #[test]
    fn test_turns_to_come_are_read_a_page_at_a_time() {
        let scan = two_leaders(SlotHistory::new(4_096));
        let staked = |before| scan.future(where_(vec![cmp(Field::Stake, Op::Gt, 0.0)], before));
        let page = staked(START);
        // Half of the epoch's thousand turns, a full page on this one.
        let after = 500 - MAX_FUTURE_TURNS;
        assert_eq!(page.turns.len(), MAX_FUTURE_TURNS);
        assert_eq!(page.rest, after as u64);
        let next = page.next.expect("more to come");
        let later = staked(next);
        assert_eq!(later.turns.len(), after, "the rest on the next page");
        assert_eq!((later.rest, later.next), (0, None));
        assert!(
            later.turns[0] > page.turns[MAX_FUTURE_TURNS - 1],
            "carries on from the page"
        );
        assert_eq!(later.turns[0].saturating_mul(SLOTS_PER_TURN), next);
    }

    #[test]
    fn test_turns_to_come_start_no_lower_than_the_held_epochs() {
        let scan = two_leaders(SlotHistory::new(4_096));
        let from_nought = scan.future(params("", false, 0));
        assert_eq!(from_nought.turns[0], START / SLOTS_PER_TURN);
        assert_eq!(from_nought.rest, scan.future(params("", false, START)).rest);
    }

    #[test]
    fn test_turns_to_come_start_at_the_next_whole_turn_and_keep_to_the_range() {
        let scan = two_leaders(SlotHistory::new(4_096));
        let first = START / SLOTS_PER_TURN;
        let reply = scan.future(SearchParams {
            from_slot: Some(START + 8),
            to_slot: Some(START + 15),
            ..params("", false, START + 1)
        });
        assert_eq!(reply.turns, [first + 2, first + 3]);
        assert_eq!(reply.next, None);
        assert_eq!(reply.rest, 0);
    }

    #[test]
    fn test_turns_to_come_match_ours_and_words() {
        let scan = two_leaders(SlotHistory::new(4_096));
        let first = START / SLOTS_PER_TURN;
        let read = |query: &str, ours: bool| scan.future(params(query, ours, START)).turns;
        assert_eq!(read("", true)[..2], [first, first + 2]);
        // Digits read as part of a slot number.
        assert_eq!(read("1009", false), [first + 2]);
        assert!(read("nobody", false).is_empty());
    }

    #[test]
    fn test_a_version_is_named_whole_or_by_its_start_at_a_dot() {
        let holds = |op: Op, values: &[&str], version: Option<&str>| {
            version_listed(&is(Field::Version, op, values), version)
        };
        assert!(holds(Op::Is, &["4.4.1"], Some("4.4.1")));
        assert!(holds(Op::Is, &["4.4"], Some("4.4.1")));
        assert!(holds(Op::Is, &["4.4.0"], Some("4.4.0-rc.1")));
        assert!(holds(Op::Is, &["3", "4"], Some("4.4.1")));
        assert!(!holds(Op::Is, &["4.4"], Some("4.40.1")));
        assert!(!holds(Op::Is, &["4.4"], None));
        assert!(holds(Op::IsNot, &["4.3"], Some("4.4.1")));
        assert!(!holds(Op::IsNot, &["4.4"], Some("4.4.1")));
        assert!(!holds(Op::IsNot, &["4.4"], None));
    }

    #[test]
    fn test_a_full_page_of_turns_to_come_fits_the_message_ceiling() {
        let keys: Vec<String> = (0..MAX_FUTURE_TURNS)
            .map(|_| Pubkey::new_unique().to_string())
            .collect();
        let map: LeaderMap = keys
            .iter()
            .map(|key| {
                let traits = LeaderTraits {
                    client: Some("HarmonicFiredancer".repeat(4)),
                    version: Some("255.255.255-rc.255".into()),
                    ip: Some("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff".into()),
                    country: Some("DE"),
                    stake: u64::MAX,
                    delinquent: false,
                    last_vote: None,
                };
                (Pubkey::from_str(key).unwrap(), traits)
            })
            .collect();
        let scan = Scan {
            map: RwLock::new(map),
            ..scan(SlotHistory::new(4_096), &keys)
        };
        let reply = scan.future(params("", false, START));
        assert_eq!(reply.turns.len(), MAX_FUTURE_TURNS);
        assert_eq!(reply.leaders.len(), MAX_FUTURE_TURNS);
        let encoded = proto::encode_with_id("slot", "future", Some(1), &reply);
        proto::assert_under("a full page", &encoded, proto::MAX_MESSAGE / 2);
    }

    #[test]
    fn test_every_leader_is_listed_within_the_message_ceiling() {
        // More than twice mainnet's staked nodes, each with the longest figures.
        let map: LeaderMap = (0..2_000)
            .map(|_| {
                let traits = LeaderTraits {
                    client: Some("JitoLabs".to_string()),
                    version: Some("4.4.1-jito.123".to_string()),
                    ip: Some("255.255.255.255".to_string()),
                    country: Some("US"),
                    stake: u64::MAX,
                    delinquent: false,
                    last_vote: None,
                };
                (Pubkey::new_unique(), traits)
            })
            .collect();
        let rows = leader_rows(&map);
        assert_eq!(rows.len(), 2_000);
        let encoded = proto::encode_with_id("peers", "leaders", Some(1), &rows);
        proto::assert_under("every leader", &encoded, proto::MAX_MESSAGE / 2);
    }

    #[test]
    fn test_a_turns_gap_runs_to_its_leaders_next_turn_or_the_epochs_end() {
        let mut held = epoch(&["a", "b"]);
        held.turns = vec![0, 1, 0, 0, 1, 1];
        held.end_slot = START + 39;
        // Turns of four slots: a at 0, 2 and 3; b at 1, 4 and 5; then 16 slots to the epoch's end.
        assert_eq!(turn_gaps(&held), [4, 8, 0, 24, 0, 16]);
    }

    #[test]
    fn test_a_gap_condition_keeps_the_turns_with_a_long_enough_gap() {
        let mut scan = two_leaders(SlotHistory::new(4_096));
        scan.epochs[0].turns = vec![0, 1, 0, 0, 1, 1];
        scan.epochs[0].end_slot = START + 23;
        let first = START / SLOTS_PER_TURN;
        let reply = scan.future(where_(vec![cmp(Field::Gap, Op::Ge, 8.0)], START));
        assert_eq!(reply.turns, [first + 1, first + 3], "gaps of 8 slots");
        // Held turns answer the same condition.
        scan.history = RwLock::new(rows(START, START + 23, |_, _| {}));
        let found = scan.search(where_(vec![cmp(Field::Gap, Op::Ge, 4.0)], START + 24));
        assert_eq!(found.turns.len(), 3, "turns 0, 1 and 3");
    }

    #[test]
    fn test_turns_to_come_name_the_one_validator_chosen() {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let scan = Scan {
            info: named(&ours, "Lantern"),
            ..scan(SlotHistory::new(4_096), &[ours, other])
        };
        let read = |conditions| scan.future(where_(conditions, START)).named;
        let name = |op: Op, value: &str| is(Field::Name, op, &[value]);
        assert_eq!(read(vec![name(Op::Is, "lantern")]), Some(ours.to_string()));
        assert_eq!(
            read(vec![name(Op::Is, "lantern"), cmp(Field::Gap, Op::Gt, 1e9)]),
            Some(ours.to_string()),
            "still chosen when no turn matches"
        );
        assert_eq!(read(vec![name(Op::Is, "nobody")]), None);
        assert_eq!(
            read(vec![name(Op::IsNot, "lantern")]),
            None,
            "left out, not chosen"
        );
        assert_eq!(read(Vec::new()), None);
        // Part of both keys: not one validator.
        let (first, second) = (ours.to_string(), other.to_string());
        let shared = first
            .chars()
            .find(|part| second.contains(*part))
            .expect("two keys share a character")
            .to_string();
        assert_eq!(read(vec![name(Op::Is, &shared)]), None);
    }
}
