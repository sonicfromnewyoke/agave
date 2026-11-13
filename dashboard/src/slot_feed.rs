//! The slot strip, the slot pages' history, the slot positions and the blocks and turns we
//! produced, on a 200 ms tick. Each slot's first shred, bank, block, footer, confirmation and root
//! come from the validator's ledger; the leader schedule, the gossip table, the tip accounts and
//! each frozen bank's counts from RPC; a block's fill, replay, cost and fees, and the TPU path's
//! totals, from the validator's metrics.
//!
//! The ledger names every slot the rooted chain passes, so a slot below the root that never froze
//! here and is not in that chain was skipped, or its frames went missing on the way. Each such slot
//! is called skipped only once the SlotHistory sysvar, read over RPC, agrees.

mod alpenglow;
mod cluster;

use {
    crate::{
        certs::FooterView,
        collect::{
            BEHIND_WALK_BLOCKS, EXECUTION_WAIT_SLOTS, EpochInfo, FINALITY_WINDOW_MILLIS,
            FULL_REACH_SLOTS, HANDOFF_BEHIND_SLOTS, HANDOFF_RUN_TURNS, IN_STEP_SLOTS, LEADER_TURNS,
            NEXT_LEADER_LOOKAHEAD, OVERVIEW_INTERVAL, PRODUCED_BLOCKS, REPLAY_RATE_WINDOW, Replies,
            SLOT_HISTORY, SLOT_OVERVIEW_LEN, WORKER_REPORT_MILLIS,
            block_life::{BlockLifeSummary, BlockLives},
            build_execution, classify_slot, execution_window,
            handoff::{HandoffNow, HandoffSummary, Handoffs},
            measure_replay_rate,
            slot_clock::SlotClock,
            system_time_nanos,
        },
        frames::{Frame, FrozenBlock, Status},
        history::SlotHistory,
        meter_source::WorkingReading,
        meters::QuicPort,
        metrics_tap::{
            BankFigures, COUNTER_STEP_MILLIS, MetricsTap, TapCounters, WindowedCounters,
            insert_capped, keep_newest,
        },
        produced::{ProducedBlock, held_from},
        proto::{Publisher, TOPIC_SLOT, TOPIC_SUMMARY},
        remote::{Remote, Shared},
        slots::{BlockDetail, ShredArrival, SlotEntry, SlotLevel, SlotRing},
        tips::TipMeter,
        turns::{LeaderTurn, TurnTracker},
        validator_info::ValidatorInfoCache,
    },
    crossbeam_channel::Receiver,
    solana_clock::Slot,
    solana_core::validator::ValidatorStartProgress,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    solana_slot_history::Check,
    solana_time_utils::timestamp,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
        sync::{
            Arc, RwLock,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant, SystemTime},
    },
};

pub const TICK: Duration = Duration::from_millis(200);
/// The cluster-wide passes, which run only with a viewer.
const SLOW_TICK: Duration = Duration::from_secs(5);
/// First shreds held until their slot is timed; the clock times up to 512 slots a tick.
const FIRST_SHREDS_KEPT: usize = 8192;
/// How long a block waits for its cost, fees and replay times, which the metrics export sends in
/// batches every ten seconds.
const FIGURES_WAIT: Duration = Duration::from_secs(60);
/// How far timing may trail the completed slot while it waits for slots' fills: past this, a slot
/// is timed by the first-shred stamp read off the ledger, as it is where the metrics never come.
const FILL_WAIT_SLOTS: u64 = 64;
/// Rooted slots, parents and footers held: twice the strip.
const CHAIN_KEPT: usize = 8192;
/// A turn waits this long for the metrics to pass its end, then takes the counters as they stand.
const TURN_WAIT_MILLIS: u64 = 60_000;
/// How long a frozen block waits for RPC's leader schedule to name its leader before it goes on as
/// another leader's.
const SCHEDULE_WAIT: Duration = Duration::from_secs(30);

debounced_keys! {
    struct Keys {
        root_slot: Slot = (TOPIC_SUMMARY, "root_slot"),
        optimistically_confirmed_slot: Slot = (TOPIC_SUMMARY, "optimistically_confirmed_slot"),
        finalized_slot: Slot = (TOPIC_SUMMARY, "finalized_slot"),
        completed_slot: Slot = (TOPIC_SUMMARY, "completed_slot"),
        estimated_slot: Slot = (TOPIC_SUMMARY, "estimated_slot"),
        observed_slot_duration_nanos: Option<u64> = (TOPIC_SUMMARY, "observed_slot_duration_nanos"),
        finality_from_leader_micros: Option<u64> = (TOPIC_SUMMARY, "finality_from_leader_micros"),
        replay_rate: Option<f64> = (TOPIC_SUMMARY, "replay_rate"),
        behind_cluster: Option<u64> = (TOPIC_SUMMARY, "behind_cluster"),
        next_leader_slot: Option<Slot> = (TOPIC_SUMMARY, "next_leader_slot"),
        block_life: Option<BlockLifeSummary> = (TOPIC_SUMMARY, "block_life"),
        handoff: Option<HandoffSummary> = (TOPIC_SUMMARY, "handoff"),
        handoff_now: Option<HandoffNow> = (TOPIC_SUMMARY, "handoff_now"),
        block_height: u64 = (TOPIC_SUMMARY, "block_height"),
    }
}

/// A block still owed figures that only the metrics or RPC's slot updates carry.
#[derive(Debug, Clone, Copy)]
struct Awaited {
    since: Instant,
    figures: bool,
    stats: bool,
    tips: bool,
    replay: bool,
    /// The cluster's tip when the block froze, which its life is judged against.
    tip: Option<Slot>,
}

/// What one validator process has told the feed; started again for another.
#[derive(Default)]
struct Followed {
    slots: SlotRing,
    clock: SlotClock,
    first_shreds: BTreeMap<Slot, u64>,
    awaited: BTreeMap<Slot, Awaited>,
    /// Blocks of ours, by slot, waiting for their figures and first shred to be captured.
    capture: BTreeMap<Slot, String>,
    /// Frozen blocks whose leader the schedule does not name yet, in the order they froze, with
    /// when they came.
    unplaced: Vec<(FrozenBlock, Instant)>,
    root: Slot,
    /// Every slot the rooted chain passed, as the ledger names them.
    rooted: BTreeSet<Slot>,
    /// Each bank's parent, for walking the tip's chain.
    parents: BTreeMap<Slot, Slot>,
    /// Each frozen block's transactions summed along its chain, from the first block seen on it.
    transactions: BTreeMap<Slot, u64>,
    /// The newest bank created, as bank forks' highest slot.
    highest: Slot,
    /// The newest slot a shred or a bank was seen for, as the blockstore's highest slot.
    shredded: Slot,
    /// The newest slot optimistically confirmed, as the blockstore's latest optimistic slot.
    optimistic: Slot,
    /// The newest bank frozen, and when it froze here.
    completed: Slot,
    completed_at: Since,
    first_observed_slot: Option<Slot>,
    leaders_resolved_to: Slot,
    completed_window: VecDeque<(Instant, Slot)>,
    /// Whether the admin socket has reported the validator running.
    ran: bool,
    /// The newest completed slot whose first shred has arrived.
    newest_arrival: Slot,
    cluster_tip: Option<Slot>,
    /// The cluster's tip as each slot's first shred or bank was seen. A slot is timed when its fill
    /// report comes, seconds later, so it is judged against the tip it arrived to.
    arrival_tips: BTreeMap<Slot, Slot>,
    /// Whether a tip ahead of replay has been seen, and whether replay has since drawn level.
    trailed_cluster: bool,
    caught_cluster: bool,
    caught_up_millis: Option<u64>,
    footer_views: BTreeMap<Slot, FooterView>,
    /// The tip accounts' writes in each slot, the last one standing, and their balances at the end
    /// of each frozen bank measured.
    tip_writes: BTreeMap<Slot, HashMap<Pubkey, u64>>,
    tip_balances: BTreeMap<Slot, Vec<u64>>,
    /// Slots whose tips were measured before a write of theirs, or of a slot before them, was
    /// reported: RPC's notification of a write can follow the ledger's block.
    tips_again: BTreeSet<Slot>,
    /// The slot of the read the held balances count from.
    tip_base_slot: Option<Slot>,
    /// Slots below the root that are neither frozen here nor in the rooted chain.
    unresolved: BTreeSet<Slot>,
    turns: TurnTracker,
    turns_scanned_to: Slot,
    turn_reference: TapCounters,
    turn_drained: Option<u64>,
    /// Turns that ended and when, waiting for the metrics to pass that moment.
    turns_ending: VecDeque<(Slot, Slot, u64)>,
    held_from: Slot,
    produced_settled_to: Slot,
    execution_pending: BTreeSet<Slot>,
    handoffs: Handoffs,
    handoff_now_for: Option<(Slot, bool, Option<u64>)>,
    block_lives: BlockLives,
    next_leader_for: Option<(Slot, Option<Pubkey>)>,
}

/// The strip's ring.
impl Default for SlotRing {
    fn default() -> Self {
        Self::new(SLOT_HISTORY)
    }
}

/// An instant that starts as the moment it is made.
struct Since(Instant);

impl Default for Since {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Followed {
    fn note_status(&mut self, slot: Slot, parent: Option<Slot>, status: Status) {
        match status {
            Status::FirstShredReceived { at_millis } => {
                self.first_shreds.entry(slot).or_insert(at_millis);
                keep_newest(&mut self.first_shreds, FIRST_SHREDS_KEPT);
                self.shredded = self.shredded.max(slot);
                self.note_arrival_tip(slot);
            }
            Status::CreatedBank => {
                self.highest = self.highest.max(slot);
                self.shredded = self.shredded.max(slot);
                if let Some(parent) = parent {
                    insert_capped(&mut self.parents, slot, parent, CHAIN_KEPT);
                }
                self.note_arrival_tip(slot);
            }
            Status::Rooted => {
                self.root = self.root.max(slot);
                self.rooted.insert(slot);
                while self.rooted.len() > CHAIN_KEPT {
                    self.rooted.pop_first();
                }
            }
            Status::Confirmed => self.optimistic = self.optimistic.max(slot),
        }
    }

    fn complete(&mut self, slot: Slot) {
        if slot > self.completed {
            self.completed = slot;
            self.completed_at = Since(Instant::now());
        }
    }

    fn mine(&self, slot: Slot) -> bool {
        self.slots.get(slot).is_some_and(|entry| entry.mine)
    }

    /// The newest slot seen or optimistically confirmed.
    fn tip(&self) -> Option<Slot> {
        let tip = self.shredded.max(self.optimistic);
        (tip > 0).then_some(tip)
    }

    fn note_arrival_tip(&mut self, slot: Slot) {
        if let Some(tip) = self.tip() {
            self.arrival_tips.entry(slot).or_insert(tip);
            keep_newest(&mut self.arrival_tips, CHAIN_KEPT);
        }
    }
}

pub struct SlotFeed {
    publisher: Arc<Publisher>,
    history: Arc<RwLock<SlotHistory>>,
    /// This epoch's record and the previous one's, for the server.
    epochs: Arc<RwLock<Vec<EpochInfo>>>,
    info_cache: Arc<RwLock<ValidatorInfoCache>>,
    tips: Option<TipMeter>,
    tap: Arc<MetricsTap>,
    remote: Shared,
    replies: Arc<Replies>,
    frames: Receiver<Frame>,
    /// Frames dropped on their way to this feed.
    frames_dropped: Arc<AtomicU64>,
    keys: Keys,
    cluster: cluster::ClusterKeys,
    certificates: alpenglow::Certificates,
    followed: Followed,
    /// The validator process the frames come from.
    pid: Option<u32>,
    overview_dirty: bool,
    overview_retained_at: Instant,
    overview_unsent: bool,
    joined: u64,
    /// Owed whole to the next viewer, as the overview is.
    produced_unsent: bool,
    last_slow_tick: Instant,
}

impl SlotFeed {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        publisher: Arc<Publisher>,
        history: Arc<RwLock<SlotHistory>>,
        tap: Arc<MetricsTap>,
        remote: Shared,
        replies: Arc<Replies>,
        frames: Receiver<Frame>,
        frames_dropped: Arc<AtomicU64>,
        epochs: Arc<RwLock<Vec<EpochInfo>>>,
        info_cache: Arc<RwLock<ValidatorInfoCache>>,
        tips: Option<TipMeter>,
    ) -> Self {
        let now = Instant::now();
        Self {
            publisher,
            history,
            epochs,
            info_cache,
            tips,
            tap,
            remote,
            replies,
            frames,
            frames_dropped,
            keys: Keys::default(),
            cluster: cluster::ClusterKeys::default(),
            certificates: alpenglow::Certificates::default(),
            followed: Followed::default(),
            pid: None,
            overview_dirty: false,
            overview_retained_at: now.checked_sub(OVERVIEW_INTERVAL).unwrap_or(now),
            overview_unsent: false,
            joined: 0,
            produced_unsent: false,
            last_slow_tick: now.checked_sub(SLOW_TICK).unwrap_or(now),
        }
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        let frozen = self.take_frames();
        let frozen = self.place(frozen);
        self.collect_gossip_stake();
        let finalized = self.remote.read().unwrap().finalized;
        // The root stands for the newest frozen bank until the ledger gives a block.
        if self.followed.completed == 0
            && let Some(root) = finalized
        {
            self.followed.complete(root);
        }
        self.note_working();
        self.collect_slot_positions(finalized);
        let (confirmed, finalized) = (self.followed.optimistic, finalized.unwrap_or_default());
        self.hold_produced_from();
        let cluster_tip = self.followed.tip();
        self.followed.cluster_tip = cluster_tip;
        // The distance waits for the first block, or a node in step would read as far behind.
        let completed = self.followed.completed;
        let behind_cluster = cluster_tip
            .filter(|_| completed > 0)
            .map(|tip| self.blocks_behind(tip, completed));
        self.mark_caught_cluster(behind_cluster);
        self.remote.write().unwrap().behind_cluster = behind_cluster;
        self.keys
            .behind_cluster
            .publish(&self.publisher, behind_cluster);
        self.collect_leaders();
        self.collect_slot_levels(&frozen, confirmed, finalized);
        self.remeasure_tips();
        self.fill_awaited();
        self.resolve_skips();
        self.settle_produced();
        self.collect_turns();
        self.collect_vote_certs();
        self.collect_finalized_paths();
        self.collect_earnings();
        self.collect_vote();

        let subscribers = self.publisher.subscriber_count();
        let joined = self.publisher.joined();
        if std::mem::replace(&mut self.joined, joined) != joined {
            self.overview_unsent = true;
        }
        if subscribers == 0 {
            if std::mem::take(&mut self.overview_dirty) {
                self.publisher.forget(TOPIC_SLOT, "overview");
                self.overview_unsent = true;
            }
        } else if self.overview_unsent
            || (self.overview_dirty
                && now.duration_since(self.overview_retained_at) >= OVERVIEW_INTERVAL)
        {
            self.publish_slot_overview();
            self.overview_dirty = false;
            self.overview_retained_at = now;
        }
        if subscribers > 0 {
            if self.produced_unsent {
                self.publish_produced();
            }
            self.collect_handoff_now();
            self.collect_cluster_charts();
        }

        if subscribers > 0 && now.duration_since(self.last_slow_tick) >= SLOW_TICK {
            self.last_slow_tick = now;
            let rate = measure_replay_rate(&self.followed.completed_window)
                .map(|rate| (rate * 10.0).round() / 10.0);
            self.keys.replay_rate.publish(&self.publisher, rate);
            let lives = self.followed.block_lives.summary(timestamp());
            self.keys.block_life.publish(&self.publisher, lives);
            self.publish_handoffs();
            self.collect_peers();
            self.collect_skip_rate();
            self.collect_leader_traits();
            self.collect_peer_table();
            self.collect_gossip_peers();
            self.collect_finalization_share();
            self.collect_cert_formation();
            self.collect_miss_list();
            self.fill_certificates();
            // As the ledger reader fills in versions and fees, and the passes above certificates.
            self.publish_produced();
        }
    }

    /// Frozen blocks in the order they froze, each once the schedule names its leader: a block of
    /// ours taken for another leader's would be timed as replayed and never captured. One the
    /// schedule leaves unnamed for [`SCHEDULE_WAIT`] goes on as another leader's.
    fn place(&mut self, frozen: Vec<FrozenBlock>) -> Vec<FrozenBlock> {
        let now = Instant::now();
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let waiting = std::mem::take(&mut self.followed.unplaced);
        let mut placed = Vec::new();
        for (block, since) in waiting
            .into_iter()
            .chain(frozen.into_iter().map(|block| (block, now)))
        {
            match remote.is_ours(block.slot) {
                Some(mine) => {
                    if let Some(entry) = self.followed.slots.set_mine(block.slot, mine) {
                        self.publish_slot(&entry);
                    }
                    placed.push(block);
                }
                None if now.duration_since(since) < SCHEDULE_WAIT => {
                    self.followed.unplaced.push((block, since));
                }
                None => placed.push(block),
            }
        }
        placed
    }

    /// Applies the frames received since the last tick, and returns the banks they froze.
    fn take_frames(&mut self) -> Vec<FrozenBlock> {
        let mut frozen = Vec::new();
        for frame in self.frames.try_iter().collect::<Vec<_>>() {
            match frame {
                Frame::Validator { pid } => {
                    if self.pid.is_some_and(|followed| followed != pid) {
                        log::info!(
                            "dashboard: validator {pid} replaced the one followed; the slot strip \
                             starts again"
                        );
                        self.followed = Followed::default();
                        frozen.clear();
                    }
                    self.pid = Some(pid);
                }
                Frame::Slot {
                    slot,
                    parent,
                    status,
                } => self.followed.note_status(slot, parent, status),
                Frame::Block(block) => {
                    let (slot, parent_slot) = (block.slot, block.parent_slot);
                    let followed = &mut self.followed;
                    followed.complete(slot);
                    followed.parents.insert(slot, parent_slot);
                    let before = followed
                        .transactions
                        .get(&parent_slot)
                        .filter(|_| parent_slot < slot)
                        .copied()
                        .unwrap_or(0);
                    let total = before.saturating_add(block.executed_transactions);
                    insert_capped(&mut followed.transactions, slot, total, CHAIN_KEPT);
                    frozen.push(block);
                }
                Frame::Footer { slot, footer } => {
                    insert_capped(&mut self.followed.footer_views, slot, footer, CHAIN_KEPT);
                }
                Frame::Account {
                    slot,
                    pubkey,
                    lamports,
                } => {
                    let followed = &mut self.followed;
                    followed
                        .tip_writes
                        .entry(slot)
                        .or_default()
                        .insert(pubkey, lamports);
                    // The balances counted through this slot were counted without the write.
                    let stale = followed.tip_balances.split_off(&slot);
                    followed.tips_again.extend(stale.into_keys());
                    keep_newest(&mut followed.tip_writes, CHAIN_KEPT);
                }
            }
        }
        let lost = self.frames_dropped.swap(0, Ordering::Relaxed);
        if lost > 0 {
            log::warn!(
                "dashboard: {lost} frames from the validator went missing; slots that may have \
                 lost theirs are checked against the SlotHistory sysvar before they read as \
                 skipped"
            );
        }
        frozen
    }

    /// The newest frozen block, for the meters and RPC's poller: its place in the epoch, the
    /// transactions along its chain, and its height.
    fn note_working(&mut self) {
        let followed = &self.followed;
        let slot = followed.completed;
        if slot == 0 {
            return;
        }
        {
            let mut remote = self.remote.write().unwrap();
            let Some(schedule) = &remote.epoch_schedule else {
                return;
            };
            let (epoch, index) = schedule.get_epoch_and_slot_index(slot);
            let slots_in_epoch = schedule.get_slots_in_epoch(epoch);
            let alpenglow = remote.alpenglow == Some(true);
            let transactions = followed.transactions.get(&slot).copied().unwrap_or(0);
            remote.working = Some(WorkingReading {
                slot,
                transaction_count: transactions,
                non_vote_transaction_count: if alpenglow { transactions } else { 0 },
                epoch,
                epoch_start_slot: slot.saturating_sub(index),
                slots_in_epoch,
                alpenglow,
            });
        }
        let parent = |at: Slot| followed.parents.get(&at).copied();
        if let Some(height) = self.tap.block_height(slot, parent) {
            self.keys.block_height.publish(&self.publisher, height);
        }
    }

    /// Each position once known: nothing has reached this feed of a slot still at nought.
    fn collect_slot_positions(&mut self, finalized: Option<Slot>) {
        let followed = &self.followed;
        let (root, highest, completed) = (followed.root, followed.highest, followed.completed);
        let known = |slot: Slot| (slot > 0).then_some(slot);
        let positions = [
            (&mut self.keys.root_slot, known(root)),
            (
                &mut self.keys.optimistically_confirmed_slot,
                known(followed.optimistic),
            ),
            (&mut self.keys.finalized_slot, finalized),
            (&mut self.keys.estimated_slot, known(highest)),
            (&mut self.keys.completed_slot, known(completed)),
        ];
        for (key, slot) in positions {
            if let Some(slot) = slot {
                key.publish(&self.publisher, slot);
            }
        }
        self.collect_slot_durations(completed);
        let followed = &mut self.followed;
        followed.clock.mark_caught_up(
            followed.trailed_cluster && !followed.caught_cluster,
            completed,
        );
        self.observe_slot_duration(completed);
    }

    fn observe_slot_duration(&mut self, completed: Slot) {
        let now = Instant::now();
        let running = matches!(
            self.remote.read().unwrap().progress,
            Some(ValidatorStartProgress::Running)
        );
        let followed = &mut self.followed;
        // The window opens once startup is over: a hold for supermajority would drag the rate for
        // a window's length, and a start at nought would read as millions of slots a second.
        followed.ran |= running;
        if completed > 0 && followed.ran {
            followed.completed_window.push_back((now, completed));
        }
        while let Some((at, _)) = followed.completed_window.front() {
            if now.duration_since(*at) <= REPLAY_RATE_WINDOW {
                break;
            }
            followed.completed_window.pop_front();
        }
        self.keys
            .observed_slot_duration_nanos
            .publish(&self.publisher, self.followed.clock.observed_nanos());
        self.keys.finality_from_leader_micros.publish(
            &self.publisher,
            self.tap
                .finality_from_leader_micros(timestamp(), FINALITY_WINDOW_MILLIS),
        );
    }

    /// Timed from the blockstore's first-shred stamp, which a slot's fill report gives back once it
    /// arrives; slots are timed as their fills arrive, up to the completed slot.
    /// A slot that never filled is timed by the stamp of its first shred read off the ledger.
    fn collect_slot_durations(&mut self, up_to: Slot) {
        let epoch_schedule = self.remote.read().unwrap().epoch_schedule.clone();
        let mut changed = Vec::new();
        let mut filled = Vec::new();
        let filled_to = self
            .tap
            .newest_fill()
            .unwrap_or_default()
            .min(up_to)
            .max(up_to.saturating_sub(FILL_WAIT_SLOTS));
        for slot in self.followed.clock.slots_to_time(filled_to) {
            let Some(arrived) = self.first_shred(slot) else {
                continue;
            };
            let followed = &mut self.followed;
            followed.clock.record(slot, arrived);
            // A slot repaired while catching up arrived long after it was made.
            if (!followed.trailed_cluster || followed.caught_cluster)
                && let Some(schedule) = &epoch_schedule
            {
                followed
                    .clock
                    .record_in_epoch(schedule.get_epoch(slot), slot, arrived);
            }
            followed.newest_arrival = followed.newest_arrival.max(slot);
            let shreds = self.tap.shred_fill(slot).map(ShredArrival::from);
            let was_full = followed
                .slots
                .get(slot)
                .and_then(SlotEntry::full_at)
                .is_some();
            if let Some(entry) = followed.slots.update(slot, |entry| {
                entry.time_millis = Some(arrived);
                if entry.shreds.is_none() {
                    entry.shreds = shreds;
                }
            }) {
                if !was_full && entry.full_at().is_some() {
                    filled.push(slot);
                }
                changed.push(entry);
            }
            self.history.write().unwrap().record_time(slot, arrived);
        }

        // A slot timed before its fill arrived is timed again by the stamp the fill gives back.
        for slot in up_to.saturating_sub(FULL_REACH_SLOTS)..=up_to {
            let late = self
                .followed
                .slots
                .get(slot)
                .is_some_and(|entry| entry.time_millis.is_some() && entry.shreds.is_none());
            let Some(fill) = late.then(|| self.tap.shred_fill(slot)).flatten() else {
                continue;
            };
            let first = fill.first_shred_millis();
            if let Some(entry) = self.followed.slots.update(slot, |entry| {
                entry.shreds = Some(ShredArrival::from(fill));
                if first.is_some() {
                    entry.time_millis = first;
                }
            }) {
                filled.push(slot);
                changed.push(entry);
            }
            if let Some(first) = first {
                self.history.write().unwrap().record_time(slot, first);
            }
        }

        let retimed = self.retime(&filled, &mut changed);
        let touched: BTreeSet<Slot> = changed
            .iter()
            .flat_map(|entry| [entry.slot, entry.slot.saturating_add(1)])
            .collect();
        for slot in touched {
            self.note_handoff(slot);
        }
        changed.iter().for_each(|entry| self.publish_slot(entry));
        if retimed {
            self.publish_produced();
        }
    }

    /// Returns whether a produced block changed.
    fn retime(&mut self, filled: &[Slot], changed: &mut Vec<SlotEntry>) -> bool {
        let mut produced_changed = false;
        for &slot in filled {
            let next =
                (slot.saturating_add(1)..=slot.saturating_add(FULL_REACH_SLOTS)).find(|later| {
                    self.followed
                        .slots
                        .get(*later)
                        .and_then(SlotEntry::full_at)
                        .is_some()
                });
            for timed in std::iter::once(slot).chain(next) {
                let Some(duration) = self.full_duration(timed) else {
                    continue;
                };
                if let Some(entry) = self
                    .followed
                    .slots
                    .update(timed, |entry| entry.duration_nanos = Some(duration))
                {
                    changed.push(entry);
                }
                if self
                    .replies
                    .produced
                    .write()
                    .unwrap()
                    .set_duration(timed, duration)
                {
                    produced_changed = true;
                }
            }
        }
        produced_changed
    }

    fn full_duration(&self, slot: Slot) -> Option<u64> {
        let slots = &self.followed.slots;
        let end = slots.get(slot)?.full_at()?;
        let start = (slot.saturating_sub(FULL_REACH_SLOTS)..slot)
            .rev()
            .find_map(|earlier| slots.get(earlier)?.full_at())?;
        Some(end.saturating_sub(start).saturating_mul(1_000_000))
    }

    /// The gap before a leader's turn, once this slot's first shred and the previous slot's fill
    /// are both known.
    fn note_handoff(&mut self, slot: Slot) {
        let followed = &self.followed;
        let tip = followed
            .arrival_tips
            .get(&slot)
            .copied()
            .or(followed.cluster_tip);
        if followed.handoffs.holds(slot)
            || (followed.trailed_cluster && !followed.caught_cluster)
            || tip.is_some_and(|tip| tip.saturating_sub(slot) > HANDOFF_BEHIND_SLOTS)
        {
            return;
        }
        let Some(previous) = slot.checked_sub(1) else {
            return;
        };
        let (Some(arrived), Some(filled)) = (
            followed.slots.get(slot).and_then(|entry| entry.time_millis),
            followed.slots.get(previous).and_then(SlotEntry::full_at),
        ) else {
            return;
        };
        let (leader, before, ours) = {
            let remote = self.remote.read().unwrap();
            (
                remote.slot_leader(slot).copied(),
                remote.slot_leader(previous).copied(),
                remote.identity,
            )
        };
        let (Some(leader), Some(before)) = (leader, before) else {
            return;
        };
        if leader == before || Some(before) == ours {
            return;
        }
        let (Ok(arrived_at), Ok(filled_at)) = (i64::try_from(arrived), i64::try_from(filled))
        else {
            return;
        };
        self.followed
            .handoffs
            .note(slot, leader, arrived_at.saturating_sub(filled_at), arrived);
    }

    /// The leader whose turn is arriving now, from the first slot of its run of turns.
    fn collect_handoff_now(&mut self) {
        let slot = self.followed.newest_arrival;
        if slot == 0 {
            return;
        }
        let (first, leader) = {
            let remote = self.remote.read().unwrap();
            let (Some(leader), Some(schedule)) =
                (remote.slot_leader(slot).copied(), &remote.epoch_schedule)
            else {
                return;
            };
            let stride = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
            let (_, index) = schedule.get_epoch_and_slot_index(slot);
            let mut first = slot.saturating_sub(index.checked_rem(stride).unwrap_or(0));
            for _ in 0..HANDOFF_RUN_TURNS {
                let Some(before) = first.checked_sub(stride) else {
                    break;
                };
                if remote
                    .slot_leader(before)
                    .is_none_or(|earlier| *earlier != leader)
                {
                    break;
                }
                first = before;
            }
            (first, leader)
        };
        let followed = &mut self.followed;
        let key = (
            first,
            followed.handoffs.holds(first),
            followed.handoffs.computed_at(),
        );
        if followed.handoff_now_for == Some(key) {
            return;
        }
        followed.handoff_now_for = Some(key);
        let country = self.remote.read().unwrap().country(&leader);
        let now = followed.handoffs.now(&leader, country, first, timestamp());
        self.keys.handoff_now.publish(&self.publisher, Some(now));
    }

    fn publish_handoffs(&mut self) {
        let remote = self.remote.read().unwrap();
        let Some(ours) = remote.identity else {
            return;
        };
        let handoffs = &mut self.followed.handoffs;
        let summary = handoffs.summary(timestamp(), &ours, |key| remote.country(key));
        drop(remote);
        self.keys.handoff.publish(&self.publisher, summary);
        if let Some(leaders) = self.followed.handoffs.take_leaders()
            && let Ok(mut held) = self.replies.handoff_leaders.write()
        {
            *held = leaders;
        }
    }

    /// Blocks on the tip's chain that replay has yet to complete. Parents are known for the banks
    /// reported made; past them the rest is counted in slots.
    fn blocks_behind(&self, tip: Slot, completed: Slot) -> u64 {
        let mut slot = tip;
        let mut blocks = 0u64;
        while slot > completed && blocks < BEHIND_WALK_BLOCKS {
            let Some(parent) = self.followed.parents.get(&slot).copied() else {
                break;
            };
            blocks = blocks.saturating_add(1);
            slot = parent;
        }
        blocks.saturating_add(slot.saturating_sub(completed))
    }

    fn mark_caught_cluster(&mut self, behind_cluster: Option<u64>) {
        let followed = &mut self.followed;
        if followed.caught_cluster {
            return;
        }
        let Some(behind) = behind_cluster else {
            return;
        };
        if behind > IN_STEP_SLOTS {
            if !followed.trailed_cluster {
                self.history.write().unwrap().live_from = Slot::MAX;
            }
            followed.trailed_cluster = true;
            return;
        }
        if !followed.trailed_cluster {
            return;
        }
        followed.caught_cluster = true;
        followed.caught_up_millis = Some(timestamp());
        self.history.write().unwrap().live_from = followed.completed.saturating_add(1);
        self.publisher.publish(
            TOPIC_SUMMARY,
            "caught_up_time_nanos",
            &system_time_nanos(SystemTime::now()),
        );
    }

    /// Our slots from the first one seen, as far as RPC has given the schedule.
    fn collect_leaders(&mut self) {
        let highest = self.followed.highest;
        // No bank has been created since the feed began, so there is no tip to start from.
        if highest == 0 {
            return;
        }
        let from = match self.followed.first_observed_slot {
            None => {
                self.followed.first_observed_slot = Some(highest);
                highest
            }
            Some(_) => self
                .followed
                .leaders_resolved_to
                .max(highest.saturating_sub(SLOT_HISTORY as u64)),
        };
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        for slot in from..=highest {
            let Some(mine) = remote.is_ours(slot) else {
                self.followed.leaders_resolved_to = slot;
                break;
            };
            if let Some(entry) = self.followed.slots.set_mine(slot, mine) {
                self.publish_slot(&entry);
            }
            self.followed.leaders_resolved_to = slot.saturating_add(1);
        }

        // As the schedule cache's lookahead: our first slot past the highest bank that has no
        // shreds yet.
        let key = (highest, remote.identity);
        if self.followed.next_leader_for == Some(key) {
            return;
        }
        let Some(identity) = remote.identity else {
            return;
        };
        let first_shreds = &self.followed.first_shreds;
        let next = (highest.saturating_add(1)..=highest.saturating_add(NEXT_LEADER_LOOKAHEAD))
            .find(|slot| {
                remote.slot_leader(*slot) == Some(&identity) && !first_shreds.contains_key(slot)
            });
        drop(remote);
        self.followed.next_leader_for = Some(key);
        self.keys.next_leader_slot.publish(&self.publisher, next);
    }

    fn collect_slot_levels(&mut self, frozen: &[FrozenBlock], confirmed: Slot, finalized: Slot) {
        let root = self.followed.root;
        let mut changed = Vec::new();
        let mut became_full = Vec::new();
        for block in frozen {
            let slot = block.slot;
            self.observe_finalization(slot);
            let followed = &self.followed;
            let fresh = followed
                .slots
                .get(slot)
                .is_none_or(|entry| entry.block.is_none());
            let mine = followed.mine(slot);
            let shreds = fresh
                .then(|| self.tap.shred_fill(slot).map(ShredArrival::from))
                .flatten();
            let detail = fresh.then(|| block_detail(block, &self.remote.read().unwrap()));
            if fresh {
                // Its figures, stats, tips and replay time are filled in as they come.
                let awaited = Awaited {
                    since: Instant::now(),
                    figures: true,
                    stats: true,
                    tips: self.tips.is_some(),
                    replay: !mine,
                    tip: followed.cluster_tip,
                };
                let arrived = self.first_shred(slot).is_some();
                let followed = &mut self.followed;
                followed.awaited.insert(slot, awaited);
                if arrived {
                    followed.newest_arrival = followed.newest_arrival.max(slot);
                }
                if mine {
                    followed.capture.insert(slot, block.blockhash.clone());
                }
            }

            let level = classify_slot(slot, root, confirmed, finalized);
            let followed = &mut self.followed;
            let was_full = followed
                .slots
                .get(slot)
                .and_then(SlotEntry::full_at)
                .is_some();
            if let Some(entry) = followed.slots.update(slot, |entry| {
                entry.level = level;
                if let Some(detail) = &detail {
                    entry.block = Some(detail.clone());
                }
                if entry.shreds.is_none() {
                    entry.shreds = shreds;
                }
            }) {
                if !was_full && entry.full_at().is_some() {
                    became_full.push(slot);
                }
                self.note_life(&entry, self.followed.cluster_tip);
                changed.push(entry);
            }
        }
        let retimed = self.retime(&became_full, &mut changed);
        // The next slot's first shred usually arrives before this block fills.
        for slot in &became_full {
            self.note_handoff(*slot);
            self.note_handoff(slot.saturating_add(1));
        }

        let slots = &mut self.followed.slots;
        changed.extend(slots.promote(finalized, SlotLevel::Finalized));
        changed.extend(slots.promote(root, SlotLevel::Rooted));
        changed.extend(slots.promote(confirmed, SlotLevel::OptimisticallyConfirmed));
        changed.iter().for_each(|entry| self.publish_slot(entry));

        // The bundle stage reports a slot after its bank freezes.
        let tap = &self.tap;
        let filled = self
            .replies
            .produced
            .write()
            .unwrap()
            .fill_bundles(|slot| tap.bundles_landed(slot));
        let led = self
            .replies
            .produced
            .write()
            .unwrap()
            .fill_leading(|slot| tap.leader_timing(slot));
        let timed = self.fill_execution();
        if filled || led || timed || retimed {
            self.publish_produced();
        }
    }

    /// A block's stages once its first shred, fill and replay are all known, which the metrics
    /// bring after it froze; blocks this node caught up on are left out.
    fn note_life(&mut self, entry: &SlotEntry, tip: Option<Slot>) {
        let followed = &mut self.followed;
        if followed.trailed_cluster && !followed.caught_cluster {
            return;
        }
        let (Some(replayed), Some(shreds), Some(first)) =
            (entry.replayed_millis, entry.shreds, entry.time_millis)
        else {
            return;
        };
        followed
            .block_lives
            .note_arrival(entry.slot, first, &shreds, replayed, tip);
    }

    /// Fills in what the metrics brought since a block froze, until the wait runs out.
    fn fill_awaited(&mut self) {
        let mut changed = Vec::new();
        let mut captured = false;
        let awaited: Vec<(Slot, Awaited)> = self
            .followed
            .awaited
            .iter()
            .map(|(slot, awaited)| (*slot, *awaited))
            .collect();
        for (slot, mut awaited) in awaited {
            let Some(entry) = self.followed.slots.get(slot) else {
                self.followed.awaited.remove(&slot);
                continue;
            };
            let Some(mut block) = entry.block.clone() else {
                self.followed.awaited.remove(&slot);
                continue;
            };
            let mut replayed_millis = None;
            // The cost tracker sends no report for a bank without transactions.
            if awaited.figures
                && let Some(figures) = (block.transactions == 0)
                    .then(BankFigures::default)
                    .or_else(|| self.tap.bank_figures(slot))
            {
                block.block_cost = figures.block_cost;
                block.total_fees = figures.total_fees;
                block.priority_fees = figures.priority_fees;
                awaited.figures = false;
            }
            if awaited.stats
                && let Some(stats) = self.remote.read().unwrap().frozen.get(&slot)
            {
                block.failed_transactions = stats.num_failed_transactions;
                if entry.mine {
                    block.entries = stats.num_transaction_entries;
                }
                awaited.stats = false;
            }
            if awaited.tips
                && let Some(parent) = self.followed.parents.get(&slot).copied()
                && let Some(tips) = self.measure_tips(slot, parent)
            {
                block.tips = Some(tips);
                awaited.tips = false;
            }
            if awaited.replay
                && let Some(times) = self.tap.replayed(slot)
            {
                block.replay_micros = Some(times.serial());
                replayed_millis = self
                    .first_shred(slot)
                    .map(|first| times.observed_millis.saturating_sub(first));
                awaited.replay = false;
            }
            if let Some(entry) = self.followed.slots.update(slot, |entry| {
                entry.block = Some(block);
                if replayed_millis.is_some() {
                    entry.replayed_millis = replayed_millis;
                }
            }) {
                self.note_life(&entry, awaited.tip);
                changed.push(entry);
            }
            let expired = awaited.since.elapsed() >= FIGURES_WAIT;
            captured |= self.capture_if_ready(slot, expired);
            if (!awaited.figures && !awaited.stats && !awaited.tips && !awaited.replay) || expired {
                self.followed.awaited.remove(&slot);
            } else {
                self.followed.awaited.insert(slot, awaited);
            }
        }
        // A block of ours whose figures came at once may still wait for its first shred.
        let waiting: Vec<Slot> = self.followed.capture.keys().copied().collect();
        for slot in waiting {
            if !self.followed.awaited.contains_key(&slot) {
                captured |= self.capture_if_ready(slot, false);
            }
        }
        changed.iter().for_each(|entry| self.publish_slot(entry));
        if captured {
            self.publish_produced();
        }
    }

    /// Captures a block of ours once its figures and first shred are known, or as it stands once
    /// they stop being waited for. Returns whether it was captured.
    fn capture_if_ready(&mut self, slot: Slot, give_up: bool) -> bool {
        let Some(blockhash) = self.followed.capture.get(&slot) else {
            return false;
        };
        let Some(entry) = self.followed.slots.get(slot) else {
            self.followed.capture.remove(&slot);
            return false;
        };
        let Some(detail) = entry.block.as_ref() else {
            return false;
        };
        let figures_in = self
            .followed
            .awaited
            .get(&slot)
            .is_none_or(|awaited| !awaited.figures && !awaited.stats && !awaited.tips);
        let first = self.first_shred(slot);
        let stale = slot.saturating_add(FILL_WAIT_SLOTS) <= self.followed.completed;
        if !give_up && !(figures_in && (first.is_some() || stale)) {
            return false;
        }
        let block = ProducedBlock {
            slot,
            slot_time_millis: first,
            blockhash: blockhash.clone(),
            duration_nanos: entry.duration_nanos,
            detail: detail.clone(),
            bundles: self.tap.bundles_landed(slot),
            versions: None,
            execution: None,
            certificate: None,
            leading: self.tap.leader_timing(slot),
        };
        self.followed.capture.remove(&slot);
        if self.replies.produced.write().unwrap().insert(block) {
            self.followed.execution_pending.insert(slot);
            return true;
        }
        false
    }

    /// With the metrics' time standing for now: worker reports reach this process in batches.
    fn fill_execution(&mut self) -> bool {
        let now = timestamp().min(self.tap.points_through().unwrap_or(0));
        let mut changed = false;
        for slot in std::mem::take(&mut self.followed.execution_pending) {
            if !self.replies.produced.read().unwrap().contains(slot) {
                continue;
            }
            let pending = slot.saturating_add(EXECUTION_WAIT_SLOTS) > self.followed.completed;
            let window = self
                .first_shred(slot)
                .zip(self.tap.shred_fill(slot))
                .map(|(start, fill)| (start, fill.full_millis));
            let Some((start, full_millis)) = window else {
                if pending {
                    self.followed.execution_pending.insert(slot);
                }
                continue;
            };
            let settled = start
                .saturating_add(full_millis)
                .saturating_add(WORKER_REPORT_MILLIS.saturating_mul(2));
            let next = slot.saturating_add(1);
            let next_start = self
                .followed
                .mine(next)
                .then(|| self.first_shred(next))
                .flatten();
            // Our next block's start bounds this one's window, once its fill gives it back.
            if now < settled || (self.followed.mine(next) && next_start.is_none() && pending) {
                self.followed.execution_pending.insert(slot);
                continue;
            }
            let follows_ours = slot
                .checked_sub(1)
                .is_some_and(|previous| self.followed.mine(previous));
            let (from, to) = execution_window(start, follows_ours, next_start, settled);
            if now < to {
                self.followed.execution_pending.insert(slot);
                continue;
            }
            let workers = self.tap.worker_time(from, to);
            let votes = self.tap.vote_time(slot);
            let Some(execution) = build_execution(workers, votes, full_millis) else {
                continue;
            };
            if self
                .replies
                .produced
                .write()
                .unwrap()
                .set_execution(slot, execution)
            {
                changed = true;
            }
        }
        changed
    }

    /// Slots below the root that never froze here: rooted if the rooted chain passed them, else
    /// skipped once the SlotHistory sysvar agrees.
    fn resolve_skips(&mut self) {
        let root = self.followed.root;
        let mut changed = Vec::new();
        for slot in self.followed.slots.incomplete_below(root) {
            if self.followed.rooted.contains(&slot) {
                // Rooted, so produced: its bank's frames went missing.
                changed.extend(
                    self.followed
                        .slots
                        .update(slot, |entry| entry.level = SlotLevel::Rooted),
                );
            } else {
                self.followed.unresolved.insert(slot);
            }
        }
        if let Some(&newest) = self.followed.unresolved.last() {
            let history = {
                let mut remote = self.remote.write().unwrap();
                remote.slot_history_wanted = Some(newest);
                remote.slot_history.clone()
            };
            if let Some(history) = history {
                for slot in std::mem::take(&mut self.followed.unresolved) {
                    let level = match history.check(slot) {
                        Check::Future => {
                            self.followed.unresolved.insert(slot);
                            continue;
                        }
                        Check::Found => SlotLevel::Rooted,
                        Check::NotFound | Check::TooOld => SlotLevel::Skipped,
                    };
                    changed.extend(
                        self.followed
                            .slots
                            .update(slot, |entry| entry.level = level),
                    );
                }
            }
            if self.followed.unresolved.is_empty() {
                self.remote.write().unwrap().slot_history_wanted = None;
            }
        }
        changed.iter().for_each(|entry| self.publish_slot(entry));
    }

    /// A block of ours the cluster skipped is not a produced block. One the rooted chain does not
    /// name may have lost its frame, so it waits for the SlotHistory sysvar.
    fn settle_produced(&mut self) {
        let root = self.followed.root;
        let after = self.followed.produced_settled_to;
        if after >= root {
            return;
        }
        let history = self.remote.read().unwrap().slot_history.clone();
        let rooted = &self.followed.rooted;
        let unknown = self
            .replies
            .produced
            .read()
            .unwrap()
            .slots_in(after.saturating_add(1), root)
            .find(|slot| {
                !rooted.contains(slot)
                    && history
                        .as_ref()
                        .is_none_or(|history| history.check(*slot) == Check::Future)
            });
        let settle_to = match unknown {
            Some(slot) => {
                let mut remote = self.remote.write().unwrap();
                remote.slot_history_wanted =
                    Some(remote.slot_history_wanted.unwrap_or(0).max(slot));
                slot.saturating_sub(1)
            }
            None => root,
        };
        let rooted = &self.followed.rooted;
        let dropped =
            self.replies
                .produced
                .write()
                .unwrap()
                .drop_unrooted(after, settle_to, |slot| {
                    rooted.contains(&slot)
                        || history
                            .as_ref()
                            .is_some_and(|history| history.check(slot) == Check::Found)
                });
        self.followed.produced_settled_to = settle_to.max(after);
        if dropped {
            // No message takes a block back, so the lists go whole.
            self.produced_unsent = true;
            self.publish_turns();
            self.publish_produced();
        }
    }

    /// This epoch's blocks, and the previous epoch's until a share of this one has passed.
    fn hold_produced_from(&mut self) {
        let root = self.followed.root;
        let Some(schedule) = self.remote.read().unwrap().epoch_schedule.clone() else {
            return;
        };
        if root == 0 {
            return;
        }
        let (epoch, index) = schedule.get_epoch_and_slot_index(root);
        let floor = held_from(
            schedule.get_first_slot_in_epoch(epoch),
            schedule.get_first_slot_in_epoch(epoch.saturating_sub(1)),
            index,
            schedule.get_slots_in_epoch(epoch),
        );
        if floor == self.followed.held_from {
            return;
        }
        self.followed.held_from = floor;
        self.replies.produced.write().unwrap().hold_from(floor);
        self.tap.hold_slots_from(floor);
        self.publisher
            .publish(TOPIC_SUMMARY, "produced_floor", &floor);
        self.publish_produced();
        self.publish_turns();
    }

    /// Once the cluster passes a turn, credits it with the change in the TPU path's totals since
    /// the previous turn ended. Those totals come in the metrics' batches, so each turn waits until
    /// the points made after it ended have been read, and takes the totals as they stood then.
    fn collect_turns(&mut self) {
        let followed = &mut self.followed;
        let from = followed
            .turns_scanned_to
            .max(followed.first_observed_slot.unwrap_or(0));
        for slot in from..followed.leaders_resolved_to {
            let mine = followed.mine(slot);
            followed.turns.observe(slot, mine);
        }
        followed.turns_scanned_to = followed.leaders_resolved_to.max(from);
        let now = timestamp();
        for (first, last) in followed.turns.ended(followed.completed) {
            followed.turns_ending.push_back((first, last, now));
        }

        let through = self.tap.points_through().unwrap_or(0);
        let mut credited = false;
        while let Some(&(first, last, ended_at)) = self.followed.turns_ending.front() {
            let passed = through > ended_at.saturating_add(COUNTER_STEP_MILLIS);
            let reading = if passed {
                self.tap.counters_at(ended_at)
            } else if now.saturating_sub(ended_at) >= TURN_WAIT_MILLIS {
                Some(self.tap.counters())
            } else {
                None
            };
            let Some(reading) = reading else {
                break;
            };
            self.followed.turns_ending.pop_front();
            let reference = self.followed.turn_reference;
            let mut store = self.replies.produced.write().unwrap();
            let produced = store.produced_in(first, last);
            store.add_turn(LeaderTurn {
                first,
                last,
                produced,
                drained_millis: ended_at,
                since_millis: self.followed.turn_drained,
                quic: QuicPort {
                    name: "tpu",
                    counts: reading.quic.totals.since(&reference.quic.totals),
                    levels: reading.quic.levels,
                    kernel_drops: None,
                },
                verify: reading.verify.since(&reference.verify),
                executed: reading.executed.since(&reference.executed),
            });
            drop(store);
            self.followed.turn_reference = reading;
            self.followed.turn_drained = Some(ended_at);
            credited = true;
        }
        if credited {
            self.publish_turns();
        }
    }

    fn publish_turns(&self) {
        let recent = self
            .replies
            .produced
            .read()
            .unwrap()
            .recent_turns(LEADER_TURNS);
        self.publisher
            .publish(TOPIC_SUMMARY, "produced_turns", &recent);
    }

    /// The whole list for a client connecting, dropped while nobody watches; each changed block to
    /// one connected.
    fn publish_produced(&mut self) {
        let changed = self.replies.produced.write().unwrap().take_changed();
        if self.publisher.subscriber_count() == 0 {
            self.publisher.forget(TOPIC_SUMMARY, "produced_blocks");
            self.produced_unsent = true;
            return;
        }
        let send = std::mem::take(&mut self.produced_unsent);
        {
            let store = self.replies.produced.read().unwrap();
            let recent = store.recent_refs(PRODUCED_BLOCKS);
            if send {
                self.publisher
                    .publish(TOPIC_SUMMARY, "produced_blocks", &recent);
            } else {
                self.publisher
                    .retain_only(TOPIC_SUMMARY, "produced_blocks", &recent);
            }
        }
        for block in changed {
            self.publisher
                .publish_update(TOPIC_SUMMARY, "produced_block", block.slot, &block);
        }
    }

    /// The blockstore's stamp of the slot's first shred: from its fill report, else as read off
    /// the ledger.
    fn first_shred(&self, slot: Slot) -> Option<u64> {
        self.tap
            .shred_fill(slot)
            .and_then(|fill| fill.first_shred_millis())
            .or_else(|| self.followed.first_shreds.get(&slot).copied())
    }

    fn publish_slot(&mut self, entry: &SlotEntry) {
        self.history.write().unwrap().record(entry);
        if !self.overview_unsent {
            self.publisher
                .publish_update(TOPIC_SLOT, "update", entry.slot, entry);
        }
        self.overview_dirty = true;
    }

    fn publish_slot_overview(&mut self) {
        let overview = self.followed.slots.overview(SLOT_OVERVIEW_LEN);
        if std::mem::take(&mut self.overview_unsent) {
            self.publisher.publish(TOPIC_SLOT, "overview", &overview);
        } else {
            self.publisher
                .retain_only(TOPIC_SLOT, "overview", &overview);
        }
    }

    /// The tips of the blocks in `tips_again`, oldest first, each from its parent's balances.
    fn remeasure_tips(&mut self) {
        for slot in std::mem::take(&mut self.followed.tips_again) {
            let followed = &self.followed;
            let measured = followed
                .slots
                .get(slot)
                .and_then(|entry| entry.block.as_ref())
                .is_some_and(|block| block.tips.is_some());
            let Some(parent) = followed.parents.get(&slot).copied().filter(|_| measured) else {
                continue;
            };
            let Some(tips) = self.measure_tips(slot, parent) else {
                continue;
            };
            let changed = self.followed.slots.update(slot, |entry| {
                if let Some(block) = &mut entry.block {
                    block.tips = Some(tips);
                }
            });
            if let Some(entry) = changed {
                self.publish_slot(&entry);
            }
            self.replies.produced.write().unwrap().set_tips(slot, tips);
        }
    }

    /// A frozen block's tips, from the tip accounts' balances against its parent's. `None` without
    /// a tip program, or while RPC's read of the balances they count from is awaited.
    fn measure_tips(&mut self, slot: Slot, parent: Slot) -> Option<u64> {
        let meter = self.tips.as_mut()?;
        let base = self.remote.read().unwrap().tip_base.clone()?;
        let followed = &mut self.followed;
        // Balances counted from another read are not on this one.
        if followed.tip_base_slot.replace(base.0) != Some(base.0) {
            followed.tip_balances.clear();
        }
        let accounts = meter.accounts().to_vec();
        let mut total = |slot| {
            tip_balances(&mut self.followed, &accounts, &base, slot).map(|balances| {
                balances
                    .iter()
                    .fold(0u64, |sum, lamports| sum.saturating_add(*lamports))
            })
        };
        let (now, before) = (total(slot)?, total(parent)?);
        Some(meter.measure_total(now, before))
    }
}

/// A slot's tip account balances: the read at the base slot's, with the writes of each slot after
/// it on the slot's fork. `None` for a slot below the base, or off a fork whose parents are known.
fn tip_balances(
    followed: &mut Followed,
    accounts: &[Pubkey],
    (base_slot, base): &(Slot, Vec<u64>),
    slot: Slot,
) -> Option<Vec<u64>> {
    let mut path = Vec::new();
    let mut at = slot;
    let mut balances = loop {
        if let Some(held) = followed.tip_balances.get(&at) {
            break held.clone();
        }
        if at == *base_slot {
            break base.clone();
        }
        if at < *base_slot {
            return None;
        }
        path.push(at);
        at = *followed.parents.get(&at)?;
    };
    for slot in path.into_iter().rev() {
        if let Some(writes) = followed.tip_writes.get(&slot) {
            for (balance, account) in balances.iter_mut().zip(accounts) {
                if let Some(lamports) = writes.get(account) {
                    *balance = *lamports;
                }
            }
        }
        followed.tip_balances.insert(slot, balances.clone());
    }
    keep_newest(&mut followed.tip_balances, CHAIN_KEPT);
    Some(balances)
}

/// A block's figures from its frame, before RPC's slot update and the cost tracker's report fill in
/// the rest. Entries are those holding transactions, as the bank counts them. Under Alpenglow votes
/// are no transactions.
fn block_detail(block: &FrozenBlock, remote: &Remote) -> BlockDetail {
    let params = remote.slot_params(block.slot);
    BlockDetail {
        transactions: block.executed_transactions,
        non_vote_transactions: match remote.alpenglow {
            Some(true) => block.executed_transactions,
            _ => 0,
        },
        entries: block.entries,
        block_cost_limit: params.map_or(0, |params| params.block_limit),
        account_cost_limit: params.map_or(0, |params| params.account_limit),
        ..BlockDetail::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote::{EpochLeaders, THEM, US},
        solana_epoch_schedule::EpochSchedule,
        solana_metrics::datapoint::DataPoint,
        solana_rpc_client_api::response::SlotTransactionStats,
        solana_slot_history::SlotHistory as SlotHistorySysvar,
        std::time::UNIX_EPOCH,
    };

    const BASE: Slot = 1_000;
    const OTHER: Pubkey = Pubkey::new_from_array([3; 32]);

    /// Epoch 0 without warmup: `ours` are ours, the rest go to two leaders turn by turn.
    fn leaders(ours: &[Slot]) -> EpochLeaders {
        let slots = EpochSchedule::without_warmup().get_slots_in_epoch(0);
        EpochLeaders::for_tests(slots, |slot| {
            if ours.contains(&slot) {
                US
            } else if slot.checked_div(4).and_then(|turn| turn.checked_rem(2)) == Some(0) {
                THEM
            } else {
                OTHER
            }
        })
    }

    fn first_shred(slot: Slot, at_millis: u64) -> Frame {
        Frame::Slot {
            slot,
            parent: None,
            status: Status::FirstShredReceived { at_millis },
        }
    }

    fn status(slot: Slot, status: Status) -> Frame {
        Frame::Slot {
            slot,
            parent: slot.checked_sub(1),
            status,
        }
    }

    fn block(slot: Slot, parent_slot: Slot, executed: u64, entries: u64) -> Frame {
        Frame::Block(FrozenBlock {
            slot,
            parent_slot,
            blockhash: format!("hash{slot}"),
            executed_transactions: executed,
            entries,
        })
    }

    /// The slot's bank made, then frozen.
    fn frozen(slot: Slot, parent: Slot, executed: u64, entries: u64) -> [Frame; 2] {
        [
            status(slot, Status::CreatedBank),
            block(slot, parent, executed, entries),
        ]
    }

    struct Harness {
        feed: SlotFeed,
        frames: crossbeam_channel::Sender<Frame>,
        tap: Arc<MetricsTap>,
        remote: Shared,
        history: Arc<RwLock<SlotHistory>>,
        replies: Arc<Replies>,
    }

    /// Slot `BASE + 2` is ours; the rest of the epoch is not.
    fn harness() -> Harness {
        harness_with(&[BASE + 2])
    }

    fn harness_with(ours: &[Slot]) -> Harness {
        let (frames, receiver) = crossbeam_channel::unbounded();
        let remote: Shared = Arc::new(RwLock::new(Remote {
            epoch_schedule: Some(EpochSchedule::without_warmup()),
            identity: Some(US),
            schedules: [(0, Arc::new(leaders(ours)))].into(),
            finalized: Some(0),
            progress: Some(ValidatorStartProgress::Running),
            ..Remote::default()
        }));
        let tap = Arc::new(MetricsTap::default());
        let history = Arc::new(RwLock::new(SlotHistory::new(4096)));
        let replies = Arc::new(Replies {
            tap: tap.clone(),
            ..Replies::default()
        });
        let feed = SlotFeed::new(
            Arc::new(Publisher::new()),
            history.clone(),
            tap.clone(),
            remote.clone(),
            replies.clone(),
            receiver,
            Arc::new(AtomicU64::new(0)),
            Arc::default(),
            Arc::default(),
            None,
        );
        Harness {
            feed,
            frames,
            tap,
            remote,
            history,
            replies,
        }
    }

    impl Harness {
        /// With RPC's slot update for each block.
        fn send(&self, frames: impl IntoIterator<Item = Frame>) {
            for frame in frames {
                if let Frame::Block(FrozenBlock { slot, .. }) = frame {
                    let stats = SlotTransactionStats {
                        num_transaction_entries: 0,
                        num_successful_transactions: 0,
                        num_failed_transactions: 0,
                        max_transactions_per_entry: 0,
                    };
                    self.remote.write().unwrap().frozen.insert(slot, stats);
                }
                self.frames.send(frame).unwrap();
            }
        }

        /// Sends `frames`, then ticks.
        fn step(&mut self, frames: impl IntoIterator<Item = Frame>) {
            self.send(frames);
            self.feed.tick();
        }

        fn entry(&self, slot: Slot) -> SlotEntry {
            self.feed.followed.slots.get(slot).cloned().unwrap()
        }

        fn metrics(&self, name: &'static str, fields: &[(&'static str, String)]) {
            self.metrics_at(name, fields, SystemTime::now());
        }

        fn metrics_at(
            &self,
            name: &'static str,
            fields: &[(&'static str, String)],
            at: SystemTime,
        ) {
            let mut point = DataPoint::new(name);
            point.timestamp = at;
            for (field, value) in fields {
                point.fields.push((field, value.clone()));
            }
            self.tap.observe_external(&point);
        }

        /// `slot` filled at `full_at_millis`, `full_millis` after its first shred.
        fn fill(&self, slot: Slot, full_at_millis: u64, full_millis: u64) {
            self.metrics_at(
                "shred_insert_is_full",
                &[
                    ("slot", format!("{slot}i")),
                    ("total_time_ms", format!("{full_millis}i")),
                    ("last_index", "99i".to_string()),
                ],
                UNIX_EPOCH
                    .checked_add(Duration::from_millis(full_at_millis))
                    .unwrap(),
            );
        }

        fn costs(&self, slot: Slot, transactions: u64) {
            self.metrics(
                "cost_tracker_stats",
                &[
                    ("bank_slot", format!("{slot}i")),
                    ("block_cost", "123456i".to_string()),
                    ("transaction_count", format!("{transactions}i")),
                    ("total_transaction_fee", "150000i".to_string()),
                    ("total_priority_fee", "75000i".to_string()),
                ],
            );
        }
    }

    #[test]
    fn test_a_frozen_block_reads_as_its_bank_would() {
        let mut harness = harness();
        harness.send([
            status(BASE, Status::CreatedBank),
            first_shred(BASE + 1, 400),
        ]);
        // One slot skipped before it.
        harness.step(frozen(BASE + 1, BASE - 1, 30, 12));
        let entry = harness.entry(BASE + 1);
        assert_eq!(entry.level, SlotLevel::Completed);
        // Timed once its fill is reported, by the blockstore's stamp the fill gives back.
        assert_eq!(entry.time_millis, None);
        harness.fill(BASE + 1, 800, 390);
        harness.feed.tick();
        let entry = harness.entry(BASE + 1);
        assert_eq!(entry.time_millis, Some(410));
        assert_eq!(entry.shreds.map(|shreds| shreds.full_millis), Some(390));
        let block = entry.block.unwrap();
        assert_eq!((block.transactions, block.entries), (30, 12));
        // Its cost and fees wait for the metrics.
        assert_eq!((block.block_cost, block.total_fees), (0, 0));
        assert!(harness.feed.followed.awaited.contains_key(&(BASE + 1)));

        harness.costs(BASE + 1, 30);
        harness.feed.tick();
        let block = harness.entry(BASE + 1).block.unwrap();
        assert_eq!(
            (block.block_cost, block.total_fees, block.priority_fees),
            (123_456, 150_000, 75_000)
        );
        assert_eq!(
            harness
                .history
                .read()
                .unwrap()
                .get(BASE + 1)
                .map(|packed| packed.fees),
            Some(150_000)
        );
    }

    #[test]
    fn test_a_slot_whose_fill_never_comes_is_timed_by_the_ledgers_stamp() {
        let mut harness = harness();
        harness.step([
            status(BASE, Status::CreatedBank),
            first_shred(BASE + 1, 400),
            block(BASE + 1, BASE, 3, 1),
        ]);
        assert_eq!(harness.entry(BASE + 1).time_millis, None);
        let later = BASE + 1 + FILL_WAIT_SLOTS;
        harness.step(frozen(later, BASE + 1, 3, 1));
        assert_eq!(harness.entry(BASE + 1).time_millis, Some(400));
        // Its fill, once reported, times it again by the blockstore's stamp.
        harness.fill(BASE + 1, 800, 395);
        harness.feed.tick();
        let entry = harness.entry(BASE + 1);
        assert_eq!(entry.time_millis, Some(405));
        assert!(entry.shreds.is_some());
    }

    #[test]
    fn test_tips_are_the_tip_accounts_movement_since_the_base_read() {
        let meter = TipMeter::new(&Pubkey::new_unique());
        let derived = meter.accounts().to_vec();
        let mut harness = harness();
        harness.feed.tips = Some(meter);
        harness.remote.write().unwrap().tip_base = Some((BASE - 1, vec![100; 8]));
        let write = |slot, account: usize, lamports| Frame::Account {
            slot,
            pubkey: derived[account],
            lamports,
        };
        // The last write of a slot stands.
        harness.step([
            status(BASE, Status::CreatedBank),
            write(BASE, 0, 150),
            write(BASE, 0, 175),
            write(BASE, 3, 110),
            block(BASE, BASE - 1, 2, 0),
        ]);
        assert_eq!(harness.entry(BASE).block.unwrap().tips, Some(85));
        harness.step([
            status(BASE + 1, Status::CreatedBank),
            write(BASE + 1, 3, 120),
            block(BASE + 1, BASE, 1, 0),
        ]);
        assert_eq!(harness.entry(BASE + 1).block.unwrap().tips, Some(10));
    }

    #[test]
    fn test_a_tip_write_reported_after_its_block_is_counted_in_it() {
        let meter = TipMeter::new(&Pubkey::new_unique());
        let account = meter.accounts()[0];
        let mut harness = harness();
        harness.feed.tips = Some(meter);
        harness.remote.write().unwrap().tip_base = Some((BASE - 1, vec![100; 8]));
        let write = |slot, lamports| Frame::Account {
            slot,
            pubkey: account,
            lamports,
        };
        harness.step(frozen(BASE, BASE - 1, 1, 0));
        assert_eq!(harness.entry(BASE).block.unwrap().tips, Some(0));
        harness.send([write(BASE, 130)]);
        harness.step(frozen(BASE + 1, BASE, 1, 0));
        assert_eq!(harness.entry(BASE).block.unwrap().tips, Some(30));
        assert_eq!(harness.entry(BASE + 1).block.unwrap().tips, Some(0));
        // The blocks after it are measured again from its balance.
        harness.step([
            status(BASE + 2, Status::CreatedBank),
            write(BASE + 2, 175),
            block(BASE + 2, BASE + 1, 1, 0),
        ]);
        assert_eq!(harness.entry(BASE + 2).block.unwrap().tips, Some(45));
        harness.step([write(BASE + 1, 140)]);
        assert_eq!(harness.entry(BASE + 1).block.unwrap().tips, Some(10));
        assert_eq!(harness.entry(BASE + 2).block.unwrap().tips, Some(35));
    }

    #[test]
    fn test_an_empty_block_needs_no_cost_report() {
        let mut harness = harness();
        harness.step(frozen(BASE, BASE - 1, 0, 0));
        // Still replayed, so its replay time is awaited, but not a cost report.
        let awaited = harness.feed.followed.awaited.get(&BASE).copied().unwrap();
        assert!(!awaited.figures && awaited.replay);
        assert_eq!(harness.entry(BASE).block.unwrap().entries, 0);
    }

    #[test]
    fn test_a_skip_below_the_root_waits_for_the_sysvar() {
        let mut harness = harness();
        harness.step([status(BASE, Status::CreatedBank)]);
        harness.step(frozen(BASE + 3, BASE, 5, 1));
        assert!(harness.entry(BASE + 2).mine);
        assert!(!harness.entry(BASE + 3).mine);
        assert_eq!(harness.entry(BASE + 2).level, SlotLevel::Incomplete);

        harness.step([
            status(BASE + 3, Status::Confirmed),
            status(BASE + 3, Status::Rooted),
        ]);
        assert_eq!(harness.entry(BASE + 3).level, SlotLevel::Rooted);
        // Neither frozen here nor rooted: skipped once the sysvar agrees, and not before.
        assert_eq!(harness.entry(BASE + 1).level, SlotLevel::Incomplete);
        assert_eq!(harness.entry(BASE + 2).level, SlotLevel::Incomplete);
        assert_eq!(
            harness.remote.read().unwrap().slot_history_wanted,
            Some(BASE + 2)
        );
        let mut sysvar = SlotHistorySysvar::default();
        sysvar.add(BASE);
        sysvar.add(BASE + 3);
        harness.remote.write().unwrap().slot_history = Some(Arc::new(sysvar));
        harness.feed.tick();
        assert_eq!(harness.entry(BASE + 1).level, SlotLevel::Skipped);
        assert_eq!(harness.entry(BASE + 2).level, SlotLevel::Skipped);
        assert_eq!(harness.remote.read().unwrap().slot_history_wanted, None);
        assert_eq!(harness.feed.keys.root_slot.last(), Some(&(BASE + 3)));
        assert_eq!(harness.feed.keys.estimated_slot.last(), Some(&(BASE + 3)));
        assert_eq!(harness.feed.keys.completed_slot.last(), Some(&(BASE + 3)));
    }

    #[test]
    fn test_a_rooted_slot_whose_frames_went_missing_is_not_a_skip() {
        let mut harness = harness();
        harness.step([status(BASE, Status::CreatedBank)]);
        // The rooted chain passes BASE + 1, whose bank's frames never came.
        harness.send([first_shred(BASE + 1, 400)]);
        harness.send(frozen(BASE + 4, BASE + 1, 5, 1));
        harness.step([
            status(BASE + 1, Status::Rooted),
            status(BASE + 4, Status::Rooted),
        ]);
        harness.feed.tick();
        assert_eq!(harness.entry(BASE + 1).level, SlotLevel::Rooted);
        assert!(harness.feed.followed.unresolved.contains(&(BASE + 2)));
        assert!(!harness.feed.followed.unresolved.contains(&(BASE + 1)));
    }

    #[test]
    fn test_slots_wait_for_the_schedule_before_being_marked() {
        let mut harness = harness();
        harness.remote.write().unwrap().schedules.clear();
        harness.step([status(BASE, Status::CreatedBank)]);
        harness.step([status(BASE + 2, Status::CreatedBank)]);
        assert!(harness.feed.followed.slots.get(BASE + 1).is_none());
        assert_eq!(harness.feed.followed.leaders_resolved_to, BASE);

        harness
            .remote
            .write()
            .unwrap()
            .schedules
            .insert(0, Arc::new(leaders(&[BASE + 2])));
        harness.feed.tick();
        assert!(harness.entry(BASE + 2).mine);
        assert_eq!(harness.feed.followed.leaders_resolved_to, BASE + 3);
    }

    #[test]
    fn test_another_validator_starts_the_strip_again() {
        let mut harness = harness();
        let hello = |pid| Frame::Validator { pid };
        harness.send([hello(7)]);
        harness.step(frozen(BASE, BASE - 1, 1, 1));
        assert!(harness.feed.followed.slots.get(BASE).is_some());
        harness.step([hello(7), status(BASE + 1, Status::CreatedBank)]);
        assert!(harness.feed.followed.slots.get(BASE).is_some());
        harness.step([hello(8)]);
        assert!(harness.feed.followed.slots.get(BASE).is_none());
        assert_eq!(harness.feed.followed.highest, 0);
    }

    #[test]
    fn test_a_block_of_ours_is_captured_once_its_figures_and_first_shred_are_in() {
        let mut harness = harness();
        harness.send([status(BASE, Status::CreatedBank)]);
        harness.step(frozen(BASE + 2, BASE, 30, 0));
        harness.feed.tick();
        assert!(harness.entry(BASE + 2).mine);
        assert!(!harness.replies.produced.read().unwrap().contains(BASE + 2));

        harness.costs(BASE + 2, 30);
        harness.fill(BASE + 2, 1_000, 380);
        harness.feed.tick();
        let produced = harness.replies.produced.read().unwrap();
        let blocks = produced.recent_refs(10);
        assert_eq!(blocks.len(), 1);
        let block = blocks[0];
        assert_eq!(block.slot, BASE + 2);
        assert_eq!(block.blockhash, format!("hash{}", BASE + 2));
        assert_eq!(block.slot_time_millis, Some(620));
        assert_eq!(
            (
                block.detail.transactions,
                block.detail.total_fees,
                block.detail.priority_fees
            ),
            (30, 150_000, 75_000)
        );
    }

    #[test]
    fn test_a_block_frozen_before_the_schedule_is_known_waits_for_its_leader() {
        let mut harness = harness();
        let schedules = std::mem::take(&mut harness.remote.write().unwrap().schedules);
        harness.costs(BASE + 2, 30);
        harness.fill(BASE + 2, 1_000, 380);
        harness.send([status(BASE, Status::CreatedBank)]);
        harness.step(frozen(BASE + 2, BASE, 30, 0));
        // Not taken for another leader's block in the meantime.
        assert!(
            harness
                .feed
                .followed
                .slots
                .get(BASE + 2)
                .is_none_or(|entry| entry.block.is_none())
        );

        harness.remote.write().unwrap().schedules = schedules;
        harness.feed.tick();
        harness.feed.tick();
        assert!(harness.entry(BASE + 2).mine);
        assert!(harness.replies.produced.read().unwrap().contains(BASE + 2));
    }

    #[test]
    fn test_a_block_of_ours_that_did_not_root_is_dropped() {
        let mut harness = harness();
        harness.send([status(BASE, Status::CreatedBank)]);
        harness.send(frozen(BASE + 2, BASE, 0, 0));
        harness.fill(BASE + 2, 1_000, 380);
        harness.feed.tick();
        assert!(harness.replies.produced.read().unwrap().contains(BASE + 2));
        // The rooted chain passes it by.
        harness.send(frozen(BASE + 3, BASE, 1, 1));
        harness.step([status(BASE + 3, Status::Rooted)]);
        // Not named by the chain: the sysvar decides.
        assert!(harness.replies.produced.read().unwrap().contains(BASE + 2));
        let mut sysvar = SlotHistorySysvar::default();
        sysvar.add(BASE);
        sysvar.add(BASE + 3);
        harness.remote.write().unwrap().slot_history = Some(Arc::new(sysvar));
        harness.feed.tick();
        assert!(!harness.replies.produced.read().unwrap().contains(BASE + 2));
    }

    #[test]
    fn test_the_gap_before_a_new_leaders_first_shred_is_a_handoff() {
        let mut harness = harness();
        // The clock times slots after the first it sees.
        harness.step(frozen(BASE + 2, BASE + 1, 2, 1));
        // BASE + 3 is THEM's last slot, BASE + 4 OTHER's first.
        harness.send(frozen(BASE + 3, BASE + 2, 2, 1));
        harness.send(frozen(BASE + 4, BASE + 3, 2, 1));
        harness.fill(BASE + 3, 1_000, 390);
        harness.fill(BASE + 4, 1_400, 370);
        harness.feed.tick();
        harness.feed.tick();
        assert_eq!(harness.entry(BASE + 3).full_at(), Some(1_000));
        assert_eq!(harness.entry(BASE + 4).time_millis, Some(1_030));
        assert!(harness.feed.followed.handoffs.holds(BASE + 4));
        // Within one leader's run there is no handoff.
        assert!(!harness.feed.followed.handoffs.holds(BASE + 3));
    }

    #[test]
    fn test_a_handoff_timed_late_is_judged_against_the_tip_it_arrived_to() {
        let mut harness = harness();
        harness.step(frozen(BASE + 2, BASE + 1, 2, 1));
        harness.send([first_shred(BASE + 3, 610)]);
        harness.send(frozen(BASE + 3, BASE + 2, 2, 1));
        harness.send([first_shred(BASE + 4, 1_030)]);
        harness.step(frozen(BASE + 4, BASE + 3, 2, 1));
        // Replay keeps up, while the fill reports come once the cluster is past both slots by more
        // than a handoff may trail it.
        for slot in BASE + 5..=BASE + 50 {
            harness.send(frozen(slot, slot - 1, 1, 1));
        }
        harness.feed.tick();
        assert!(!harness.feed.followed.trailed_cluster);
        harness.fill(BASE + 3, 1_000, 390);
        harness.fill(BASE + 4, 1_400, 370);
        harness.feed.tick();
        assert_eq!(harness.entry(BASE + 4).time_millis, Some(1_030));
        assert!(harness.feed.followed.handoffs.holds(BASE + 4));
    }

    #[test]
    fn test_before_its_first_block_the_root_stands_for_the_completed_slot() {
        let mut harness = harness();
        harness.remote.write().unwrap().finalized = Some(BASE);
        harness.step([first_shred(BASE + 1, 1_000)]);
        assert_eq!(harness.feed.keys.completed_slot.last(), Some(&BASE));
        assert_eq!(harness.feed.keys.behind_cluster.last(), Some(&Some(1)));
        assert_eq!(harness.feed.followed.completed_window.len(), 1);
        harness.step(frozen(BASE + 1, BASE, 1, 1));
        assert_eq!(harness.feed.keys.completed_slot.last(), Some(&(BASE + 1)));
    }

    #[test]
    fn test_the_working_reading_is_the_newest_frozen_block_with_its_chain() {
        let mut harness = harness_with(&[]);
        harness.remote.write().unwrap().alpenglow = Some(true);
        harness.metrics(
            "bank-new_from_parent-heights",
            &[
                ("slot", format!("{BASE}i")),
                ("block_height", "900i".to_string()),
            ],
        );
        harness.step([
            block(BASE + 1, BASE, 30, 4),
            // One slot skipped before it.
            block(BASE + 3, BASE + 1, 20, 4),
            // Another fork's, frozen after the newest.
            block(BASE + 2, BASE, 70, 4),
        ]);
        let working = harness.remote.read().unwrap().working.unwrap();
        assert_eq!(
            (
                working.slot,
                working.transaction_count,
                working.non_vote_transaction_count
            ),
            (BASE + 3, 50, 50)
        );
        assert_eq!((working.epoch, working.epoch_start_slot), (0, 0));
        // The bank reported, and one for each block after it on the chain.
        assert_eq!(harness.feed.keys.block_height.last(), Some(&902));
    }

    #[test]
    fn test_the_replay_window_opens_once_the_validator_runs() {
        let mut harness = harness();
        harness.remote.write().unwrap().progress =
            Some(ValidatorStartProgress::WaitingForSupermajority {
                slot: BASE,
                gossip_stake_percent: 0,
            });
        harness.step(frozen(BASE + 1, BASE, 1, 1));
        assert!(harness.feed.followed.completed_window.is_empty());
        harness.remote.write().unwrap().progress = Some(ValidatorStartProgress::Running);
        harness.feed.tick();
        assert_eq!(harness.feed.followed.completed_window.len(), 1);
        // Once open, an admin socket that stops answering leaves it so.
        harness.remote.write().unwrap().progress = None;
        harness.feed.tick();
        assert_eq!(harness.feed.followed.completed_window.len(), 2);
    }

    #[test]
    fn test_no_distance_is_published_before_a_block_completes() {
        let mut harness = harness();
        harness.step([
            first_shred(BASE + 50, 1_000),
            status(BASE + 50, Status::CreatedBank),
        ]);
        assert_eq!(harness.feed.keys.behind_cluster.last(), Some(&None));
        assert!(!harness.feed.followed.trailed_cluster);
        // Nor does the replay rate's window start at nought.
        assert!(harness.feed.followed.completed_window.is_empty());
        // Nor any slot position still at nought.
        assert_eq!(harness.feed.keys.root_slot.last(), None);
        assert_eq!(harness.feed.keys.completed_slot.last(), None);
        assert_eq!(harness.feed.keys.estimated_slot.last(), Some(&(BASE + 50)));
    }

    #[test]
    fn test_a_turn_is_credited_once_the_metrics_pass_its_end() {
        let ours: Vec<Slot> = (BASE + 4..=BASE + 7).collect();
        let mut harness = harness_with(&ours);
        harness.step([status(BASE, Status::CreatedBank)]);
        for slot in BASE + 1..=BASE + 8 {
            harness.send(frozen(slot, slot - 1, 0, 0));
        }
        harness.feed.tick();
        assert_eq!(harness.feed.followed.turns_ending.len(), 1);
        assert!(
            harness
                .replies
                .produced
                .read()
                .unwrap()
                .recent_turns(8)
                .is_empty()
        );
        harness.metrics_at(
            "shred_fetch_receiver",
            &[("packets_count", "3i".to_string())],
            SystemTime::now()
                .checked_add(Duration::from_secs(1))
                .unwrap(),
        );
        harness.feed.tick();
        let turns = harness.replies.produced.read().unwrap().recent_turns(8);
        assert_eq!(turns.len(), 1);
        assert_eq!((turns[0].first, turns[0].last), (BASE + 4, BASE + 7));
    }

    #[test]
    fn test_a_node_behind_the_tip_is_caught_up_once_level_with_it() {
        let mut harness = harness();
        harness.send(frozen(BASE, BASE - 1, 1, 1));
        harness.step([first_shred(BASE + 12, 5_000)]);
        assert_eq!(harness.feed.keys.behind_cluster.last(), Some(&Some(12)));
        assert!(harness.feed.followed.trailed_cluster);
        for slot in BASE + 1..=BASE + 12 {
            harness.send(frozen(slot, slot - 1, 1, 1));
        }
        harness.feed.tick();
        assert_eq!(harness.feed.keys.behind_cluster.last(), Some(&Some(0)));
        assert!(harness.feed.followed.caught_cluster);
    }

    #[test]
    fn test_the_next_leader_slot_is_our_first_past_the_highest_bank() {
        let mut harness = harness_with(&[BASE + 2, BASE + 40]);
        harness.step([status(BASE, Status::CreatedBank)]);
        assert_eq!(
            harness.feed.keys.next_leader_slot.last(),
            Some(&Some(BASE + 2))
        );
        harness.step([status(BASE + 3, Status::CreatedBank)]);
        assert_eq!(
            harness.feed.keys.next_leader_slot.last(),
            Some(&Some(BASE + 40))
        );
    }
}
