//! Counters lifted from the metrics points the validator exports about itself, read in batches
//! from its metrics writer: a name match, then one short lock for the set a point adds into, the
//! per-slot points included.
//! Points carry deltas, accumulated into totals. A few fields are levels,
//! replaced by the latest reading and never summed.

use {
    crate::{certs::VoteSent, collect::system_time_nanos},
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_metrics::datapoint::DataPoint,
    solana_pubkey::Pubkey,
    solana_time_utils::timestamp,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
        str::FromStr,
        sync::{
            Mutex, MutexGuard, PoisonError,
            atomic::{AtomicU64, Ordering},
        },
        time::SystemTime,
    },
};

const ACCOUNTS_DB_TIMINGS: &str = "accounts_db_store_timings";

/// Each part of the validator that reports its own size, for the host card's memory split.
const ACCOUNTS_INDEX: &str = "accounts_index";
const ACCOUNTS_WRITE_CACHE: &str = "accounts_cache_size";
/// One point per column family every ten seconds, tagged with its name.
const BLOCKSTORE_CFS: &str = "blockstore_rocksdb_cfs";
const CF_NAME: &str = "cf_name";

const SHREDS_TURBINE: &str = "shred_fetch_receiver";
const SHREDS_REPAIR: &str = "shred_fetch_repair_receiver";

/// The receivers on the other two UDP ports the socket panel lists; the QUIC ports count
/// transactions, and serve repair's receiver never reports.
const GOSSIP_RECEIVER: &str = "gossip_receiver";

/// Turbine goes out over XDP and reports shreds only.
const GOSSIP_SENDER: &str = "Gossip";
const REPAIR_SENDER: &str = "Repair";
const SENT_BYTES: &str = "streamer-send-bytes_total";
const SENT_MILLIS: &str = "streamer-send-sample_duration_ms";
const TPU_VOTE_RECEIVER: &str = "tpu_vote_receiver";

const PACKETS_COUNT: &str = "packets_count";

/// Reported once a second and reset; submitted directly, so it arrives at any log level.
/// Same worker, tick and `id` as the counts point, so both read into one set of counters.
const WORKER_ERROR_METRICS: &str = "banking_stage_worker_error_metrics";

const ACCOUNTS_LOADS: &str = "accounts_db_load_accounts";
const ACCOUNTS_STORES: &str = "accounts_db-stores";
const ACCOUNTS_FLUSH: &str = "accounts_db-flush_accounts_cache";

const ACCOUNTS_CLEAN: &str = "clean_accounts";
/// Shrink rewrites one slot's storage without its dead accounts; the ancient one packs old
/// slots' storages together. Both report the same fields.
const ACCOUNTS_SHRINK: &str = "shrink_stats";
const ACCOUNTS_SHRINK_ANCIENT: &str = "shrink_ancient_stats";
/// One point each time a background job starts or ends: its one field names the job and holds
/// how many of it now run, so the time between the two is how long the job took.
const ACCOUNTS_ACTIVE: &str = "accounts_db_active";

/// The program cache's counters, reported and reset once per bank, so each point is one slot's
/// work.
const PROGRAM_CACHE: &str = "loaded-programs-cache-stats";

const QUIC_TPU: &str = "quic_streamer_tpu";
const QUIC_TPU_FORWARDS: &str = "quic_streamer_tpu_forwards";
const QUIC_TPU_VOTE: &str = "quic_streamer_tpu_vote";

/// `tpu-vote-verifier` is left alone: votes never reach the scheduler.
const TPU_VERIFIER: &str = "tpu-verifier";

const BUNDLE_STAGE: &str = "bundle_stage-loop_stats";

const BUNDLE_SLOT_STATS: &str = "bundle_stage-stats";

/// Every twenty milliseconds while it has work, with no slot on it.
const WORKER_TIMING: &str = "banking_stage_worker_timing";

const VOTE_SLOT_TIMING: &str = "banking_stage-leader_slot_vote_execute_and_commit_timings";

const WORKER_ID: &str = "id";

/// The worker threads, one point each, summed. Exported at any log level: agave submits it directly
/// with `solana_metrics::submit(.., Level::Trace)`.
const WORKER_COUNTS: &str = "banking_stage_worker_counts";

const SCHEDULER_SLOT_COUNTS: &str = "banking_stage_scheduler_slot_counts";

/// Submitted only where an XDP config was given, so its absence says XDP is off.
const XDP_NETWORK_CONFIG: &str = "xdp-network-config";

const RETRANSMIT_STAGE: &str = "retransmit-stage";

const RETRANSMIT_SLOT_STATS: &str = "retransmit-stage-slot-stats";

/// Sent with `datapoint_info!`, so absent below `solana=info`.
const REPLAY_SLOT_STATS: &str = "replay-slot-stats";

const SHRED_FULL: &str = "shred_insert_is_full";

/// Sent for every bank made, with its block height.
const NEW_BANK: &str = "bank-new_from_parent-heights";

/// Reported for every slot, tagged with whether it was ours; only ours are kept.
const COST_TRACKER: &str = "cost_tracker_stats";

const WFSM_GOSSIP: &str = "wfsm_gossip";

/// Gossip reports itself every two seconds across these points, clearing each count as it sends it.
const GOSSIP_STATS: [&str; 5] = [
    "cluster_info_stats",
    "cluster_info_stats2",
    "cluster_info_stats3",
    "cluster_info_stats4",
    "cluster_info_stats5",
];
const GOSSIP_ENTRIES: &str = "cluster_info_crds_stats";
const GOSSIP_ENTRY_FAILS: &str = "cluster_info_crds_stats_fails";

/// In the order gossip reports them.
pub const GOSSIP_ENTRY_TYPES: [&str; 14] = [
    "LegacyContactInfo",
    "Vote",
    "LowestSlot",
    "LegacySnapshotHashes",
    "AccountsHashes",
    "EpochSlots",
    "LegacyVersion",
    "Version",
    "NodeInstance",
    "DuplicateShred",
    "SnapshotHashes",
    "ContactInfo",
    "RestartLastVotedForkSlots",
    "RestartHeaviestFork",
];

const IS_LEADER: &str = "is_leader";

const IS_XDP: &str = "is_xdp";

const SLOT: &str = "slot";

/// About a minute and a half: shorter missed the program cache's mean by a third, as compilation
/// arrives in bursts.
const REPLAY_SLOTS: usize = 256;

const VOTE_TRACKING: &str = "event_handler_slot_tracking";

/// Votor's per-epoch timing of each validator's votes, one point per vote account.
const VOTE_ARRIVAL: &str = "consensus_vote_metrics";

const VOTE_ACCOUNT: &str = "address";

/// Under alpenglow, the block creation loop's report on one of our leader slots.
const LEADER_START: &str = "slot-metrics";

const LEADER_BANK: &str = "leader-slot-start-to-cleared-elapsed-ms";

const BROADCAST_TRANSMIT: &str = "broadcast-transmit-shreds-stats";

const BROADCAST_PROCESS: &str = "broadcast-process-shreds-stats";

const SCHEDULER_SLOT_TIMING: &str = "banking_stage_scheduler_slot_timing";

const VOTE_TRACKS: usize = 4096;

/// A leader window's first slot at a time, so several minutes of them.
const FINALITY_SAMPLES: usize = 1024;

/// Read when replay freezes a slot; during a catch-up the blockstore fills far ahead of replay.
const SHRED_FILLS: usize = 4096;

/// Banks' heights held: the newest frozen block's chain meets one well inside a metrics batch.
const BLOCK_HEIGHTS: usize = 1024;

/// Reports held until the slot feed copies them into one of our blocks.
const PENDING_SLOTS: usize = 500;
/// As many banks as the slot ring holds.
const BANK_FIGURES: usize = 4096;

/// Well past 1.2 epochs of the largest stake's slots; a guard for a floor never set.
const MAX_HELD_SLOTS: usize = 32_768;

const WORKER_TIMINGS: usize = 2048;

const SCHEDULER_ID: &str = "id";

const OWN_SCHEDULER_ID: &str = "0";

/// Values stamped with when they arrived, for a median over a recent window.
#[derive(Debug, Default)]
struct TimedSamples(Mutex<VecDeque<(u64, u64)>>);

impl TimedSamples {
    fn push(&self, at_millis: u64, value: u64) {
        push_capped(&mut locked(&self.0), (at_millis, value), FINALITY_SAMPLES);
    }

    fn median(&self, now_millis: u64, window_millis: u64) -> Option<u64> {
        let samples = locked(&self.0);
        let mut recent: Vec<u64> = samples
            .iter()
            .filter(|(at, _)| now_millis.saturating_sub(*at) <= window_millis)
            .map(|(_, value)| *value)
            .collect();
        drop(samples);
        recent.sort_unstable();
        recent.get(recent.len().checked_div(2)?).copied()
    }
}

#[derive(Debug, Default)]
pub struct MetricsTap {
    counters: Mutex<TapCounters>,
    memory: Mutex<MemorySet>,

    slot_waterfalls: Mutex<HeldSlots<SlotWaterfall>>,

    slot_costs: Mutex<HeldSlots<SlotCost>>,
    bank_figures: Mutex<BTreeMap<Slot, BankFigures>>,

    slot_lists_revision: AtomicU64,

    /// Kept one by one because the panel wants the worst slot as well as the mean.
    replay_slots: Mutex<BTreeMap<Slot, ReplaySlotTimes>>,
    vote_tracks: Mutex<BTreeMap<Slot, VoteSent>>,
    /// Each finalized slot votor tracked, and whether its fast certificate finalized it.
    finalized_paths: Mutex<VecDeque<(Slot, bool)>>,

    /// Microseconds from a slot's last shred to our notarize vote, stamped when votor reported it.
    vote_lag: TimedSamples,

    /// Microseconds from the leader closing a block, by its clock, to its finalization here.
    from_leader: TimedSamples,

    /// Held until the slot feed takes them.
    block_votes: Mutex<VecDeque<BlockVotes>>,

    closed_blocks: Mutex<ClosedBlocks>,

    shred_fills: Mutex<BTreeMap<Slot, ShredFill>>,

    block_heights: Mutex<BTreeMap<Slot, u64>>,

    bundle_slots: Mutex<BTreeMap<Slot, Bundles>>,

    worker_timings: Mutex<VecDeque<WorkerTiming>>,

    vote_timings: Mutex<BTreeMap<Slot, StageTimes>>,

    vote_arrivals: Mutex<VoteArrivals>,

    leader_timings: Mutex<BTreeMap<Slot, LeaderTiming>>,

    /// Latched: it cannot change while the process runs.
    xdp: Mutex<Option<XdpConfig>>,

    stake_in_gossip: Mutex<Option<StakeInGossip>>,

    /// For points read after the fact: the counters as each step of point time began.
    external: Mutex<ExternalSteps>,
}

/// The counters read for a moment of point time stand at most this long after it.
pub const COUNTER_STEP_MILLIS: u64 = 200;
/// A minute of point time.
const COUNTER_STEPS: usize = 300;
/// The point time [`COUNTER_STEPS`] cover. Points come in out of order by less than this.
const COUNTER_STEPS_MILLIS: u64 = COUNTER_STEP_MILLIS.saturating_mul(COUNTER_STEPS as u64);

/// Points read from the validator's metrics export reach the tap in batches, long after they were
/// made, so the counters a reader wants for a moment are kept as each step of point time began.
#[derive(Debug, Default)]
struct ExternalSteps {
    /// The newest point time read, in unix milliseconds.
    through_millis: u64,
    /// By the step's first millisecond: the counters before the first point made in it.
    steps: VecDeque<(u64, TapCounters)>,
}

/// Where one of our leader slots' time went, merged from the reports that each carry part of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LeaderTiming {
    /// Under alpenglow, from trying to start the slot to its bank existing, any wait for the
    /// parent's replay included.
    pub start_micros: Option<u64>,
    /// The part of the start spent waiting for the parent's replay; reported from 4.4 on.
    pub parent_wait_micros: Option<u64>,
    /// The bank open to transactions, from set to cleared.
    pub open_millis: Option<u64>,
    /// From broadcast's first entries for the slot to its last shred sent.
    pub broadcast_micros: Option<u64>,
    pub shredding_micros: Option<u64>,
    pub scheduler_receive_micros: Option<u64>,
    pub scheduler_schedule_micros: Option<u64>,
    /// The window began on a parent not yet certified.
    pub fast_handover: bool,
    /// The block had to move to another parent.
    pub parent_switched: bool,
}

/// One validator's votes over an epoch, each timed from its slot starting here to the vote arriving.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VoteArrival {
    pub notarize_mean_micros: u64,
    pub notarize_spread_micros: Option<u64>,
    pub notarize_worst_micros: Option<u64>,
    /// The notarize votes the mean is over.
    pub notarize_count: Option<u64>,
    pub finalize_mean_micros: Option<u64>,
}

/// The newest epoch's arrivals; votor reports each epoch once, after it has finished.
#[derive(Debug, Default)]
struct VoteArrivals {
    epoch: Option<Epoch>,
    by_vote: HashMap<Pubkey, VoteArrival>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StakeInGossip {
    pub online: u64,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct XdpConfig {
    /// Whether the socket bound with `XDP_ZEROCOPY`. The flag is passed straight to
    /// `bind`, which fails rather than falling back, so true means zero-copy.
    pub zero_copy: bool,
    pub driver: String,
    pub vendor: String,
    pub model: String,
    pub kernel_version: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SlotWaterfall {
    pub slot: Slot,
    pub source: SchedulerSource,
    #[serde(flatten)]
    pub counts: SchedulerTotals,
}

/// What the cost update service reports of every frozen bank holding transactions, leader or not:
/// the block figures a collector outside the validator cannot read off the bank.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BankFigures {
    pub block_cost: u64,
    pub transactions: u64,
    pub total_fees: u64,
    pub priority_fees: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SlotCost {
    pub slot: Slot,
    pub costliest_account: String,
    pub costliest_cost: u64,
    pub block_cost: u64,
    pub accounts: u64,
    /// Accounts more than one transaction wanted to write. The cost tracker's
    /// own definition: within five percent of the per-account ceiling.
    pub contended: u64,
    pub new_account_data: u64,
    pub in_flight: u64,
}

/// How often an account was a block's costliest, over the blocks held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Recurrence {
    pub blocks: u64,
    pub of: u64,
    pub peak_cost: u64,
    pub peak_slot: Slot,
}

/// Our leader slots' reports from the produced store's floor on, and which changed since taken.
#[derive(Debug, Default)]
struct HeldSlots<T> {
    held: BTreeMap<Slot, T>,
    changed: BTreeSet<Slot>,
    floor: Slot,
}

impl<T: Clone> HeldSlots<T> {
    /// Kept unless below the floor, or held already and `replaces` says no.
    fn put(&mut self, slot: Slot, value: T, replaces: impl FnOnce(&T) -> bool) -> bool {
        if slot < self.floor {
            return false;
        }
        match self.held.get_mut(&slot) {
            Some(held) if replaces(held) => *held = value,
            Some(_) => return false,
            None => insert_capped(&mut self.held, slot, value, MAX_HELD_SLOTS),
        }
        self.changed.insert(slot);
        true
    }

    fn recent(&self, count: usize) -> Vec<T> {
        let mut recent: Vec<T> = self.held.values().rev().take(count).cloned().collect();
        recent.reverse();
        recent
    }

    fn take_changed(&mut self) -> Vec<T> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .filter_map(|slot| self.held.get(&slot).cloned())
            .collect()
    }

    fn hold_from(&mut self, floor: Slot) {
        self.floor = floor;
        self.held = self.held.split_off(&floor);
        self.changed = self.changed.split_off(&floor);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShredFill {
    pub shreds: u64,
    pub repaired: u64,
    /// Rebuilt here from erasure coding rather than received.
    pub recovered: u64,
    pub full_millis: u64,
    /// Unix nanoseconds when the block became full here, from the point's own clock.
    pub full_at_nanos: u64,
}

impl ShredFill {
    /// The blockstore's stamp of the slot's first shred, which `full_millis` is counted from: the
    /// fill is timed a few microseconds before its point, so this is the stamp or a millisecond
    /// after it.
    pub fn first_shred_millis(&self) -> Option<u64> {
        (self.full_at_nanos > 0)
            .then(|| (self.full_at_nanos / 1_000_000).saturating_sub(self.full_millis))
    }
}

/// Microseconds from a block's last shred to our notarize vote and to its finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockVotes {
    pub slot: Slot,
    pub vote_us: Option<u64>,
    pub finalized_us: Option<u64>,
}

/// When each block's leader closed it, and the finality waiting for that, kept until both meet.
#[derive(Debug, Default)]
struct ClosedBlocks {
    closed_at: BTreeMap<Slot, u64>,
    waiting: BTreeMap<Slot, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerTiming {
    at_millis: u64,
    worker: u64,
    times: StageTimes,
    longest_batch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkerSum {
    pub workers: u64,
    pub times: StageTimes,
    pub longest_batch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerSource {
    #[default]
    Scheduler,
    /// BAM on jito; it receives batches rather than packets.
    Bam,
}

fn scheduler_source(point: &DataPoint) -> SchedulerSource {
    match tag_named(point, SCHEDULER_ID) {
        None | Some(OWN_SCHEDULER_ID) => SchedulerSource::Scheduler,
        Some(_) => SchedulerSource::Bam,
    }
}

/// `received` is left out: the two schedulers count it in different units.
fn describes_more_work(new: &SchedulerTotals, held: &SchedulerTotals) -> bool {
    (new.scheduled, new.finished, new.buffered) > (held.scheduled, held.finished, held.buffered)
}

pub trait WindowedCounters: Copy + Default {
    fn since(&self, previous: &Self) -> Self;
    fn plus(&self, other: &Self) -> Self;
}

/// Declares a set's totals and their window arithmetic from one field list, so no field can be
/// left out of the arithmetic and read nought for ever. A field given names adds in the point
/// fields of those names.
macro_rules! counter_totals {
    (
        $(#[$meta:meta])*
        pub struct $totals:ident {
            $($(#[$field_meta:meta])* pub $field:ident: u64 $(= $($name:literal)|+)?,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
        pub struct $totals {
            $($(#[$field_meta])* pub $field: u64,)*
        }

        impl WindowedCounters for $totals {
            /// Saturating: a lower reading means a mid-flight install or a reset counter.
            fn since(&self, previous: &Self) -> Self {
                Self {
                    $($field: self.$field.saturating_sub(previous.$field),)*
                }
            }

            fn plus(&self, other: &Self) -> Self {
                Self {
                    $($field: self.$field.saturating_add(other.$field),)*
                }
            }
        }

        impl $totals {
            /// The counter the point field `name` adds into.
            fn counter(&mut self, name: &str) -> Option<&mut u64> {
                match name {
                    $($($($name)|+ => Some(&mut self.$field),)?)*
                    _ => None,
                }
            }
        }

        impl AddPoint for $totals {
            fn add_point(&mut self, point: &DataPoint) {
                for (name, value) in &point.fields {
                    if let Some(counter) = self.counter(name) {
                        add_value(counter, value);
                    }
                }
            }
        }
    };
}

counter_totals! {
    pub struct StageTimes {
        pub cost_model: u64 = "cost_model_us",
        pub load_execute: u64 = "load_execute_us",
        pub freeze_lock: u64 = "freeze_lock_us",
        pub record: u64 = "record_us",
        pub commit: u64 = "commit_us",
        pub send_votes: u64 = "find_and_send_votes_us",
    }
}

counter_totals! {
    /// Bundles sanitized and landed in a block of ours.
    pub struct Bundles {
        pub sanitized: u64 = "num_sanitized_ok",
        pub executed: u64 = "execution_results_ok",
    }
}

counter_totals! {
    /// One replayed slot's timings, in microseconds: `fetch`, `confirming` and `completing` are
    /// disjoint on replay's thread, the verify fields overlap, and the rest is worker thread time.
    pub struct ReplaySlotTimes {
        pub slot: u64,
        // Replay's own thread, sequential.
        pub fetch: u64 = "fetch_entries_time",
        /// The second name, as on `dispatch`, is sent when a block's footer already collected the
        /// scheduler's results, as every alpenglow block does; the values are the same.
        pub confirming: u64 = "confirmation_without_replay_us" | "confirmation_time_us",
        pub completing: u64 = "bank_complete_time_us",

        // Verification jobs, concurrent. Relative only.
        pub poh_verify: u64 = "entry_poh_verification_time",
        pub tx_verify: u64 = "entry_transaction_verification_time",
        pub dispatch: u64 = "task_submission_us" | "replay_time",

        // Thread time across the workers.
        pub execute: u64 = "execute_us",
        pub bytecode: u64 = "execute_details_execute_inner_us",
        pub serialising: u64 = "execute_details_serialize_us",
        pub deserialising: u64 = "execute_details_deserialize_us",
        pub load: u64 = "load_us",
        pub store: u64 = "store_us",
        pub program_cache: u64 = "program_cache_us",
        /// Compiling a program that was not in the cache, summed: what a miss cost, not which
        /// third of the compiler it went to.
        pub compiling: u64 = "execute_details_create_executor_load_elf_us"
            | "execute_details_create_executor_verify_code_us"
            | "execute_details_create_executor_jit_compile_us",
        pub checking: u64 = "validate_transactions_us"
            | "validate_fees_us"
            | "filter_executable_us",
        pub other: u64 = "collect_balances_us"
            | "collect_logs_us"
            | "update_stakes_cache_us"
            | "update_transaction_statuses"
            | "check_block_limits_us",

        pub transactions: u64 = "total_transactions",
        /// Every entry replay ran through, ticks included.
        pub entries: u64 = "total_entries",

        pub observed_millis: u64,
    }
}

impl ReplaySlotTimes {
    pub fn serial(&self) -> u64 {
        self.fetch
            .saturating_add(self.confirming)
            .saturating_add(self.completing)
    }
}

counter_totals! {
    /// The packets each receiver handed on, turbine's shreds by layer, and what gossip and repair
    /// sent. Receivers share field names, so only the layers are read by name alone.
    pub struct NetworkTotals {
        pub shreds_turbine: u64,
        pub shreds_repair: u64,
        pub packets_gossip: u64,
        pub packets_tpu_vote: u64,
        pub turbine_root: u64 = "num_shreds_received_root",
        pub turbine_layer_1: u64 = "num_shreds_received_1st_layer",
        pub turbine_layer_2: u64 = "num_shreds_received_2nd_layer",
        pub turbine_layer_3: u64 = "num_shreds_received_3rd_layer",
        pub xdp_dropped: u64,
        pub gossip_sent_bytes: u64,
        pub gossip_sent_millis: u64,
        pub repair_sent_bytes: u64,
        pub repair_sent_millis: u64,
    }
}

counter_totals! {
    /// Reads are counted in accounts, since nothing on the load path counts bytes.
    pub struct AccountsTotals {
        pub loaded_from_write_cache: u64 = "num_loaded_from_write_cache",
        pub loaded_from_read_cache: u64 = "num_loaded_from_read_cache",
        pub loaded_from_storage: u64 = "num_loaded_from_index_storage",
        pub stored_accounts: u64 = "num_accounts_stored",
        pub stored_bytes: u64 = "account_bytes_stored",
        pub cache_hits: u64 = "read_only_accounts_cache_hits",
        pub cache_misses: u64 = "read_only_accounts_cache_misses",
        pub cache_evicts: u64 = "read_only_accounts_cache_evicts",
    }
}

/// Levels. The difference between the two storage figures is what shrink reclaims.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountsLevels {
    pub storage_bytes: u64,
    pub storage_alive_bytes: u64,
    pub storage_count: u64,
    pub cache_bytes: u64,
    pub cache_entries: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountsSet {
    pub totals: AccountsTotals,
    pub levels: AccountsLevels,
}

counter_totals! {
    /// What the index was asked, and what clean and shrink did. The index reports every ten
    /// seconds, clean once a run and shrink once a second.
    pub struct AccountsUpkeepTotals {
        pub index_found: u64,
        /// With the index all in memory, lookups of accounts that do not exist.
        pub index_missing: u64,
        pub index_inserts: u64,
        pub index_deletes: u64,
        pub clean_runs: u64,
        pub cleaned_slots: u64,
        pub cleaned_accounts: u64,
        pub shrunk_slots: u64,
        pub shrunk_accounts: u64,
        /// Dead bytes dropped, and live bytes written again to drop them.
        pub shrunk_bytes: u64,
        pub rewritten_bytes: u64,
        /// How long each job ran, counted as it ends.
        pub clean_micros: u64,
        pub shrink_micros: u64,
        pub flush_micros: u64,
    }
}

/// Levels, as last reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct AccountsUpkeepLevels {
    pub index_entries: u64,
    /// The slots of the index's hash maps, which is what its memory follows.
    pub index_capacity: u64,
    pub index_bytes: u64,
    pub write_cache_bytes: u64,
    pub write_cache_slots: u64,
    pub write_cache_accounts: u64,
    pub cleaned_to: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountsUpkeep {
    pub totals: AccountsUpkeepTotals,
    pub levels: AccountsUpkeepLevels,
    /// When the clean, shrink and flush now running began.
    began: [Option<SystemTime>; 3],
}

/// Sizes the validator's parts report of themselves; `None` until first reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MemoryParts {
    /// Agave's own estimate, which counts one slot list entry per account.
    pub index_estimate: Option<u64>,
    pub write_cache: Option<u64>,
    pub read_cache: Option<u64>,
    /// Memtables, block cache and table readers, summed over the column families.
    pub blockstore: Option<u64>,
}

/// The blockstore's table files by what they hold. Excludes the write-ahead log and manifests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerFiles {
    pub shreds: u64,
    pub history: u64,
    pub metadata: u64,
}

impl LedgerFiles {
    fn add(&mut self, column_family: &str, bytes: u64) {
        let part = match column_family {
            "data_shred" | "alt_data_shred" | "code_shred" => &mut self.shreds,
            "transaction_status" | "address_signatures" | "transaction_memos" | "rewards" => {
                &mut self.history
            }
            _ => &mut self.metadata,
        };
        *part = part.saturating_add(bytes);
    }
}

/// Bytes written to the storage files, across every accounts path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountsFiles {
    pub live: u64,
    pub fragmented: u64,
}

#[derive(Debug, Default)]
struct MemorySet {
    index_estimate: Option<u64>,
    write_cache: Option<u64>,
    read_cache: Option<u64>,
    blockstore: BTreeMap<String, u64>,
    blockstore_files: BTreeMap<String, u64>,
}

counter_totals! {
    pub struct ProgramCacheTotals {
        pub hits: u64 = "hits",
        pub misses: u64 = "misses",
        pub evictions: u64 = "evictions",
        pub reloads: u64 = "reloads",
        pub insertions: u64 = "insertions",
        pub lost_insertions: u64 = "lost_insertions",
        pub replacements: u64 = "replace_entry",
        pub one_hit_wonders: u64 = "one_hit_wonders",
        pub prunes_orphan: u64 = "prunes_orphan",
        pub prunes_environment: u64 = "prunes_environment",
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProgramCacheSet {
    pub totals: ProgramCacheTotals,
    /// Entries loaded when an eviction last ran, reset with each bank, so the panel takes the
    /// window's peak.
    pub water_level: u64,
}

counter_totals! {
    /// One window of a QUIC port's counters. They do not partition the offer: the listener drops
    /// uncounted on either side of the handshake.
    pub struct QuicTotals {
        /// Cumulative on the wire; the denominator for the rest.
        pub offered: u64,
        pub shed_all: u64 = "connection_rate_limited_across_all",
        pub shed_address: u64 = "connection_rate_limited_per_ipaddr",
        pub refused_full: u64 = "refused_connections_too_many_open_connections",
        pub handshake_timeout: u64 = "connection_setup_timeout",
        pub handshake_error: u64 = "connection_setup_error",
        pub handshook: u64 = "new_connections",
        /// Four overlapping counters for one event, never summed; see `refusedTable` in `tpuPath.ts`.
        pub add_failed: u64 = "connection_add_failed",
        /// `connection_add_failed_on_pruning` is raised on the same refusal and would count one
        /// event twice.
        pub add_failed_staked: u64 = "connection_add_failed_staked_node",
        pub add_failed_unstaked: u64 = "connection_add_failed_unstaked_node",
        pub add_failed_banned: u64 = "connection_add_failed_banned",
        pub admitted_staked: u64 = "connection_added_from_staked_peer",
        pub admitted_unstaked: u64 = "connection_added_from_unstaked_peer",
        pub streams: u64 = "new_streams",
        pub throttled_staked: u64 = "throttled_staked_streams",
        pub throttled_unstaked: u64 = "throttled_unstaked_streams",
        pub read_timeouts: u64 = "stream_read_timeouts",
        pub read_errors: u64 = "stream_read_errors",
        pub invalid_size: u64 = "invalid_stream_size",
        pub handed_on: u64 = "packets_sent_to_consumer",
        /// The one row here meaning this validator could not keep up.
        pub queue_full: u64 = "total_handle_chunk_to_packet_send_full_err",
        pub disconnected: u64 = "total_handle_chunk_to_packet_send_disconnected_err",
    }
}

/// Kept apart from the counters so a window cannot sum them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct QuicLevels {
    pub open: u64,
    pub active_streams: u64,
}

/// One QUIC port: who was let in, what they sent, and what got through.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuicSet {
    pub totals: QuicTotals,
    pub levels: QuicLevels,
}

counter_totals! {
    pub struct BundleTotals {
        pub received: u64 = "num_bundles_received",
        pub packets: u64 = "num_packets_received",
    }
}

counter_totals! {
    pub struct VerifyTotals {
        pub received: u64 = "total_packets",
        /// Ordinary: the network sends transactions more than once.
        pub duplicate: u64 = "total_dedup",
        pub below_floor: u64 = "total_dropped_below_priority_floor",
        pub verified: u64 = "total_valid_packets",
        /// Batches, not transactions, so never added to a packet count.
        pub evicted_batches: u64 = "eviction_drops",
    }
}

counter_totals! {
    /// `max_queue_len` is a gauge, `num_messages_processed` counts batches, and `total` sums
    /// errors drawn elsewhere, so none is read.
    pub struct ExecutedTotals {
        pub attempted: u64 = "transactions_attempted_processing_count",
        pub cost_throttled: u64 = "cost_model_throttled_transactions_count",
        pub retryable: u64 = "retryable_transaction_count",
        pub expired_bank: u64 = "retryable_expired_bank_count",
        pub processed: u64 = "processed_transactions_count",
        /// The rest landed having failed, which still costs their fee.
        pub succeeded: u64 = "processed_with_successful_result_count",

        // From the error point beside it. No name is shared with the counts point, so both are
        // read here.
        pub too_many_locks: u64 = "too_many_account_locks",
        pub account_missing: u64 = "account_not_found",
        pub fee_payer_broke: u64 = "insufficient_funds",
        pub fee_payer_invalid: u64 = "invalid_account_for_fee",
        pub blockhash_missing: u64 = "blockhash_not_found",
        pub blockhash_old: u64 = "blockhash_too_old",
        pub already_processed: u64 = "already_processed",
        pub bad_compute_budget: u64 = "invalid_compute_budget",
        pub program_not_executable: u64 = "invalid_program_for_execution",
    }
}

counter_totals! {
    pub struct SchedulerTotals {
        pub received: u64 = "num_received",

        // Lost at the door, before ever being buffered.
        /// Not held because the validator was forwarding rather than buffering.
        pub not_held: u64 = "num_dropped_on_receive",
        pub check_queue_full: u64 = "num_dropped_on_check_work_queue_full",
        pub unparsable: u64 = "num_dropped_on_parsing_and_sanitization",
        pub bad_locks: u64 = "num_dropped_on_validate_locks",
        pub compute_budget: u64 = "num_dropped_on_receive_compute_budget",
        pub too_old: u64 = "num_dropped_on_receive_age",
        pub already_processed: u64 = "num_dropped_on_receive_already_processed",
        pub fee_payer: u64 = "num_dropped_on_receive_fee_payer",
        pub filtered: u64 = "num_dropped_on_filter_key",
        pub nonce_conflict: u64 = "num_dropped_on_nonce_dedup",

        pub buffered: u64 = "num_buffered",

        // Lost from the container, after being buffered.
        /// Pushed out by something of higher priority when the queue was full.
        pub queue_full: u64 = "num_dropped_on_capacity",
        pub nonce_evicted: u64 = "num_evicted_on_nonce_dedup",
        pub cleared: u64 = "num_dropped_on_clear",
        pub cleaned: u64 = "num_dropped_on_clean",

        pub scheduled: u64 = "num_scheduled",
        /// Held back this pass for account conflicts or busy workers. Pressure, not
        /// losses.
        pub blocked_conflicts: u64 = "num_unschedulable_conflicts",
        pub blocked_threads: u64 = "num_unschedulable_threads",

        pub finished: u64 = "num_finished",
        pub retried: u64 = "num_retryable",
    }
}

counter_totals! {
    /// Messages are counted in packets and the rest in entries; the times are in microseconds.
    pub struct GossipTotals {
        /// Every packet read off gossip's sockets, before any check.
        pub received: u64 = "packets_received_count",
        pub received_push: u64 = "packets_received_push_messages_count",
        pub received_pull_requests: u64 = "packets_received_pull_requests_count",
        pub received_pull_responses: u64 = "packets_received_pull_responses_count",
        pub received_ping: u64 = "packets_received_ping_messages_count",
        pub received_pong: u64 = "packets_received_pong_messages_count",
        pub received_prune: u64 = "packets_received_prune_messages_count",
        pub sent_push: u64 = "packets_sent_push_messages_count",
        pub sent_pull_requests: u64 = "packets_sent_pull_requests_count",
        pub sent_pull_responses: u64 = "packets_sent_pull_responses_count",
        pub sent_ping: u64 = "packets_sent_ping_messages_count",
        pub sent_pong: u64 = "packets_sent_pong_messages_count",
        pub sent_prune: u64 = "packets_sent_prune_messages_count",

        pub accepted_push: u64,
        pub accepted_pull: u64,
        pub rejected_push: u64,
        pub rejected_pull: u64,
        pub duplicate_push: u64 = "num_duplicate_push_messages",
        pub redundant_pull: u64 = "num_redundant_pull_responses",
        /// Timed out of the table.
        pub expired: u64 = "purge_count",
        /// Trimmed once the table held more distinct pubkeys than it keeps.
        pub evicted: u64 = "trim_crds_table_purged_values_count",

        pub dropped_in: u64 = "gossip_packets_dropped_count",
        pub dropped_out: u64 = "gossip_transmit_packets_dropped_count",
        pub pull_no_budget: u64 = "gossip_pull_request_no_budget",
        pub pull_scan_exhausted: u64 = "pull_request_scan_budget_exhausted",
        pub other_shred_version_push: u64 = "skip_push_message_shred_version",
        pub other_shred_version_pull_responses: u64 = "skip_pull_response_shred_version",
        pub other_shred_version_pull_requests: u64 = "skip_pull_shred_version",
        pub ping_check_failed: u64 = "pull_request_ping_pong_check_failed_count",
        /// Values whose sender has not yet answered a ping. Gossip's own spelling.
        pub unverified_addresses: u64 = "num_unverifed_gossip_addrs",
        pub bad_prune_destination: u64 = "bad_prune_destination",

        pub push_us: u64 = "handle_batch_push_messages_time",
        pub pull_requests_us: u64 = "handle_batch_pull_requests_time",
        pub pull_responses_us: u64 = "handle_batch_pull_responses_time",
        pub ping_us: u64 = "handle_batch_ping_messages_time",
        pub pong_us: u64 = "handle_batch_pong_messages_time",
        pub prune_us: u64 = "handle_batch_prune_messages_time",
        pub verify_us: u64 = "verify_gossip_packets_time",
        /// Includes the six handling times above.
        pub process_us: u64 = "process_gossip_packets_time",

        /// Points of the first kind seen, so a quiet gossip can be told from a silent one.
        pub reports: u64,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GossipLevels {
    pub table_size: u64,
    pub pubkeys: u64,
    pub nodes: u64,
    pub staked_nodes: u64,
}

/// Per entry type, in the order of `GOSSIP_ENTRY_TYPES`; rejected is push and pull together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GossipEntryTotals {
    pub push: [u64; GOSSIP_ENTRY_TYPES.len()],
    pub pull: [u64; GOSSIP_ENTRY_TYPES.len()],
    pub rejected: [u64; GOSSIP_ENTRY_TYPES.len()],
}

impl WindowedCounters for GossipEntryTotals {
    fn since(&self, previous: &Self) -> Self {
        let less = |a: &[u64; GOSSIP_ENTRY_TYPES.len()], b: &[u64; GOSSIP_ENTRY_TYPES.len()]| {
            std::array::from_fn(|index| a[index].saturating_sub(b[index]))
        };
        Self {
            push: less(&self.push, &previous.push),
            pull: less(&self.pull, &previous.pull),
            rejected: less(&self.rejected, &previous.rejected),
        }
    }

    fn plus(&self, other: &Self) -> Self {
        let more = |a: &[u64; GOSSIP_ENTRY_TYPES.len()], b: &[u64; GOSSIP_ENTRY_TYPES.len()]| {
            std::array::from_fn(|index| a[index].saturating_add(b[index]))
        };
        Self {
            push: more(&self.push, &other.push),
            pull: more(&self.pull, &other.pull),
            rejected: more(&self.rejected, &other.rejected),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GossipSet {
    pub totals: GossipTotals,
    pub levels: GossipLevels,
    pub entries: GossipEntryTotals,
}

/// Every set the points add into, under one lock so a reading is whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TapCounters {
    pub network: NetworkTotals,
    /// The `is_xdp` tag retransmit last sent.
    pub retransmit_xdp: Option<bool>,
    pub accounts: AccountsSet,
    pub accounts_upkeep: AccountsUpkeep,
    pub program_cache: ProgramCacheSet,
    pub quic: QuicSet,
    pub quic_forwards: QuicSet,
    pub quic_vote: QuicSet,
    /// Separate sets, since the stages either side of the scheduler do not reconcile.
    pub verify: VerifyTotals,
    pub executed: ExecutedTotals,
    pub bundles: BundleTotals,
    pub gossip: GossipSet,
}

impl MetricsTap {
    #[cfg(test)]
    pub(crate) fn observe_point(&self, point: &DataPoint) {
        self.observe_at(point, timestamp());
    }

    /// A point read from the validator's metrics export, after the fact: what the tap would stamp
    /// with its arrival takes the point's own time instead.
    pub fn observe_external(&self, point: &DataPoint) {
        let at_millis = system_time_nanos(point.timestamp) / 1_000_000;
        {
            let mut external = locked(&self.external);
            let step =
                at_millis.saturating_sub(at_millis.checked_rem(COUNTER_STEP_MILLIS).unwrap_or(0));
            // Further back than the steps reach: the validator's clock was set back, and the
            // steps kept count time on the clock it left.
            if external
                .steps
                .back()
                .is_some_and(|(newest, _)| step.saturating_add(COUNTER_STEPS_MILLIS) < *newest)
            {
                external.steps.clear();
                external.through_millis = 0;
            }
            if external
                .steps
                .back()
                .is_none_or(|(newest, _)| step > *newest)
            {
                let counters = self.counters();
                push_capped(&mut external.steps, (step, counters), COUNTER_STEPS);
            }
            external.through_millis = external.through_millis.max(at_millis);
        }
        self.observe_at(point, at_millis);
    }

    /// The newest point time read after the fact, in unix milliseconds.
    pub fn points_through(&self) -> Option<u64> {
        let through = locked(&self.external).through_millis;
        (through > 0).then_some(through)
    }

    /// The counters as they stood at `at_millis` of point time, up to [`COUNTER_STEP_MILLIS`] later;
    /// `None` until a point made after that has been read.
    pub fn counters_at(&self, at_millis: u64) -> Option<TapCounters> {
        locked(&self.external)
            .steps
            .iter()
            .find(|(step, _)| *step > at_millis)
            .map(|(_, counters)| *counters)
    }

    /// `now_millis` stands for when the point was made, which the arrival-stamped figures read.
    fn observe_at(&self, point: &DataPoint, now_millis: u64) {
        locked(&self.counters).add_point(point);
        match point.name {
            ACCOUNTS_DB_TIMINGS | ACCOUNTS_INDEX | ACCOUNTS_WRITE_CACHE | BLOCKSTORE_CFS => {
                locked(&self.memory).add_point(point)
            }
            SCHEDULER_SLOT_COUNTS => self.remember_slot(point),
            REPLAY_SLOT_STATS => self.remember_replay(point, now_millis),
            VOTE_TRACKING => self.remember_vote_track(point, now_millis),
            VOTE_ARRIVAL => self.remember_vote_arrival(point),
            LEADER_START | LEADER_BANK | BROADCAST_TRANSMIT | BROADCAST_PROCESS => {
                self.remember_leader_timing(point)
            }
            SCHEDULER_SLOT_TIMING if scheduler_source(point) == SchedulerSource::Scheduler => {
                self.remember_leader_timing(point)
            }
            SHRED_FULL => self.remember_fill(point),
            NEW_BANK => self.remember_height(point),
            BUNDLE_SLOT_STATS => self.remember_bundles(point),
            WORKER_TIMING => self.remember_worker_timing(point, now_millis),
            VOTE_SLOT_TIMING => self.remember_vote_timing(point),
            XDP_NETWORK_CONFIG => self.remember_xdp(point),
            WFSM_GOSSIP => self.remember_stake_in_gossip(point),
            COST_TRACKER => self.remember_cost(point),
            _ => (),
        }
    }

    fn remember_slot(&self, point: &DataPoint) {
        let Some(slot) = number_named(point, SLOT) else {
            return;
        };

        let mut counts = SchedulerTotals::default();
        counts.add_point(point);
        let waterfall = SlotWaterfall {
            slot,
            source: scheduler_source(point),
            counts,
        };

        let mut slots = locked(&self.slot_waterfalls);
        // A build running two schedulers reports every leader slot twice; keep
        // the report that did the work.
        if slots.put(slot, waterfall, |held| {
            describes_more_work(&waterfall.counts, &held.counts)
        }) {
            self.note_slot_lists_changed();
        }
    }

    fn note_slot_lists_changed(&self) {
        self.slot_lists_revision.fetch_add(1, Ordering::Relaxed);
    }

    /// `driver` and `zero_copy` are tags; an unparseable `zero_copy` reads as false.
    fn remember_xdp(&self, point: &DataPoint) {
        let tag = |wanted| tag_named(point, wanted);
        let field = |wanted| {
            field_named(point, wanted)
                .map(field_str)
                .unwrap_or_default()
        };
        let config = XdpConfig {
            zero_copy: tag("zero_copy") == Some("true"),
            driver: tag("driver").unwrap_or_default().to_string(),
            vendor: field("vendor"),
            model: field("model"),
            kernel_version: field("kernel_version"),
        };
        *locked(&self.xdp) = Some(config);
    }

    fn remember_leader_timing(&self, point: &DataPoint) {
        let field = |wanted| number_named(point, wanted);
        let Some(slot) = field(SLOT) else {
            return;
        };
        let mut timings = locked(&self.leader_timings);
        let timing = timings.entry(slot).or_default();
        match point.name {
            LEADER_START => {
                if let Some(start) = field("slot_delay_us") {
                    timing.start_micros = Some(start);
                    timing.parent_wait_micros = field("replay_is_behind_us");
                }
                timing.fast_handover |= field("leader_handover_fast") == Some(1);
                timing.parent_switched |= field("leader_handover_sad") == Some(1);
            }
            LEADER_BANK => timing.open_millis = field("elapsed"),
            BROADCAST_TRANSMIT => timing.broadcast_micros = field("end_to_end_elapsed"),
            BROADCAST_PROCESS => timing.shredding_micros = field("shredding_time"),
            _ => {
                timing.scheduler_receive_micros = field("receive_time_us");
                timing.scheduler_schedule_micros = field("schedule_time_us");
            }
        }
        keep_newest(&mut timings, PENDING_SLOTS);
    }

    fn remember_vote_arrival(&self, point: &DataPoint) {
        let Some(vote) = tag_named(point, VOTE_ACCOUNT).and_then(|key| Pubkey::from_str(key).ok())
        else {
            return;
        };
        let field = |wanted| number_named(point, wanted);
        // Absent where the validator sent no notarize vote that epoch.
        let (Some(epoch), Some(notarize_mean_micros)) =
            (field("epoch"), field("notar_vote_us_mean"))
        else {
            return;
        };
        let arrival = VoteArrival {
            notarize_mean_micros,
            notarize_spread_micros: field("notar_vote_us_stddev"),
            notarize_worst_micros: field("notar_vote_us_maximum"),
            notarize_count: field("notar_vote_count"),
            finalize_mean_micros: field("finalize_vote_us_mean"),
        };
        let mut arrivals = locked(&self.vote_arrivals);
        match arrivals.epoch {
            Some(held) if epoch < held => return,
            Some(held) if epoch == held => (),
            _ => {
                arrivals.epoch = Some(epoch);
                arrivals.by_vote.clear();
            }
        }
        arrivals.by_vote.insert(vote, arrival);
    }

    fn remember_vote_track(&self, point: &DataPoint, now_millis: u64) {
        let field = |wanted| number_named(point, wanted);
        let Some(slot) = field(SLOT) else {
            return;
        };
        // Only a late fast certificate can make this read fast for a slot the second round
        // finalized, and the pool prunes a slot too soon after its root for that to be common.
        if let Some(fast) = field_named(point, "is_fast_finalization") {
            push_capped(
                &mut locked(&self.finalized_paths),
                (slot, fast == "true"),
                VOTE_TRACKS,
            );
        }
        let vote = VoteSent {
            first_shred_us: field("first_shred"),
            parent_ready_us: field("parent_ready"),
            notarize_us: field("vote_notarize"),
            skip_us: field("vote_skip"),
        };
        if let Some(first_shred_us) = vote.first_shred_us {
            self.remember_after_last_shred(
                slot,
                first_shred_us,
                field("finalized"),
                vote.notarize_us,
                now_millis,
            );
        }
        insert_capped(&mut locked(&self.vote_tracks), slot, vote, VOTE_TRACKS);
    }

    /// Every time counts from votor's start on the slot, and the block's arrival from its first
    /// shred, so the gaps after the last shred need no clock of their own.
    fn remember_after_last_shred(
        &self,
        slot: Slot,
        first_shred_us: u64,
        finalized_us: Option<u64>,
        notarize_us: Option<u64>,
        now_millis: u64,
    ) {
        let Some(full_millis) = self.shred_fill(slot).map(|fill| fill.full_millis) else {
            return;
        };
        let last_shred_us = first_shred_us.saturating_add(full_millis.saturating_mul(1_000));
        let finalized = finalized_us.and_then(|at| at.checked_sub(last_shred_us));
        let vote = notarize_us.and_then(|at| at.checked_sub(last_shred_us));
        if let Some(micros) = finalized {
            self.time_from_leader(slot, micros, now_millis);
        }
        if let Some(micros) = vote {
            self.vote_lag.push(now_millis, micros);
        }
        if vote.is_some() || finalized.is_some() {
            let votes = BlockVotes {
                slot,
                vote_us: vote,
                finalized_us: finalized,
            };
            push_capped(&mut locked(&self.block_votes), votes, VOTE_TRACKS);
        }
    }

    /// Each slot's, once.
    pub fn take_block_votes(&self) -> Vec<BlockVotes> {
        Vec::from(std::mem::take(&mut *locked(&self.block_votes)))
    }

    /// The median time from the leader closing a block to its finalization here, over the window.
    pub fn finality_from_leader_micros(&self, now_millis: u64, window_millis: u64) -> Option<u64> {
        self.from_leader.median(now_millis, window_millis)
    }

    /// The footer's producer clock for `slot`, from the certificate walk.
    pub fn note_block_closed(&self, slot: Slot, closed_at_nanos: u64) {
        let mut closed = locked(&self.closed_blocks);
        insert_capped(&mut closed.closed_at, slot, closed_at_nanos, SHRED_FILLS);
        let waiting = closed.waiting.remove(&slot);
        drop(closed);
        if let Some(finality_us) = waiting {
            self.push_from_leader(slot, closed_at_nanos, finality_us, timestamp());
        }
    }

    /// Votor usually reports after the walk has read the footer; otherwise the finality waits.
    fn time_from_leader(&self, slot: Slot, finality_us: u64, now_millis: u64) {
        let mut closed = locked(&self.closed_blocks);
        let Some(&closed_at_nanos) = closed.closed_at.get(&slot) else {
            insert_capped(&mut closed.waiting, slot, finality_us, SHRED_FILLS);
            return;
        };
        drop(closed);
        self.push_from_leader(slot, closed_at_nanos, finality_us, now_millis);
    }

    /// A leader whose clock runs ahead of ours reads negative, and that sample is left out.
    fn push_from_leader(
        &self,
        slot: Slot,
        closed_at_nanos: u64,
        finality_us: u64,
        now_millis: u64,
    ) {
        let Some(full_at_nanos) = self
            .shred_fill(slot)
            .map(|fill| fill.full_at_nanos)
            .filter(|nanos| *nanos > 0)
        else {
            return;
        };
        let Some(rotor_us) = full_at_nanos
            .checked_sub(closed_at_nanos)
            .and_then(|nanos| nanos.checked_div(1_000))
        else {
            return;
        };
        self.from_leader
            .push(now_millis, rotor_us.saturating_add(finality_us));
    }

    /// The median time from last shred to our notarize vote over the samples reported in the window.
    pub fn vote_lag_micros(&self, now_millis: u64, window_millis: u64) -> Option<u64> {
        self.vote_lag.median(now_millis, window_millis)
    }

    pub fn take_finalized_paths(&self) -> Vec<(Slot, bool)> {
        Vec::from(std::mem::take(&mut *locked(&self.finalized_paths)))
    }

    pub fn take_vote_tracks(&self) -> Vec<(Slot, VoteSent)> {
        std::mem::take(&mut *locked(&self.vote_tracks))
            .into_iter()
            .collect()
    }

    /// The first `slot` field stands: the loop reads only fields that name a counter.
    fn remember_replay(&self, point: &DataPoint, now_millis: u64) {
        let mut slot = ReplaySlotTimes {
            slot: number_named(point, SLOT).unwrap_or(0),
            observed_millis: now_millis,
            ..ReplaySlotTimes::default()
        };
        let mut seen = false;
        for (name, value) in &point.fields {
            if let (Some(counter), Some(micros)) = (slot.counter(name), field_u64(value)) {
                *counter = counter.saturating_add(micros);
                seen = true;
            }
        }

        // A point naming nothing read here would drag every mean towards nought.
        if !seen {
            return;
        }

        // A slot replayed twice keeps the later report. Bounded from the lowest
        // slot, which is the oldest during ordinary running.
        insert_capped(
            &mut locked(&self.replay_slots),
            slot.slot,
            slot,
            REPLAY_SLOTS,
        );
    }

    /// The -1 the blockstore sends for an unknown last index drops the point.
    fn remember_fill(&self, point: &DataPoint) {
        let field = |wanted| number_named(point, wanted);
        let (Some(slot), Some(last_index)) = (field(SLOT), field("last_index")) else {
            return;
        };
        let fill = ShredFill {
            shreds: last_index.saturating_add(1),
            repaired: field("num_repaired").unwrap_or(0),
            recovered: field("num_recovered").unwrap_or(0),
            full_millis: field("total_time_ms").unwrap_or(0),
            full_at_nanos: system_time_nanos(point.timestamp),
        };
        insert_capped(&mut locked(&self.shred_fills), slot, fill, SHRED_FILLS);
    }

    fn remember_height(&self, point: &DataPoint) {
        let field = |wanted| number_named(point, wanted);
        if let (Some(slot), Some(height)) = (field(SLOT), field("block_height")) {
            insert_capped(
                &mut locked(&self.block_heights),
                slot,
                height,
                BLOCK_HEIGHTS,
            );
        }
    }

    fn remember_worker_timing(&self, point: &DataPoint, at_millis: u64) {
        let mut times = StageTimes::default();
        times.add_point(point);
        let timing = WorkerTiming {
            at_millis,
            worker: tag_named(point, WORKER_ID)
                .and_then(|id| id.parse().ok())
                .unwrap_or(u64::MAX),
            times,
            longest_batch: number_named(point, "load_execute_us_max").unwrap_or(0),
        };
        push_capped(&mut locked(&self.worker_timings), timing, WORKER_TIMINGS);
    }

    fn remember_vote_timing(&self, point: &DataPoint) {
        let Some(slot) = number_named(point, SLOT) else {
            return;
        };
        let mut times = StageTimes::default();
        times.add_point(point);
        insert_capped(&mut locked(&self.vote_timings), slot, times, PENDING_SLOTS);
    }

    fn remember_bundles(&self, point: &DataPoint) {
        let Some(slot) = number_named(point, SLOT) else {
            return;
        };
        let mut slots = locked(&self.bundle_slots);
        slots.entry(slot).or_default().add_point(point);
        keep_newest(&mut slots, PENDING_SLOTS);
    }

    fn remember_cost(&self, point: &DataPoint) {
        let Some(slot) = number_named(point, "bank_slot") else {
            return;
        };
        let field = |wanted| number_named(point, wanted).unwrap_or(0);
        let figures = BankFigures {
            block_cost: field("block_cost"),
            transactions: field("transaction_count"),
            total_fees: field("total_transaction_fee"),
            priority_fees: field("total_priority_fee"),
        };
        insert_capped(&mut locked(&self.bank_figures), slot, figures, BANK_FIGURES);
        if tag_named(point, IS_LEADER) != Some("true") {
            return;
        }
        let cost = SlotCost {
            slot,
            costliest_account: field_named(point, "costliest_account")
                .unwrap_or_default()
                .trim_matches('"')
                .to_string(),
            costliest_cost: field("costliest_account_cost"),
            block_cost: figures.block_cost,
            accounts: field("number_of_accounts"),
            contended: field("number_of_contended_accounts"),
            new_account_data: field("allocated_accounts_data_size"),
            in_flight: field("inflight_transaction_count"),
        };
        if locked(&self.slot_costs).put(slot, cost, |_| true) {
            self.note_slot_lists_changed();
        }
    }

    pub fn slot_lists_revision(&self) -> u64 {
        self.slot_lists_revision.load(Ordering::Relaxed)
    }

    /// The newest `count`, oldest first.
    pub fn recent_costs(&self, count: usize) -> Vec<SlotCost> {
        locked(&self.slot_costs).recent(count)
    }

    /// Those recorded or replaced since the last call, oldest first.
    pub fn take_changed_costs(&self) -> Vec<SlotCost> {
        locked(&self.slot_costs).take_changed()
    }

    pub fn cost(&self, slot: Slot) -> Option<SlotCost> {
        locked(&self.slot_costs).held.get(&slot).cloned()
    }

    /// Reported shortly after the bank froze, and never for one without transactions.
    pub fn bank_figures(&self, slot: Slot) -> Option<BankFigures> {
        locked(&self.bank_figures).get(&slot).copied()
    }

    /// How often each of `accounts` was a held block's costliest, and its costliest block, in
    /// one pass over the blocks held.
    pub fn costliest_recurrences(&self, accounts: &[&str]) -> HashMap<String, Recurrence> {
        let costs = locked(&self.slot_costs);
        let of = costs.held.len() as u64;
        let mut found: HashMap<String, Recurrence> = HashMap::new();
        for cost in costs.held.values() {
            if !accounts.contains(&cost.costliest_account.as_str()) {
                continue;
            }
            let seen = found
                .entry(cost.costliest_account.clone())
                .or_insert(Recurrence {
                    blocks: 0,
                    of,
                    peak_cost: cost.costliest_cost,
                    peak_slot: cost.slot,
                });
            seen.blocks = seen.blocks.saturating_add(1);
            if cost.costliest_cost > seen.peak_cost {
                seen.peak_cost = cost.costliest_cost;
                seen.peak_slot = cost.slot;
            }
        }
        found
    }

    /// Our leader slots' reports older than `floor` are dropped and refused from then on.
    pub fn hold_slots_from(&self, floor: Slot) {
        locked(&self.slot_waterfalls).hold_from(floor);
        locked(&self.slot_costs).hold_from(floor);
        self.note_slot_lists_changed();
    }

    #[cfg(test)]
    pub fn slot_costs(&self) -> Vec<SlotCost> {
        self.recent_costs(usize::MAX)
    }

    fn remember_stake_in_gossip(&self, point: &DataPoint) {
        let field = |wanted| number_named(point, wanted).unwrap_or(0);
        let seen = StakeInGossip {
            online: field("online_stake"),
            total: field("total_activated_stake"),
        };
        if seen.total > 0 {
            *locked(&self.stake_in_gossip) = Some(seen);
        }
    }

    pub fn stake_in_gossip(&self) -> Option<StakeInGossip> {
        *locked(&self.stake_in_gossip)
    }

    pub fn memory_parts(&self) -> MemoryParts {
        let memory = locked(&self.memory);
        MemoryParts {
            index_estimate: memory.index_estimate,
            write_cache: memory.write_cache,
            read_cache: memory.read_cache,
            blockstore: (!memory.blockstore.is_empty()).then(|| {
                memory
                    .blockstore
                    .values()
                    .copied()
                    .fold(0, u64::saturating_add)
            }),
        }
    }

    /// `None` until a column family has reported.
    pub fn ledger_files(&self) -> Option<LedgerFiles> {
        let memory = locked(&self.memory);
        (!memory.blockstore_files.is_empty()).then(|| {
            let mut files = LedgerFiles::default();
            for (column_family, bytes) in &memory.blockstore_files {
                files.add(column_family, *bytes);
            }
            files
        })
    }

    /// `None` until the storages have reported.
    pub fn accounts_files(&self) -> Option<AccountsFiles> {
        let levels = locked(&self.counters).accounts.levels;
        (levels.storage_bytes > 0).then_some(AccountsFiles {
            live: levels.storage_alive_bytes,
            fragmented: levels
                .storage_bytes
                .saturating_sub(levels.storage_alive_bytes),
        })
    }

    pub fn xdp(&self) -> Option<XdpConfig> {
        locked(&self.xdp).clone()
    }

    pub fn replayed(&self, slot: Slot) -> Option<ReplaySlotTimes> {
        locked(&self.replay_slots).get(&slot).copied()
    }

    pub fn shred_fill(&self, slot: Slot) -> Option<ShredFill> {
        locked(&self.shred_fills).get(&slot).copied()
    }

    /// `slot`'s block height: the newest bank on its chain the metrics reported, plus one for each
    /// block after it, `parent` walking the chain down.
    pub fn block_height(&self, slot: Slot, parent: impl Fn(Slot) -> Option<Slot>) -> Option<u64> {
        let heights = locked(&self.block_heights);
        let (mut at, mut above) = (slot, 0u64);
        loop {
            if let Some(height) = heights.get(&at) {
                return height.checked_add(above);
            }
            at = parent(at).filter(|parent| *parent < at)?;
            above = above.saturating_add(1);
        }
    }

    /// The newest slot whose fill has been reported.
    pub fn newest_fill(&self) -> Option<Slot> {
        locked(&self.shred_fills).keys().next_back().copied()
    }

    pub fn worker_time(&self, from: u64, to: u64) -> Option<WorkerSum> {
        let timings = locked(&self.worker_timings);
        let mut sum = WorkerSum::default();
        let mut workers = BTreeSet::new();
        let first = timings.partition_point(|timing| timing.at_millis < from);
        for timing in timings
            .range(first..)
            .take_while(|timing| timing.at_millis < to)
        {
            sum.times = sum.times.plus(&timing.times);
            sum.longest_batch = sum.longest_batch.max(timing.longest_batch);
            workers.insert(timing.worker);
        }
        if workers.is_empty() {
            return None;
        }
        sum.workers = workers.len() as u64;
        Some(sum)
    }

    pub fn leader_timing(&self, slot: Slot) -> Option<LeaderTiming> {
        locked(&self.leader_timings).get(&slot).copied()
    }

    /// The epoch they describe, and each vote account's arrivals.
    pub fn vote_arrival_epoch(&self) -> Option<Epoch> {
        locked(&self.vote_arrivals).epoch
    }

    pub fn vote_arrivals(&self) -> (Option<Epoch>, HashMap<Pubkey, VoteArrival>) {
        let arrivals = locked(&self.vote_arrivals);
        (arrivals.epoch, arrivals.by_vote.clone())
    }

    pub fn vote_time(&self, slot: Slot) -> Option<StageTimes> {
        locked(&self.vote_timings).get(&slot).copied()
    }

    pub fn bundles_landed(&self, slot: Slot) -> Option<Bundles> {
        locked(&self.bundle_slots).get(&slot).copied()
    }

    pub fn replay_slots(&self) -> Vec<ReplaySlotTimes> {
        locked(&self.replay_slots).values().copied().collect()
    }

    /// The newest `count`, oldest first.
    pub fn recent_waterfalls(&self, count: usize) -> Vec<SlotWaterfall> {
        locked(&self.slot_waterfalls).recent(count)
    }

    /// Those recorded or replaced since the last call, oldest first.
    pub fn take_changed_waterfalls(&self) -> Vec<SlotWaterfall> {
        locked(&self.slot_waterfalls).take_changed()
    }

    /// Those held in `first..=last`.
    pub fn waterfalls_in(&self, first: Slot, last: Slot) -> Vec<SlotWaterfall> {
        locked(&self.slot_waterfalls)
            .held
            .range(first..=last)
            .map(|(_, waterfall)| *waterfall)
            .collect()
    }

    #[cfg(test)]
    pub fn slot_waterfalls(&self) -> Vec<SlotWaterfall> {
        self.recent_waterfalls(usize::MAX)
    }

    pub fn counters(&self) -> TapCounters {
        *locked(&self.counters)
    }
}

/// A set of figures one kind of metrics point adds into, under one lock.
trait AddPoint {
    fn add_point(&mut self, point: &DataPoint);
}

impl AddPoint for TapCounters {
    fn add_point(&mut self, point: &DataPoint) {
        let network = &mut self.network;
        match point.name {
            SHREDS_TURBINE => add_named(&mut network.shreds_turbine, point, PACKETS_COUNT),
            SHREDS_REPAIR => add_named(&mut network.shreds_repair, point, PACKETS_COUNT),
            GOSSIP_RECEIVER => add_named(&mut network.packets_gossip, point, PACKETS_COUNT),
            TPU_VOTE_RECEIVER => add_named(&mut network.packets_tpu_vote, point, PACKETS_COUNT),
            GOSSIP_SENDER => {
                add_named(&mut network.gossip_sent_bytes, point, SENT_BYTES);
                add_named(&mut network.gossip_sent_millis, point, SENT_MILLIS);
            }
            REPAIR_SENDER => {
                add_named(&mut network.repair_sent_bytes, point, SENT_BYTES);
                add_named(&mut network.repair_sent_millis, point, SENT_MILLIS);
            }
            RETRANSMIT_STAGE => {
                add_named(
                    &mut network.xdp_dropped,
                    point,
                    "num_shreds_dropped_xdp_full",
                );
                if let Some(is_xdp) = tag_named(point, IS_XDP) {
                    self.retransmit_xdp = Some(is_xdp == "true");
                }
            }
            RETRANSMIT_SLOT_STATS => network.add_point(point),
            ACCOUNTS_DB_TIMINGS | ACCOUNTS_LOADS | ACCOUNTS_STORES | ACCOUNTS_FLUSH => {
                self.accounts.add_point(point)
            }
            ACCOUNTS_INDEX
            | ACCOUNTS_WRITE_CACHE
            | ACCOUNTS_CLEAN
            | ACCOUNTS_SHRINK
            | ACCOUNTS_SHRINK_ANCIENT
            | ACCOUNTS_ACTIVE => self.accounts_upkeep.add_point(point),
            PROGRAM_CACHE => {
                self.program_cache.totals.add_point(point);
                if let Some(level) = number_named(point, "water_level") {
                    self.program_cache.water_level = level;
                }
            }
            QUIC_TPU => self.quic.add_point(point),
            QUIC_TPU_FORWARDS => self.quic_forwards.add_point(point),
            QUIC_TPU_VOTE => self.quic_vote.add_point(point),
            TPU_VERIFIER => self.verify.add_point(point),
            BUNDLE_STAGE => self.bundles.add_point(point),
            WORKER_COUNTS | WORKER_ERROR_METRICS => self.executed.add_point(point),
            GOSSIP_ENTRIES | GOSSIP_ENTRY_FAILS => self.gossip.add_entries(point),
            name if GOSSIP_STATS.contains(&name) => self.gossip.add_stats(point),
            _ => (),
        }
    }
}

impl AddPoint for AccountsSet {
    fn add_point(&mut self, point: &DataPoint) {
        self.totals.add_point(point);
        let levels = &mut self.levels;
        for (name, value) in &point.fields {
            let level = match *name {
                "total_bytes" => &mut levels.storage_bytes,
                "total_alive_bytes" => &mut levels.storage_alive_bytes,
                "total_count" => &mut levels.storage_count,
                "read_only_accounts_cache_data_size" => &mut levels.cache_bytes,
                "read_only_accounts_cache_entries" => &mut levels.cache_entries,
                _ => continue,
            };
            set_value(level, value);
        }
    }
}

/// Matched by point as well as field: the two shrinks, clean and the flush share field names.
impl AddPoint for AccountsUpkeep {
    fn add_point(&mut self, point: &DataPoint) {
        let (totals, levels) = (&mut self.totals, &mut self.levels);
        if point.name == ACCOUNTS_CLEAN {
            totals.clean_runs = totals.clean_runs.saturating_add(1);
        }
        for (name, value) in &point.fields {
            if point.name == ACCOUNTS_ACTIVE {
                let (began, micros) = match *name {
                    "clean" => (&mut self.began[0], &mut totals.clean_micros),
                    "shrink" | "squash_ancient" => (&mut self.began[1], &mut totals.shrink_micros),
                    "flush" => (&mut self.began[2], &mut totals.flush_micros),
                    _ => continue,
                };
                match field_u64(value) {
                    Some(0) => {
                        let ran = began
                            .take()
                            .and_then(|began| point.timestamp.duration_since(began).ok());
                        let ran = ran.map_or(0, |ran| u64::try_from(ran.as_micros()).unwrap_or(0));
                        *micros = micros.saturating_add(ran);
                    }
                    Some(_) => *began = began.or(Some(point.timestamp)),
                    None => {}
                }
                continue;
            }
            let level = match (point.name, *name) {
                (ACCOUNTS_INDEX, "count") => Some(&mut levels.index_entries),
                (ACCOUNTS_INDEX, "capacity_in_mem") => Some(&mut levels.index_capacity),
                (ACCOUNTS_INDEX, "estimate_mem_bytes") => Some(&mut levels.index_bytes),
                (ACCOUNTS_WRITE_CACHE, "total_size") => Some(&mut levels.write_cache_bytes),
                (ACCOUNTS_WRITE_CACHE, "num_slots") => Some(&mut levels.write_cache_slots),
                (ACCOUNTS_WRITE_CACHE, "total_accounts_count") => {
                    Some(&mut levels.write_cache_accounts)
                }
                (ACCOUNTS_CLEAN, "max_clean_root") => Some(&mut levels.cleaned_to),
                _ => None,
            };
            if let Some(level) = level {
                set_value(level, value);
                continue;
            }

            let counter = match (point.name, *name) {
                (ACCOUNTS_INDEX, "gets_from_mem") => &mut totals.index_found,
                (ACCOUNTS_INDEX, "gets_missing") => &mut totals.index_missing,
                (ACCOUNTS_INDEX, "inserts") => &mut totals.index_inserts,
                (ACCOUNTS_INDEX, "deletes") => &mut totals.index_deletes,
                (ACCOUNTS_CLEAN, "slots_cleaned") => &mut totals.cleaned_slots,
                (ACCOUNTS_CLEAN, "num_accounts_removed_from_index") => &mut totals.cleaned_accounts,
                (ACCOUNTS_SHRINK | ACCOUNTS_SHRINK_ANCIENT, "num_slots_shrunk") => {
                    &mut totals.shrunk_slots
                }
                (ACCOUNTS_SHRINK | ACCOUNTS_SHRINK_ANCIENT, "accounts_removed") => {
                    &mut totals.shrunk_accounts
                }
                (ACCOUNTS_SHRINK | ACCOUNTS_SHRINK_ANCIENT, "bytes_removed") => {
                    &mut totals.shrunk_bytes
                }
                (ACCOUNTS_SHRINK | ACCOUNTS_SHRINK_ANCIENT, "bytes_written") => {
                    &mut totals.rewritten_bytes
                }
                _ => continue,
            };
            add_value(counter, value);
        }
    }
}

impl AddPoint for MemorySet {
    fn add_point(&mut self, point: &DataPoint) {
        let field = |wanted| number_named(point, wanted);
        match point.name {
            ACCOUNTS_DB_TIMINGS => {
                self.read_cache = field("read_only_accounts_cache_data_size").or(self.read_cache)
            }
            ACCOUNTS_INDEX => {
                self.index_estimate = field("estimate_mem_bytes").or(self.index_estimate)
            }
            ACCOUNTS_WRITE_CACHE => self.write_cache = field("total_size").or(self.write_cache),
            BLOCKSTORE_CFS => {
                let Some(cf) = tag_named(point, CF_NAME) else {
                    return;
                };
                // A property RocksDB could not read comes as -1, which is not a u64.
                let bytes = [
                    "size_all_mem_tables",
                    "block_cache_usage",
                    "estimate_table_readers_mem",
                ]
                .into_iter()
                .filter_map(field)
                .fold(0, u64::saturating_add);
                self.blockstore.insert(cf.to_string(), bytes);
                if let Some(bytes) = field("total_sst_files_size") {
                    self.blockstore_files.insert(cf.to_string(), bytes);
                }
            }
            _ => (),
        }
    }
}

impl AddPoint for QuicSet {
    fn add_point(&mut self, point: &DataPoint) {
        self.totals.add_point(point);
        for (name, value) in &point.fields {
            let level = match *name {
                // Cumulative on the wire, unlike the counters beside it: stored, not added.
                "total_incoming_connection_attempts" => &mut self.totals.offered,
                // `peak_open_staked_connections` is reset as reported, neither a level nor a count.
                "open_connections" => &mut self.levels.open,
                "active_streams" => &mut self.levels.active_streams,
                _ => continue,
            };
            set_value(level, value);
        }
    }
}

impl GossipSet {
    /// Fields are named `<type>-push` and `<type>-pull`, with `all-` for the sum.
    fn add_entries(&mut self, point: &DataPoint) {
        let fails = point.name == GOSSIP_ENTRY_FAILS;
        let totals = &mut self.totals;
        let entries = &mut self.entries;
        for (name, value) in &point.fields {
            let Some((kind, route)) = name.rsplit_once('-') else {
                continue;
            };
            let push = match route {
                "push" => true,
                "pull" => false,
                _ => continue,
            };
            let counter = if kind == "all" {
                match (fails, push) {
                    (false, true) => &mut totals.accepted_push,
                    (false, false) => &mut totals.accepted_pull,
                    (true, true) => &mut totals.rejected_push,
                    (true, false) => &mut totals.rejected_pull,
                }
            } else {
                let Some(index) = GOSSIP_ENTRY_TYPES.iter().position(|known| *known == kind) else {
                    continue;
                };
                match (fails, push) {
                    (false, true) => &mut entries.push[index],
                    (false, false) => &mut entries.pull[index],
                    (true, _) => &mut entries.rejected[index],
                }
            };
            add_value(counter, value);
        }
    }

    fn add_stats(&mut self, point: &DataPoint) {
        if point.name == GOSSIP_STATS[0] {
            self.totals.reports = self.totals.reports.saturating_add(1);
        }
        self.totals.add_point(point);
        let levels = &mut self.levels;
        for (name, value) in &point.fields {
            let level = match *name {
                "table_size" => &mut levels.table_size,
                "num_pubkeys" => &mut levels.pubkeys,
                "num_nodes" => &mut levels.nodes,
                "num_nodes_staked" => &mut levels.staked_nodes,
                _ => continue,
            };
            set_value(level, value);
        }
    }
}

/// A poisoned lock is still read: these hold plain figures, valid whatever panicked.
fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The first field of the point named `wanted`, as written.
fn field_named<'a>(point: &'a DataPoint, wanted: &str) -> Option<&'a str> {
    let (_, value) = point.fields.iter().find(|(name, _)| *name == wanted)?;
    Some(value.as_str())
}

pub(crate) fn number_named(point: &DataPoint, wanted: &str) -> Option<u64> {
    field_named(point, wanted).and_then(field_u64)
}

fn tag_named<'a>(point: &'a DataPoint, wanted: &str) -> Option<&'a str> {
    let (_, value) = point.tags.iter().find(|(name, _)| *name == wanted)?;
    Some(value.as_str())
}

pub(crate) fn keep_newest<K: Ord, V>(map: &mut BTreeMap<K, V>, cap: usize) {
    while map.len() > cap {
        map.pop_first();
    }
}

pub(crate) fn insert_capped<K: Ord, V>(map: &mut BTreeMap<K, V>, key: K, value: V, cap: usize) {
    map.insert(key, value);
    keep_newest(map, cap);
}

pub(crate) fn push_capped<T>(queue: &mut VecDeque<T>, value: T, cap: usize) {
    queue.push_back(value);
    while queue.len() > cap {
        queue.pop_front();
    }
}

fn add_named(counter: &mut u64, point: &DataPoint, wanted: &str) {
    if let Some(value) = field_named(point, wanted) {
        add_value(counter, value);
    }
}

fn add_value(counter: &mut u64, value: &str) {
    if let Some(delta) = field_u64(value) {
        *counter = counter.saturating_add(delta);
    }
}

fn set_value(level: &mut u64, value: &str) {
    if let Some(latest) = field_u64(value) {
        *level = latest;
    }
}

/// The line protocol marks an integer with a trailing `i`; anything else is not a counter.
fn field_u64(value: &str) -> Option<u64> {
    value.strip_suffix('i')?.parse().ok()
}

fn field_str(value: &str) -> String {
    let inner = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value);
    inner.replace("\\\"", "\"")
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::UNIX_EPOCH};

    fn named(name: &'static str, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = DataPoint::new(name);
        for (field, value) in fields {
            point.fields.push((field, (*value).to_string()));
        }
        point
    }

    fn tagged_slot_point(id: &str, slot: u64, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = slot_point(slot, fields);
        point.tags.push((SCHEDULER_ID, id.to_string()));
        point
    }

    fn point(fields: &[(&'static str, &str)]) -> DataPoint {
        named(ACCOUNTS_DB_TIMINGS, fields)
    }

    fn slot_point_named(
        name: &'static str,
        slot: Slot,
        fields: &[(&'static str, &str)],
    ) -> DataPoint {
        let mut point = named(name, fields);
        point.fields.push((SLOT, format!("{slot}i")));
        point
    }

    fn arrival_point(vote: &Pubkey, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = named(VOTE_ARRIVAL, fields);
        point.add_tag(VOTE_ACCOUNT, &vote.to_string());
        point
    }

    #[test]
    fn test_memory_parts_keep_each_latest_size_and_sum_the_column_families() {
        let tap = MetricsTap::default();
        assert_eq!(tap.memory_parts(), MemoryParts::default());

        let column = |cf: &str, memtables: &str| {
            let mut point = named(
                BLOCKSTORE_CFS,
                &[
                    ("size_all_mem_tables", memtables),
                    ("block_cache_usage", "200i"),
                    ("estimate_table_readers_mem", "-1i"),
                ],
            );
            point.add_tag(CF_NAME, cf);
            point
        };
        tap.observe_point(&column("data_shred", "100i"));
        tap.observe_point(&column("meta", "50i"));
        tap.observe_point(&column("data_shred", "300i"));
        tap.observe_point(&named(ACCOUNTS_INDEX, &[("estimate_mem_bytes", "9000i")]));
        tap.observe_point(&named(
            "accounts_index_startup",
            &[("estimate_mem_bytes", "1i")],
        ));
        tap.observe_point(&named(ACCOUNTS_WRITE_CACHE, &[("total_size", "700i")]));
        tap.observe_point(&point(&[("read_only_accounts_cache_data_size", "800i")]));

        assert_eq!(
            tap.memory_parts(),
            MemoryParts {
                index_estimate: Some(9000),
                write_cache: Some(700),
                read_cache: Some(800),
                blockstore: Some(500 + 250),
            }
        );
    }

    #[test]
    fn test_ledger_files_group_the_column_families_and_keep_each_latest_size() {
        let tap = MetricsTap::default();
        assert_eq!(tap.ledger_files(), None);

        let column = |cf: &str, size: &str| {
            let mut point = named(BLOCKSTORE_CFS, &[("total_sst_files_size", size)]);
            point.add_tag(CF_NAME, cf);
            point
        };
        tap.observe_point(&column("data_shred", "100i"));
        tap.observe_point(&column("data_shred", "300i"));
        tap.observe_point(&column("code_shred", "50i"));
        tap.observe_point(&column("alt_data_shred", "5i"));
        tap.observe_point(&column("transaction_status", "20i"));
        tap.observe_point(&column("address_signatures", "7i"));
        tap.observe_point(&column("meta", "9i"));
        tap.observe_point(&column("root", "1i"));
        // A property RocksDB could not read leaves the last size held.
        tap.observe_point(&column("meta", "-1i"));

        assert_eq!(
            tap.ledger_files(),
            Some(LedgerFiles {
                shreds: 355,
                history: 27,
                metadata: 10,
            })
        );
    }

    #[test]
    fn test_accounts_files_split_the_storages_into_live_and_fragmented() {
        let tap = MetricsTap::default();
        assert_eq!(tap.accounts_files(), None);

        tap.observe_point(&named(
            ACCOUNTS_STORES,
            &[("total_bytes", "104i"), ("total_alive_bytes", "100i")],
        ));

        assert_eq!(
            tap.accounts_files(),
            Some(AccountsFiles {
                live: 100,
                fragmented: 4,
            })
        );
    }

    #[test]
    fn test_a_leader_slots_timing_is_merged_from_its_reports() {
        let tap = MetricsTap::default();
        let slot = 4_000;
        tap.observe_point(&slot_point_named(
            LEADER_START,
            slot,
            &[
                ("slot_delay_us", "46100i"),
                ("replay_is_behind_us", "1200i"),
                ("leader_handover_fast", "1i"),
            ],
        ));
        tap.observe_point(&slot_point_named(LEADER_BANK, slot, &[("elapsed", "400i")]));
        tap.observe_point(&slot_point_named(
            BROADCAST_TRANSMIT,
            slot,
            &[("end_to_end_elapsed", "411000i")],
        ));
        tap.observe_point(&slot_point_named(
            BROADCAST_PROCESS,
            slot,
            &[("shredding_time", "9800i")],
        ));
        tap.observe_point(&slot_point_named(
            SCHEDULER_SLOT_TIMING,
            slot,
            &[
                ("receive_time_us", "142000i"),
                ("schedule_time_us", "31000i"),
            ],
        ));

        assert_eq!(
            tap.leader_timing(slot),
            Some(LeaderTiming {
                start_micros: Some(46_100),
                parent_wait_micros: Some(1_200),
                open_millis: Some(400),
                broadcast_micros: Some(411_000),
                shredding_micros: Some(9_800),
                scheduler_receive_micros: Some(142_000),
                scheduler_schedule_micros: Some(31_000),
                fast_handover: true,
                parent_switched: false,
            })
        );
        assert_eq!(tap.leader_timing(slot.saturating_add(1)), None);
    }

    #[test]
    fn test_from_44_the_start_and_the_wait_for_the_parent_come_apart() {
        let tap = MetricsTap::default();
        tap.observe_point(&slot_point_named(
            LEADER_START,
            12,
            &[
                ("slot_delay_us", "46100i"),
                ("replay_is_behind_us", "41800i"),
                ("leader_handover_sad", "1i"),
            ],
        ));
        let timing = tap.leader_timing(12).unwrap();
        assert_eq!(timing.start_micros, Some(46_100));
        assert_eq!(timing.parent_wait_micros, Some(41_800));
        assert!(timing.parent_switched);

        // A start that took no measurable time is still a reading.
        tap.observe_point(&slot_point_named(
            LEADER_START,
            13,
            &[("slot_delay_us", "0i"), ("replay_is_behind_us", "0i")],
        ));
        assert_eq!(tap.leader_timing(13).unwrap().start_micros, Some(0));
    }

    #[test]
    fn test_another_schedulers_slot_timing_is_not_ours() {
        let tap = MetricsTap::default();
        let mut bam = slot_point_named(SCHEDULER_SLOT_TIMING, 9, &[("receive_time_us", "5i")]);
        bam.tags.push((SCHEDULER_ID, "10000".to_string()));
        tap.observe_point(&bam);
        assert_eq!(tap.leader_timing(9), None);
    }

    #[test]
    fn test_vote_arrivals_keep_the_newest_epoch_only() {
        let tap = MetricsTap::default();
        let (early, late) = (Pubkey::new_unique(), Pubkey::new_unique());
        let full = [
            ("epoch", "7i"),
            ("notar_vote_count", "400i"),
            ("notar_vote_us_mean", "118000i"),
            ("notar_vote_us_stddev", "30000i"),
            ("notar_vote_us_maximum", "470000i"),
            ("finalize_vote_us_mean", "166000i"),
        ];
        tap.observe_point(&arrival_point(&early, &full));
        // Votor sends no mean for a validator that never voted that way.
        tap.observe_point(&arrival_point(
            &Pubkey::new_unique(),
            &[("epoch", "7i"), ("notar_vote_count", "0i")],
        ));
        let (epoch, held) = tap.vote_arrivals();
        assert_eq!(epoch, Some(7));
        assert_eq!(held.len(), 1);
        assert_eq!(
            held.get(&early),
            Some(&VoteArrival {
                notarize_mean_micros: 118_000,
                notarize_spread_micros: Some(30_000),
                notarize_worst_micros: Some(470_000),
                notarize_count: Some(400),
                finalize_mean_micros: Some(166_000),
            })
        );

        tap.observe_point(&arrival_point(
            &late,
            &[("epoch", "8i"), ("notar_vote_us_mean", "92000i")],
        ));
        tap.observe_point(&arrival_point(
            &early,
            &[("epoch", "7i"), ("notar_vote_us_mean", "1i")],
        ));
        let (epoch, held) = tap.vote_arrivals();
        assert_eq!(epoch, Some(8), "a later epoch replaces the earlier one");
        assert_eq!(held.keys().collect::<Vec<_>>(), vec![&late]);
        assert_eq!(held[&late].finalize_mean_micros, None);
    }

    #[test]
    fn test_a_vote_arrival_without_a_readable_account_is_dropped() {
        let tap = MetricsTap::default();
        let mut point = named(
            VOTE_ARRIVAL,
            &[("epoch", "7i"), ("notar_vote_us_mean", "5i")],
        );
        point.add_tag(VOTE_ACCOUNT, "not a key");
        tap.observe_point(&point);
        assert_eq!(tap.vote_arrivals(), (None, HashMap::new()));
    }

    #[test]
    fn test_integer_fields_carry_the_line_protocol_suffix() {
        // What `add_field_i64` writes, which is not what a reader expects.
        assert_eq!(field_u64("42i"), Some(42));
        assert_eq!(field_u64("0i"), Some(0));
    }

    #[test]
    fn test_anything_that_is_not_an_integer_field_is_left_alone() {
        assert_eq!(field_u64("42"), None);
        assert_eq!(field_u64("1.5"), None);
        assert_eq!(field_u64("true"), None);
        assert_eq!(field_u64("\"words\"i"), None);
    }

    #[test]
    fn test_the_totals_accumulate_across_points() {
        // Each point carries what happened since the last, so the totals are the sum.
        let tap = MetricsTap::default();
        tap.observe_point(&point(&[
            ("read_only_accounts_cache_hits", "10i"),
            ("read_only_accounts_cache_misses", "2i"),
            ("read_only_accounts_cache_evicts", "1i"),
        ]));
        tap.observe_point(&point(&[
            ("read_only_accounts_cache_hits", "5i"),
            ("read_only_accounts_cache_misses", "1i"),
        ]));

        let mut expected = TapCounters::default();
        expected.accounts.totals = AccountsTotals {
            cache_hits: 15,
            cache_misses: 3,
            cache_evicts: 1,
            ..AccountsTotals::default()
        };
        assert_eq!(tap.counters(), expected);
    }

    #[test]
    fn test_the_retransmit_point_gives_the_xdp_drops_and_the_path() {
        let tap = MetricsTap::default();
        let mut point = named(RETRANSMIT_STAGE, &[("num_shreds_dropped_xdp_full", "12i")]);
        point.tags.push((IS_XDP, "true".to_string()));
        tap.observe_point(&point);
        tap.observe_point(&point);

        let counters = tap.counters();
        assert_eq!(counters.network.xdp_dropped, 24);
        assert_eq!(counters.retransmit_xdp, Some(true));
        assert_eq!(MetricsTap::default().counters().retransmit_xdp, None);
    }

    #[test]
    fn test_a_slot_stats_point_adds_each_layer_into_its_own_count() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            RETRANSMIT_SLOT_STATS,
            &[
                ("slot", "100i"),
                ("num_shreds_received_root", "3i"),
                ("num_shreds_received_1st_layer", "400i"),
                ("num_shreds_received_2nd_layer", "1100i"),
                ("num_shreds_received_3rd_layer", "0i"),
            ],
        ));
        let counters = tap.counters();
        assert_eq!(counters.network.turbine_root, 3);
        assert_eq!(counters.network.turbine_layer_1, 400);
        assert_eq!(counters.network.turbine_layer_2, 1100);
        assert_eq!(counters.network.turbine_layer_3, 0);
    }

    #[test]
    fn test_other_points_are_ignored() {
        let tap = MetricsTap::default();
        let mut other = DataPoint::new("banking_stage-loop-stats");
        other
            .fields
            .push(("read_only_accounts_cache_hits", "99i".to_string()));
        tap.observe_point(&other);
        // Votes never reach the scheduler, so counting them at the top would inflate a total the
        // stages below cannot account for.
        tap.observe_point(&named("tpu-vote-verifier", &[("total_packets", "5000i")]));
        assert_eq!(tap.counters(), TapCounters::default());
    }

    #[test]
    fn test_each_socket_receiver_counts_into_its_own_port() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            SHREDS_TURBINE,
            &[
                ("packet_batches_count", "7i"),
                ("packets_count", "900i"),
                ("channel_len", "3i"),
            ],
        ));
        tap.observe_point(&named(GOSSIP_RECEIVER, &[("packets_count", "40i")]));
        tap.observe_point(&named(TPU_VOTE_RECEIVER, &[("packets_count", "70i")]));
        tap.observe_point(&named(GOSSIP_RECEIVER, &[("packets_count", "2i")]));
        tap.observe_point(&named(SHREDS_REPAIR, &[("packets_count", "12i")]));

        let counters = tap.counters();
        assert_eq!(counters.network.shreds_turbine, 900);
        assert_eq!(counters.network.shreds_repair, 12);
        assert_eq!(counters.network.packets_gossip, 42);
        assert_eq!(counters.network.packets_tpu_vote, 70);
    }

    #[test]
    fn test_a_block_height_counts_the_blocks_past_the_newest_bank_reported() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            NEW_BANK,
            &[
                ("slot", "100i"),
                ("block_height", "90i"),
                ("parent_slot", "98i"),
            ],
        ));
        let parents = BTreeMap::from([(104, 103), (103, 100), (102, 101)]);
        let parent = |slot| parents.get(&slot).copied();
        assert_eq!(tap.block_height(100, parent), Some(90));
        assert_eq!(tap.block_height(104, parent), Some(92));
        // Off the reported chain.
        assert_eq!(tap.block_height(102, parent), None);
    }

    #[test]
    fn test_a_replay_record_says_when_it_arrived() {
        let tap = MetricsTap::default();
        let before = timestamp();
        tap.observe_point(&named(
            REPLAY_SLOT_STATS,
            &[("slot", "443895975i"), ("execute_us", "231000i")],
        ));
        let observed = tap.replayed(443_895_975).unwrap().observed_millis;
        assert!(observed >= before && observed <= timestamp());
        assert!(tap.replayed(443_895_974).is_none());
    }

    #[test]
    fn test_filled_slot_is_read_as_counts() {
        // Names from `ledger/src/slot_stats.rs`. The last index is the highest
        // shred, so the count is one more.
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            SHRED_FULL,
            &[
                ("slot", "444422652i"),
                ("total_time_ms", "921i"),
                ("last_index", "927i"),
                ("num_repaired", "63i"),
                ("num_recovered", "12i"),
            ],
        ));

        let fill = tap.shred_fill(444_422_652).unwrap();
        assert_eq!(fill.shreds, 928);
        assert_eq!(fill.repaired, 63);
        assert_eq!(fill.recovered, 12);
        assert_eq!(fill.full_millis, 921);
        assert!(tap.shred_fill(444_422_651).is_none());
        assert_eq!(tap.newest_fill(), Some(444_422_652));
    }

    #[test]
    fn test_a_fill_gives_back_the_first_shred_stamp_it_was_counted_from() {
        let tap = MetricsTap::default();
        let mut point = named(
            SHRED_FULL,
            &[
                ("slot", "444422652i"),
                ("total_time_ms", "921i"),
                ("last_index", "927i"),
            ],
        );
        point.timestamp = UNIX_EPOCH + std::time::Duration::from_nanos(1_794_392_646_486_400_000);
        tap.observe_external(&point);
        let fill = tap.shred_fill(444_422_652).unwrap();
        assert_eq!(fill.first_shred_millis(), Some(1_794_392_646_486 - 921));
        assert_eq!(ShredFill::default().first_shred_millis(), None);
    }

    #[test]
    fn test_a_fill_with_no_last_index_is_dropped() {
        // The blockstore writes -1 where it has not seen the last shred, which
        // is not a count and must not become one.
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            SHRED_FULL,
            &[
                ("slot", "10i"),
                ("total_time_ms", "400i"),
                ("last_index", "-1i"),
            ],
        ));
        assert!(tap.shred_fill(10).is_none());
    }

    #[test]
    fn test_only_the_newest_filled_slots_are_kept() {
        let tap = MetricsTap::default();
        for slot in 0..SHRED_FILLS.saturating_add(10) {
            tap.observe_point(&named(
                SHRED_FULL,
                &[("slot", &format!("{slot}i")), ("last_index", "99i")],
            ));
        }
        assert!(tap.shred_fill(9).is_none(), "the first ten dropped");
        assert!(tap.shred_fill(10).is_some());
        assert!(
            tap.shred_fill((SHRED_FILLS as u64).saturating_add(9))
                .is_some()
        );
    }

    #[test]
    fn test_a_leader_slots_bundles_are_summed_across_the_stage_threads() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            BUNDLE_SLOT_STATS,
            &[
                ("id", "0i"),
                ("slot", "445527719i"),
                ("num_sanitized_ok", "10i"),
                ("execution_results_ok", "8i"),
            ],
        ));
        tap.observe_point(&named(
            BUNDLE_SLOT_STATS,
            &[
                ("id", "1i"),
                ("slot", "445527719i"),
                ("num_sanitized_ok", "7i"),
                ("execution_results_ok", "6i"),
            ],
        ));

        let landed = tap.bundles_landed(445_527_719).unwrap();
        assert_eq!((landed.sanitized, landed.executed), (17, 14));
        assert!(tap.bundles_landed(445_527_718).is_none());
    }

    #[test]
    fn test_a_bundle_point_without_a_slot_is_dropped() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(BUNDLE_SLOT_STATS, &[("num_sanitized_ok", "3i")]));
        assert!(tap.bundle_slots.lock().unwrap().is_empty());
    }

    #[test]
    fn test_each_named_sender_keeps_its_bytes_and_its_window_apart() {
        let tap = MetricsTap::default();
        fn sent<'a>(bytes: &'a str, millis: &'a str) -> [(&'static str, &'a str); 3] {
            [
                (SENT_BYTES, bytes),
                (SENT_MILLIS, millis),
                ("streamer-send-host_count", "1000i"),
            ]
        }
        tap.observe_point(&named(GOSSIP_SENDER, &sent("22786715i", "5503i")));
        tap.observe_point(&named(REPAIR_SENDER, &sent("2138992i", "10004i")));
        tap.observe_point(&named(GOSSIP_SENDER, &sent("21012098i", "5383i")));

        let counters = tap.counters();
        assert_eq!(counters.network.gossip_sent_bytes, 43_798_813);
        assert_eq!(counters.network.gossip_sent_millis, 10_886);
        assert_eq!(counters.network.repair_sent_bytes, 2_138_992);
        assert_eq!(counters.network.repair_sent_millis, 10_004);
    }

    #[test]
    fn test_the_waterfall_counters_land_where_they_belong() {
        // Several of these names differ only in their tail; a transposed pair would
        // put the wrong reason under a label.
        let tap = MetricsTap::default();
        tap.observe_point(&slot_point(
            7,
            &[
                ("num_received", "1000i"),
                ("num_dropped_on_receive", "900i"),
                ("num_dropped_on_check_work_queue_full", "1i"),
                ("num_dropped_on_parsing_and_sanitization", "2i"),
                ("num_dropped_on_validate_locks", "3i"),
                ("num_dropped_on_receive_compute_budget", "4i"),
                ("num_dropped_on_receive_age", "5i"),
                ("num_dropped_on_receive_already_processed", "6i"),
                ("num_dropped_on_receive_fee_payer", "7i"),
                ("num_dropped_on_filter_key", "8i"),
                ("num_dropped_on_nonce_dedup", "9i"),
                ("num_buffered", "55i"),
                ("num_dropped_on_capacity", "10i"),
                ("num_evicted_on_nonce_dedup", "11i"),
                ("num_dropped_on_clear", "12i"),
                ("num_dropped_on_clean", "13i"),
                ("num_scheduled", "40i"),
                ("num_unschedulable_conflicts", "14i"),
                ("num_unschedulable_threads", "15i"),
                ("num_finished", "38i"),
                ("num_retryable", "16i"),
                ("min_priority", "5i"),
                ("max_priority", "900000i"),
            ],
        ));

        let counters = tap.slot_waterfalls()[0].counts;
        assert_eq!(counters.received, 1_000);
        assert_eq!(counters.not_held, 900);
        assert_eq!(counters.check_queue_full, 1);
        assert_eq!(counters.unparsable, 2);
        assert_eq!(counters.bad_locks, 3);
        assert_eq!(counters.compute_budget, 4);
        assert_eq!(counters.too_old, 5);
        assert_eq!(counters.already_processed, 6);
        assert_eq!(counters.fee_payer, 7);
        assert_eq!(counters.filtered, 8);
        assert_eq!(counters.nonce_conflict, 9);
        assert_eq!(counters.buffered, 55);
        assert_eq!(counters.queue_full, 10);
        assert_eq!(counters.nonce_evicted, 11);
        assert_eq!(counters.cleared, 12);
        assert_eq!(counters.cleaned, 13);
        assert_eq!(counters.scheduled, 40);
        assert_eq!(counters.blocked_conflicts, 14);
        assert_eq!(counters.blocked_threads, 15);
        assert_eq!(counters.finished, 38);
        assert_eq!(counters.retried, 16);
    }

    #[test]
    fn test_a_window_of_readings_differences_and_sums() {
        let first = SchedulerTotals {
            received: 100,
            buffered: 10,
            ..SchedulerTotals::default()
        };
        let second = SchedulerTotals {
            received: 250,
            buffered: 25,
            ..SchedulerTotals::default()
        };

        let step = second.since(&first);
        assert_eq!(step.received, 150);
        assert_eq!(step.buffered, 15);

        assert_eq!(step.plus(&step).received, 300);
        // Backwards, which only happens if a counter was reset under us.
        assert_eq!(first.since(&second).received, 0);
    }

    fn slot_point(slot: u64, fields: &[(&'static str, &str)]) -> DataPoint {
        let slot = format!("{slot}i");
        let mut all = vec![("slot", slot.as_str())];
        all.extend_from_slice(fields);
        named(SCHEDULER_SLOT_COUNTS, &all)
    }

    fn worker_point(id: &str, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = named(WORKER_TIMING, fields);
        point.tags.push((WORKER_ID, id.to_string()));
        point
    }

    #[test]
    fn test_worker_reports_are_summed_inside_the_window_only() {
        let tap = MetricsTap::default();
        let fields = [
            ("load_execute_us", "100i"),
            ("load_execute_us_max", "40i"),
            ("record_us", "10i"),
            ("commit_us", "5i"),
        ];
        tap.remember_worker_timing(&worker_point("0", &fields), 999);
        tap.remember_worker_timing(&worker_point("0", &fields), 1_000);
        tap.remember_worker_timing(&worker_point("1", &[("load_execute_us_max", "70i")]), 1_300);
        tap.remember_worker_timing(&worker_point("0", &fields), 1_400);
        let sum = tap.worker_time(1_000, 1_400).unwrap();
        assert_eq!(sum.workers, 2);
        assert_eq!(sum.times.load_execute, 100);
        assert_eq!(sum.times.record, 10);
        assert_eq!(sum.times.commit, 5);
        assert_eq!(sum.longest_batch, 70);
        assert_eq!(
            tap.worker_time(1_400, 1_500).unwrap().times.load_execute,
            100,
            "a report on the boundary goes to the window it opens"
        );
        assert_eq!(
            tap.worker_time(2_000, 3_000),
            None,
            "no report in the window"
        );
    }

    #[test]
    fn test_the_vote_worker_report_is_kept_by_slot() {
        let tap = MetricsTap::default();
        let mut point = named(
            VOTE_SLOT_TIMING,
            &[("load_execute_us", "9100i"), ("record_us", "3900i")],
        );
        point.fields.push(("slot", "77i".to_string()));
        tap.observe_point(&point);
        let times = tap.vote_time(77).unwrap();
        assert_eq!(times.load_execute, 9_100);
        assert_eq!(times.record, 3_900);
        assert_eq!(
            times.cost_model, 0,
            "the vote worker has no cost model field"
        );
        assert_eq!(tap.vote_time(78), None);
    }

    fn cost_point(is_leader: bool, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = named(COST_TRACKER, fields);
        point.tags.push((IS_LEADER, is_leader.to_string()));
        point
    }

    #[test]
    fn test_the_slot_lists_revision_moves_only_when_a_list_does() {
        let tap = MetricsTap::default();
        let start = tap.slot_lists_revision();
        tap.observe_point(&slot_point(430_789_128, &[("num_received", "500i")]));
        let after_waterfall = tap.slot_lists_revision();
        assert_ne!(after_waterfall, start);
        tap.observe_point(&slot_point(430_789_128, &[("num_received", "5i")]));
        assert_eq!(tap.slot_lists_revision(), after_waterfall);
        tap.observe_point(&cost_point(true, &[("bank_slot", "430789128i")]));
        assert_ne!(tap.slot_lists_revision(), after_waterfall);
    }

    #[test]
    fn test_a_block_this_validator_produced_is_kept() {
        let tap = MetricsTap::default();
        tap.observe_point(&cost_point(
            true,
            &[
                ("bank_slot", "441034909i"),
                ("block_cost", "42574937i"),
                (
                    "costliest_account",
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                ),
                ("costliest_account_cost", "11842006i"),
                ("number_of_accounts", "3847i"),
                ("number_of_contended_accounts", "412i"),
                ("allocated_accounts_data_size", "421888i"),
                ("inflight_transaction_count", "0i"),
            ],
        ));

        let held = tap.slot_costs();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].slot, 441_034_909);
        assert_eq!(
            held[0].costliest_account,
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        );
        assert_eq!(held[0].costliest_cost, 11_842_006);
        assert_eq!(held[0].contended, 412);
    }

    fn shreds_at(tap: &MetricsTap, millis: u64, packets: &str) {
        let mut point = DataPoint::new(SHREDS_TURBINE);
        point.timestamp = UNIX_EPOCH
            .checked_add(std::time::Duration::from_millis(millis))
            .unwrap();
        point.fields.push((PACKETS_COUNT, format!("{packets}i")));
        tap.observe_external(&point);
    }

    #[test]
    fn test_counters_are_read_as_they_stood_at_a_moment_of_point_time() {
        let tap = MetricsTap::default();
        assert_eq!(tap.points_through(), None);
        shreds_at(&tap, 10_050, "5");
        shreds_at(&tap, 10_150, "7");
        // Nothing made after the moment has been read yet.
        assert_eq!(tap.counters_at(10_100), None);
        shreds_at(&tap, 10_450, "11");
        shreds_at(&tap, 12_000, "13");
        assert_eq!(tap.points_through(), Some(12_000));
        // Up to the step after the moment: both points of its step.
        assert_eq!(tap.counters_at(10_100).unwrap().network.shreds_turbine, 12);
        assert_eq!(tap.counters_at(10_500).unwrap().network.shreds_turbine, 23);
        assert_eq!(tap.counters().network.shreds_turbine, 36);
    }

    #[test]
    fn test_point_time_set_back_starts_the_steps_over() {
        let tap = MetricsTap::default();
        const DAY_MILLIS: u64 = 86_400_000;
        let before = 70 * DAY_MILLIS;
        shreds_at(&tap, before, "5");
        shreds_at(&tap, before + 500, "7");
        // The validator's clock is set back 66 days.
        let after = before - 66 * DAY_MILLIS;
        shreds_at(&tap, after + 50, "11");
        shreds_at(&tap, after + 450, "13");
        assert_eq!(tap.points_through(), Some(after + 450));
        assert_eq!(
            tap.counters_at(after + 100).unwrap().network.shreds_turbine,
            23
        );
        assert_eq!(tap.counters_at(after + 450), None);
    }

    #[test]
    fn test_every_banks_figures_are_kept_beside_our_costs() {
        let tap = MetricsTap::default();
        let figures = [
            ("block_cost", "1874211i"),
            ("transaction_count", "1291i"),
            ("total_transaction_fee", "8216330i"),
            ("total_priority_fee", "1756330i"),
        ];
        let mut other = vec![("bank_slot", "441034910i")];
        other.extend_from_slice(&figures);
        tap.observe_point(&cost_point(false, &other));
        let mut ours = vec![("bank_slot", "441034911i")];
        ours.extend_from_slice(&figures);
        tap.observe_point(&cost_point(true, &ours));

        let expected = BankFigures {
            block_cost: 1_874_211,
            transactions: 1_291,
            total_fees: 8_216_330,
            priority_fees: 1_756_330,
        };
        assert_eq!(tap.bank_figures(441_034_910), Some(expected));
        assert_eq!(tap.bank_figures(441_034_911), Some(expected));
        assert_eq!(tap.bank_figures(441_034_912), None);
        // Only our own block is a slot cost.
        let held = tap.slot_costs();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].slot, 441_034_911);
    }

    #[test]
    fn test_a_cost_point_with_no_slot_is_dropped() {
        let tap = MetricsTap::default();
        tap.observe_point(&cost_point(true, &[("block_cost", "42574937i")]));
        assert!(tap.slot_costs().is_empty());
    }

    #[test]
    fn test_the_leader_tag_is_read_as_a_tag_not_a_field() {
        let tap = MetricsTap::default();
        let mut point = named(COST_TRACKER, &[("bank_slot", "441034909i")]);
        point.fields.push((IS_LEADER, "true".to_string()));
        tap.observe_point(&point);
        assert!(
            tap.slot_costs().is_empty(),
            "the validator does not send it as a field"
        );
    }

    fn replay_point(fields: &[(&'static str, &str)]) -> DataPoint {
        named(REPLAY_SLOT_STATS, fields)
    }

    #[test]
    fn test_votor_timelines_are_kept_by_slot_until_taken() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "6726788i"),
                ("first_shred", "1200i"),
                ("vote_notarize", "413200i"),
            ],
        ));
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "6726789i"),
                ("parent_ready", "90i"),
                ("vote_notarize", "150i"),
            ],
        ));
        tap.observe_point(&named(VOTE_TRACKING, &[("first_shred", "5i")]));

        let tracks = tap.take_vote_tracks();
        assert_eq!(tracks.len(), 2, "the point without a slot is dropped");
        assert_eq!(
            tracks[0],
            (
                6_726_788,
                VoteSent {
                    first_shred_us: Some(1_200),
                    parent_ready_us: None,
                    notarize_us: Some(413_200),
                    skip_us: None,
                }
            )
        );
        assert_eq!(tracks[1].1.parent_ready_us, Some(90));
        assert!(tap.take_vote_tracks().is_empty(), "taken once");
    }

    /// Full at unix second 1,000, so close times can be set relative to it.
    const FULL_AT_NANOS: u64 = 1_000_000_000_000;

    fn full_block_at(tap: &MetricsTap, slot: &str, total_time_ms: &str) {
        let mut point = named(
            SHRED_FULL,
            &[
                ("slot", slot),
                ("total_time_ms", total_time_ms),
                ("last_index", "40i"),
            ],
        );
        point.timestamp = UNIX_EPOCH
            .checked_add(std::time::Duration::from_nanos(FULL_AT_NANOS))
            .unwrap();
        tap.observe_point(&point);
    }

    fn finalized_89_ms_after_the_last_shred(tap: &MetricsTap) {
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "100i"),
                ("first_shred", "1000i"),
                ("finalized", "270000i"),
            ],
        ));
    }

    #[test]
    fn test_from_the_leader_adds_the_time_since_it_closed_the_block() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "100i", "180i");
        tap.note_block_closed(100, FULL_AT_NANOS.saturating_sub(40_000_000));
        finalized_89_ms_after_the_last_shred(&tap);
        assert_eq!(
            tap.finality_from_leader_micros(timestamp(), 60_000),
            Some(129_000)
        );
    }

    #[test]
    fn test_from_the_leader_waits_for_the_close_time_when_votor_reports_first() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "100i", "180i");
        finalized_89_ms_after_the_last_shred(&tap);
        assert_eq!(tap.finality_from_leader_micros(timestamp(), 60_000), None);
        tap.note_block_closed(100, FULL_AT_NANOS.saturating_sub(40_000_000));
        assert_eq!(
            tap.finality_from_leader_micros(timestamp(), 60_000),
            Some(129_000)
        );
    }

    #[test]
    fn test_a_leader_clock_ahead_of_ours_is_left_out() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "100i", "180i");
        tap.note_block_closed(100, FULL_AT_NANOS.saturating_add(1));
        finalized_89_ms_after_the_last_shred(&tap);
        assert_eq!(tap.finality_from_leader_micros(timestamp(), 60_000), None);
        assert_eq!(
            tap.take_block_votes()
                .first()
                .and_then(|votes| votes.finalized_us),
            Some(89_000)
        );
    }

    #[test]
    fn test_finality_and_our_vote_are_timed_from_the_last_shred() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "100i", "180i");
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "100i"),
                ("first_shred", "1000i"),
                ("vote_notarize", "230000i"),
                ("finalized", "270000i"),
            ],
        ));

        assert_eq!(
            tap.take_block_votes(),
            vec![BlockVotes {
                slot: 100,
                vote_us: Some(49_000),
                finalized_us: Some(89_000),
            }]
        );
        assert_eq!(tap.vote_lag_micros(timestamp(), 60_000), Some(49_000));
        assert_eq!(
            tap.vote_lag_micros(timestamp().saturating_add(120_000), 60_000),
            None,
            "outside the window"
        );
    }

    #[test]
    fn test_our_vote_is_timed_from_the_last_shred() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "100i", "180i");
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "100i"),
                ("first_shred", "1000i"),
                ("vote_notarize", "193000i"),
            ],
        ));

        assert_eq!(tap.vote_lag_micros(timestamp(), 60_000), Some(12_000));
        assert_eq!(
            tap.take_block_votes(),
            vec![BlockVotes {
                slot: 100,
                vote_us: Some(12_000),
                finalized_us: None,
            }]
        );
        assert!(tap.take_block_votes().is_empty(), "taken once");
    }

    #[test]
    fn test_finalized_paths_are_taken_once() {
        let tap = MetricsTap::default();
        for (slot, fast) in [("100i", "true"), ("101i", "false")] {
            tap.observe_point(&named(
                VOTE_TRACKING,
                &[
                    ("slot", slot),
                    ("finalized", "180000i"),
                    ("is_fast_finalization", fast),
                ],
            ));
        }
        // Not finalized yet, so no path.
        tap.observe_point(&named(VOTE_TRACKING, &[("slot", "102i")]));

        assert_eq!(tap.take_finalized_paths(), vec![(100, true), (101, false)]);
        assert!(tap.take_finalized_paths().is_empty(), "taken once");
    }

    #[test]
    fn test_finality_needs_the_first_shred_and_the_full_block() {
        let tap = MetricsTap::default();
        full_block_at(&tap, "101i", "100i");
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "101i"),
                ("vote_notarize", "5i"),
                ("finalized", "45000i"),
            ],
        ));
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "102i"),
                ("first_shred", "1000i"),
                ("finalized", "270000i"),
            ],
        ));
        full_block_at(&tap, "103i", "300i");
        tap.observe_point(&named(
            VOTE_TRACKING,
            &[
                ("slot", "103i"),
                ("first_shred", "1000i"),
                ("finalized", "200000i"),
            ],
        ));

        assert!(tap.take_block_votes().is_empty());
    }

    #[test]
    fn test_time_to_our_vote_is_the_median_sample() {
        let tap = MetricsTap::default();
        for (slot, voted) in [("104i", "30000i"), ("108i", "10000i"), ("112i", "20000i")] {
            full_block_at(&tap, slot, "0i");
            tap.observe_point(&named(
                VOTE_TRACKING,
                &[
                    ("slot", slot),
                    ("first_shred", "0i"),
                    ("vote_notarize", voted),
                ],
            ));
        }

        assert_eq!(tap.vote_lag_micros(timestamp(), 60_000), Some(20_000));
    }

    #[test]
    fn test_a_replayed_slot_is_read_field_by_field() {
        let tap = MetricsTap::default();
        tap.observe_point(&replay_point(&[
            ("fetch_entries_time", "2034i"),
            ("confirmation_without_replay_us", "17288i"),
            ("bank_complete_time_us", "443i"),
            ("entry_poh_verification_time", "28601i"),
            ("entry_transaction_verification_time", "13644i"),
            ("task_submission_us", "9828i"),
            ("execute_us", "176771i"),
            ("load_us", "26210i"),
            ("store_us", "10150i"),
            ("program_cache_us", "22954i"),
            ("total_transactions", "1232i"),
        ]));

        let held = tap.replay_slots();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].confirming, 17_288);
        assert_eq!(held[0].execute, 176_771);
        assert_eq!(held[0].transactions, 1_232);
        assert_eq!(held[0].serial(), 2_034 + 17_288 + 443);
        assert_eq!(held[0].load, 26_210);
        assert_eq!(held[0].store, 10_150);
        assert_eq!(held[0].program_cache, 22_954);
    }

    #[test]
    fn test_an_alpenglow_slot_reports_the_same_timings_under_its_other_names() {
        let tap = MetricsTap::default();
        tap.observe_point(&replay_point(&[
            ("fetch_entries_time", "2034i"),
            ("confirmation_time_us", "17288i"),
            ("bank_complete_time_us", "443i"),
            ("replay_time", "9828i"),
            ("execute_batches_us", "50000i"),
        ]));
        let held = tap.replay_slots();
        assert_eq!(held[0].confirming, 17_288);
        assert_eq!(held[0].dispatch, 9_828);
        assert_eq!(held[0].serial(), 2_034 + 17_288 + 443);
        assert_eq!(
            held[0].execute, 0,
            "the batch wall clock is not thread time"
        );
    }

    #[test]
    fn test_the_status_field_carries_no_micros_suffix() {
        // Every figure around it ends `_us` and this one does not.
        let tap = MetricsTap::default();
        tap.observe_point(&replay_point(&[("update_transaction_statuses", "1212i")]));
        assert_eq!(tap.replay_slots()[0].other, 1_212);
    }

    #[test]
    fn test_the_costs_of_a_program_cache_miss_are_summed() {
        let tap = MetricsTap::default();
        tap.observe_point(&replay_point(&[
            ("execute_details_create_executor_load_elf_us", "12506i"),
            ("execute_details_create_executor_verify_code_us", "1701i"),
            ("execute_details_create_executor_jit_compile_us", "7486i"),
        ]));
        assert_eq!(tap.replay_slots()[0].compiling, 12_506 + 1_701 + 7_486);
    }

    #[test]
    fn test_a_point_naming_nothing_this_reads_is_dropped() {
        // A point this does not understand would drag every mean down.
        let tap = MetricsTap::default();
        tap.observe_point(&replay_point(&[("some_field_from_a_later_release", "9i")]));
        assert!(tap.replay_slots().is_empty());
    }

    #[test]
    fn test_only_the_newest_replayed_slots_are_kept() {
        let tap = MetricsTap::default();
        for slot in 0..REPLAY_SLOTS.saturating_add(10) {
            tap.observe_point(&replay_point(&[
                ("slot", &format!("{slot}i")),
                ("execute_us", &format!("{slot}i")),
            ]));
        }

        let held = tap.replay_slots();
        assert_eq!(held.len(), REPLAY_SLOTS);
        assert_eq!(held[0].execute, 10, "lowest first, the first ten dropped");
    }

    #[test]
    fn test_a_leader_slot_is_kept_whole_rather_than_accumulated() {
        let tap = MetricsTap::default();
        tap.observe_point(&slot_point(
            430_789_128,
            &[("num_received", "500i"), ("num_buffered", "80i")],
        ));
        tap.observe_point(&slot_point(
            430_789_129,
            &[("num_received", "600i"), ("num_buffered", "90i")],
        ));

        let held = tap.slot_waterfalls();
        assert_eq!(held.len(), 2);
        assert_eq!(held[0].slot, 430_789_128);
        assert_eq!(held[0].counts.received, 500);
        assert_eq!(held[1].counts.buffered, 90);
    }

    #[test]
    fn test_slot_is_read_as_an_integer_field() {
        // `add_field_i64` writes "430789128i"; a change upstream fails here rather than emptying
        // the panel.
        let tap = MetricsTap::default();
        let mut bare = DataPoint::new(SCHEDULER_SLOT_COUNTS);
        bare.fields.push(("slot", "430789128".to_string()));
        bare.fields.push(("num_received", "500i".to_string()));
        tap.observe_point(&bare);
        assert!(tap.slot_waterfalls().is_empty());

        tap.observe_point(&slot_point(430_789_128, &[("num_received", "500i")]));
        assert_eq!(tap.slot_waterfalls().len(), 1);
    }

    #[test]
    fn test_idle_scheduler_does_not_empty_a_slot() {
        // Two schedulers report every leader slot; keeping the last to arrive emptied the panel on
        // half of them.
        for order in [["10000", "0"], ["0", "10000"]] {
            let tap = MetricsTap::default();
            for id in order {
                let fields: &[(&'static str, &str)] = if id == "10000" {
                    &[
                        ("num_received", "40i"),
                        ("num_buffered", "700i"),
                        ("num_scheduled", "738i"),
                        ("num_finished", "735i"),
                    ]
                } else {
                    &[("num_received", "710i"), ("num_buffered", "717i")]
                };
                tap.observe_point(&tagged_slot_point(id, 100, fields));
            }

            let held = tap.slot_waterfalls();
            assert_eq!(held.len(), 1, "one row per slot, whatever the order");
            assert_eq!(held[0].counts.finished, 735, "arrival order {order:?}");
            assert_eq!(held[0].source, SchedulerSource::Bam);
        }
    }

    #[test]
    fn test_the_scheduler_that_built_the_slot_is_named() {
        let tap = MetricsTap::default();
        tap.observe_point(&tagged_slot_point("0", 1, &[("num_scheduled", "5i")]));
        tap.observe_point(&tagged_slot_point("10000", 2, &[("num_scheduled", "5i")]));
        tap.observe_point(&slot_point(3, &[("num_scheduled", "5i")]));

        let held = tap.slot_waterfalls();
        let sources: Vec<SchedulerSource> = held.iter().map(|slot| slot.source).collect();
        assert_eq!(
            sources,
            [
                SchedulerSource::Scheduler,
                SchedulerSource::Bam,
                SchedulerSource::Scheduler
            ]
        );
    }

    #[test]
    fn test_an_empty_leader_slot_keeps_the_report_that_saw_the_most() {
        let tap = MetricsTap::default();
        tap.observe_point(&tagged_slot_point("10000", 100, &[("num_received", "2i")]));
        tap.observe_point(&tagged_slot_point("0", 100, &[("num_buffered", "31i")]));

        let held = tap.slot_waterfalls();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].counts.buffered, 31);
        assert_eq!(held[0].source, SchedulerSource::Scheduler);
    }

    #[test]
    fn test_leader_slots_below_the_floor_are_dropped_and_refused() {
        let tap = MetricsTap::default();
        for slot in 0..8 {
            tap.observe_point(&slot_point(slot, &[("num_received", "1i")]));
            tap.observe_point(&cost_point(true, &[("bank_slot", &format!("{slot}i"))]));
        }
        tap.take_changed_waterfalls();
        tap.hold_slots_from(5);
        assert_eq!(tap.slot_waterfalls().first().map(|w| w.slot), Some(5));
        assert_eq!(tap.slot_costs().first().map(|c| c.slot), Some(5));
        tap.observe_point(&slot_point(2, &[("num_received", "1i")]));
        assert_eq!(tap.slot_waterfalls().len(), 3);
        assert!(tap.take_changed_waterfalls().is_empty());
    }

    #[test]
    fn test_each_slot_changed_is_taken_once() {
        let tap = MetricsTap::default();
        tap.observe_point(&slot_point(20, &[("num_received", "1i")]));
        tap.observe_point(&slot_point(21, &[("num_received", "1i")]));
        assert_eq!(
            tap.take_changed_waterfalls()
                .iter()
                .map(|w| w.slot)
                .collect::<Vec<_>>(),
            [20, 21]
        );
        assert!(tap.take_changed_waterfalls().is_empty());
        tap.observe_point(&slot_point(20, &[("num_buffered", "9i")]));
        assert_eq!(tap.take_changed_waterfalls().len(), 1, "more work replaces");
        tap.observe_point(&slot_point(20, &[("num_buffered", "2i")]));
        assert!(
            tap.take_changed_waterfalls().is_empty(),
            "less work does not"
        );
        tap.observe_point(&cost_point(true, &[("bank_slot", "20i")]));
        assert_eq!(tap.take_changed_costs().len(), 1);
        assert_eq!(tap.recent_costs(8).len(), 1);
    }

    #[test]
    fn test_the_costliest_account_is_counted_over_the_blocks_held() {
        let tap = MetricsTap::default();
        for (slot, account, cost) in [(1, "A", "5i"), (2, "B", "9i"), (3, "A", "7i")] {
            tap.observe_point(&cost_point(
                true,
                &[
                    ("bank_slot", &format!("{slot}i")),
                    ("costliest_account", account),
                    ("costliest_account_cost", cost),
                ],
            ));
        }
        let found = tap.costliest_recurrences(&["A", "C"]);
        assert_eq!(
            found.get("A"),
            Some(&Recurrence {
                blocks: 2,
                of: 3,
                peak_cost: 7,
                peak_slot: 3,
            })
        );
        assert_eq!(found.get("C"), None);
    }

    const QUIC_POINT: &[(&str, &str)] = &[
        ("total_incoming_connection_attempts", "18420i"),
        ("connection_rate_limited_across_all", "2140i"),
        ("connection_rate_limited_per_ipaddr", "6880i"),
        ("refused_connections_too_many_open_connections", "412i"),
        ("connection_setup_timeout", "1205i"),
        ("connection_setup_error", "338i"),
        ("new_connections", "7445i"),
        ("connection_add_failed", "7i"),
        ("connection_added_from_staked_peer", "1890i"),
        ("connection_added_from_unstaked_peer", "5548i"),
        ("new_streams", "42880i"),
        ("throttled_staked_streams", "0i"),
        ("throttled_unstaked_streams", "3412i"),
        ("stream_read_timeouts", "288i"),
        ("stream_read_errors", "41i"),
        ("invalid_stream_size", "12i"),
        ("packets_sent_to_consumer", "900i"),
        ("bytes_sent_to_consumer", "64000i"),
        ("total_handle_chunk_to_packet_send_full_err", "8i"),
        ("total_handle_chunk_to_packet_send_disconnected_err", "1i"),
        ("open_connections", "1284i"),
        ("active_streams", "46i"),
    ];

    #[test]
    fn test_the_quic_fields_are_the_ones_the_point_carries() {
        // A name from the struct rather than the wire reads nought for ever.
        let tap = MetricsTap::default();
        tap.observe_point(&named(QUIC_TPU, QUIC_POINT));

        let read = tap.counters();
        let quic = read.quic.totals;
        assert_eq!(quic.offered, 18_420);
        assert_eq!(quic.shed_all, 2_140);
        assert_eq!(quic.shed_address, 6_880);
        assert_eq!(quic.refused_full, 412);
        assert_eq!(quic.handshake_timeout, 1_205);
        assert_eq!(quic.handshake_error, 338);
        assert_eq!(quic.handshook, 7_445);
        assert_eq!(quic.add_failed, 7);
        assert_eq!(quic.admitted_staked, 1_890);
        assert_eq!(quic.admitted_unstaked, 5_548);
        assert_eq!(quic.streams, 42_880);
        assert_eq!(quic.throttled_staked, 0);
        assert_eq!(quic.throttled_unstaked, 3_412);
        assert_eq!(quic.read_timeouts, 288);
        assert_eq!(quic.read_errors, 41);
        assert_eq!(quic.invalid_size, 12);
        assert_eq!(quic.handed_on, 900);
        assert_eq!(quic.queue_full, 8);
        assert_eq!(quic.disconnected, 1);
        assert_eq!(read.quic.levels.open, 1_284);
        assert_eq!(read.quic.levels.active_streams, 46);
    }

    #[test]
    fn test_the_refusal_counters_are_kept_apart_rather_than_summed() {
        // Four overlapping names for one refusal. The tap reads them apart; the panel
        // takes the larger rather than the sum.
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            QUIC_TPU,
            &[
                ("connection_add_failed", "40i"),
                ("connection_add_failed_staked_node", "3i"),
                ("connection_add_failed_unstaked_node", "40i"),
                ("connection_add_failed_banned", "2i"),
                ("connection_add_failed_on_pruning", "9i"),
            ],
        ));

        let quic = tap.counters().quic.totals;
        assert_eq!(quic.add_failed, 40);
        assert_eq!(quic.add_failed_staked, 3);
        assert_eq!(quic.add_failed_unstaked, 40);
        assert_eq!(quic.add_failed_banned, 2);
    }

    #[test]
    fn test_the_cumulative_offer_is_stored_rather_than_added() {
        // Every counter is reported with `swap` except the offer, which arrives cumulative.
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            QUIC_TPU,
            &[
                ("total_incoming_connection_attempts", "1000i"),
                ("new_streams", "10i"),
            ],
        ));
        tap.observe_point(&named(
            QUIC_TPU,
            &[
                ("total_incoming_connection_attempts", "1600i"),
                ("new_streams", "10i"),
            ],
        ));

        let quic = tap.counters().quic.totals;
        assert_eq!(quic.offered, 1_600);
        assert_eq!(quic.streams, 20);
    }

    #[test]
    fn test_the_levels_are_the_latest_reading_rather_than_a_sum() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            QUIC_TPU,
            &[("open_connections", "900i"), ("active_streams", "12i")],
        ));
        tap.observe_point(&named(
            QUIC_TPU,
            &[("open_connections", "870i"), ("active_streams", "9i")],
        ));

        let levels = tap.counters().quic.levels;
        assert_eq!(levels.open, 870);
        assert_eq!(levels.active_streams, 9);
    }

    #[test]
    fn test_each_quic_port_counts_into_its_own_set() {
        // Three listeners share field names under three point names; summed, the busiest hides the
        // others.
        let tap = MetricsTap::default();
        tap.observe_point(&named(QUIC_TPU, &[("new_streams", "900i")]));
        tap.observe_point(&named(QUIC_TPU_FORWARDS, &[("new_streams", "40i")]));
        tap.observe_point(&named(QUIC_TPU_VOTE, &[("new_streams", "70i")]));

        let read = tap.counters();
        assert_eq!(read.quic.totals.streams, 900);
        assert_eq!(read.quic_forwards.totals.streams, 40);
        assert_eq!(read.quic_vote.totals.streams, 70);
    }

    #[test]
    fn test_the_accounts_points_add_into_one_set_of_figures() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            ACCOUNTS_LOADS,
            &[
                ("num_loaded_from_write_cache", "10i"),
                ("num_loaded_from_read_cache", "900i"),
                ("num_loaded_from_index_storage", "40i"),
            ],
        ));
        tap.observe_point(&named(
            ACCOUNTS_FLUSH,
            &[
                ("num_accounts_stored", "500i"),
                ("account_bytes_stored", "64000i"),
            ],
        ));
        tap.observe_point(&named(
            ACCOUNTS_STORES,
            &[
                ("total_bytes", "137400000000i"),
                ("total_alive_bytes", "27700000000i"),
                ("total_count", "812i"),
            ],
        ));

        let counters = tap.counters();
        assert_eq!(counters.accounts.totals.loaded_from_storage, 40);
        assert_eq!(counters.accounts.totals.stored_accounts, 500);
        assert_eq!(counters.accounts.totals.stored_bytes, 64_000);
        assert_eq!(counters.accounts.levels.storage_alive_bytes, 27_700_000_000);
        assert_eq!(counters.accounts.levels.storage_count, 812);
    }

    #[test]
    fn test_upkeep_reads_each_point_by_its_own_fields() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            ACCOUNTS_INDEX,
            &[
                ("count", "900i"),
                ("count_in_mem", "900i"),
                ("capacity_in_mem", "1024i"),
                ("estimate_mem_bytes", "64000i"),
                ("gets_from_mem", "70i"),
                ("gets_missing", "5i"),
                ("inserts", "3i"),
                ("deletes", "2i"),
            ],
        ));
        tap.observe_point(&named(
            ACCOUNTS_WRITE_CACHE,
            &[
                ("total_size", "700i"),
                ("num_slots", "4i"),
                ("total_accounts_count", "30i"),
            ],
        ));
        for root in ["100i", "200i"] {
            tap.observe_point(&named(
                ACCOUNTS_CLEAN,
                &[
                    ("max_clean_root", root),
                    ("total_us", "1500i"),
                    ("slots_cleaned", "8i"),
                    ("num_accounts_removed_from_index", "6i"),
                ],
            ));
        }
        // The flush stores accounts too, under a name shrink also reports.
        let shrunk = [
            ("num_slots_shrunk", "2i"),
            ("accounts_removed", "11i"),
            ("bytes_removed", "4000i"),
            ("bytes_written", "9000i"),
            ("num_accounts_stored", "50i"),
        ];
        tap.observe_point(&named(ACCOUNTS_SHRINK, &shrunk));
        tap.observe_point(&named(ACCOUNTS_SHRINK_ANCIENT, &shrunk));
        // A clean still running counts once it ends.
        let active = |job, running, at_millis| {
            let mut point = named(ACCOUNTS_ACTIVE, &[(job, running)]);
            point.timestamp = UNIX_EPOCH + std::time::Duration::from_millis(at_millis);
            point
        };
        tap.observe_point(&active("clean", "1i", 1_000));
        tap.observe_point(&active("flush", "1i", 1_000));
        tap.observe_point(&active("flush", "0i", 1_040));
        tap.observe_point(&active("shrink", "1i", 1_100));
        tap.observe_point(&active("shrink", "0i", 1_300));
        tap.observe_point(&active("squash_ancient", "1i", 1_300));
        tap.observe_point(&active("squash_ancient", "0i", 1_350));

        let counters = tap.counters();
        assert_eq!(
            counters.accounts_upkeep.totals,
            AccountsUpkeepTotals {
                index_found: 70,
                index_missing: 5,
                index_inserts: 3,
                index_deletes: 2,
                clean_runs: 2,
                cleaned_slots: 16,
                cleaned_accounts: 12,
                shrunk_slots: 4,
                shrunk_accounts: 22,
                shrunk_bytes: 8000,
                rewritten_bytes: 18000,
                clean_micros: 0,
                shrink_micros: 250_000,
                flush_micros: 40_000,
            }
        );
        assert_eq!(
            counters.accounts_upkeep.levels,
            AccountsUpkeepLevels {
                index_entries: 900,
                index_capacity: 1024,
                index_bytes: 64000,
                write_cache_bytes: 700,
                write_cache_slots: 4,
                write_cache_accounts: 30,
                cleaned_to: 200,
            }
        );
        assert_eq!(counters.accounts.totals.stored_accounts, 0);
    }

    #[test]
    fn test_the_storage_levels_are_replaced_rather_than_summed() {
        // A level, not a count: summed, a steady hundred gigabytes would read as
        // terabytes within a minute.
        let tap = MetricsTap::default();
        tap.observe_point(&named(ACCOUNTS_STORES, &[("total_bytes", "100i")]));
        tap.observe_point(&named(ACCOUNTS_STORES, &[("total_bytes", "104i")]));
        assert_eq!(tap.counters().accounts.levels.storage_bytes, 104);
    }

    #[test]
    fn test_a_level_is_replaced_rather_than_accumulated() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(PROGRAM_CACHE, &[("water_level", "400i")]));
        tap.observe_point(&named(PROGRAM_CACHE, &[("water_level", "412i")]));
        assert_eq!(tap.counters().program_cache.water_level, 412);
    }

    #[test]
    fn test_replace_entry_lands_on_replacements() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            PROGRAM_CACHE,
            &[("replace_entry", "3i"), ("hits", "90i"), ("misses", "10i")],
        ));

        let cache = tap.counters().program_cache.totals;
        assert_eq!(cache.replacements, 3);
        assert_eq!(cache.hits, 90);
        assert_eq!(cache.misses, 10);
    }

    #[test]
    fn test_the_verify_stage_accounts_for_every_packet_it_was_given() {
        // No counter exists for a failed signature; it is what is left once duplicates, underpaying
        // and verified are taken off.
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            TPU_VERIFIER,
            &[
                ("total_packets", "1000i"),
                ("total_dedup", "300i"),
                ("total_dropped_below_priority_floor", "50i"),
                ("total_valid_packets", "620i"),
                ("total_verify_time_us", "4200i"),
            ],
        ));

        let verify = tap.counters().verify;
        assert_eq!(verify.received, 1_000);
        let accounted = verify
            .duplicate
            .saturating_add(verify.below_floor)
            .saturating_add(verify.verified);
        assert_eq!(verify.received.saturating_sub(accounted), 30);
    }

    #[test]
    fn test_the_bundle_stage_adds_into_one_set() {
        let tap = MetricsTap::default();
        for _ in 0..2 {
            tap.observe_point(&named(
                BUNDLE_STAGE,
                &[
                    ("num_bundles_received", "3i"),
                    ("num_packets_received", "11i"),
                ],
            ));
        }
        assert_eq!(
            tap.counters().bundles,
            BundleTotals {
                received: 6,
                packets: 22,
            }
        );
    }

    #[test]
    fn test_every_worker_adds_into_the_same_execution_totals() {
        let tap = MetricsTap::default();
        for _ in 0..4 {
            tap.observe_point(&named(
                WORKER_COUNTS,
                &[
                    ("transactions_attempted_processing_count", "100i"),
                    ("processed_transactions_count", "90i"),
                    ("processed_with_successful_result_count", "80i"),
                ],
            ));
        }

        let executed = tap.counters().executed;
        assert_eq!(executed.attempted, 400);
        assert_eq!(executed.processed, 360);
        assert_eq!(executed.succeeded, 320);
    }

    #[test]
    fn test_worker_error_reasons_join_its_counts() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            WORKER_COUNTS,
            &[
                ("transactions_attempted_processing_count", "101i"),
                ("retryable_transaction_count", "13i"),
                ("processed_transactions_count", "63i"),
                ("processed_with_successful_result_count", "63i"),
            ],
        ));
        tap.observe_point(&named(
            WORKER_ERROR_METRICS,
            &[
                ("blockhash_not_found", "12i"),
                ("insufficient_funds", "8i"),
                ("already_processed", "4i"),
                // Counted, but drawn as a retry rather than as a loss, so it
                // must not land in one of the reasons above.
                ("account_in_use", "13i"),
                // The sum of every error, including those drawn elsewhere.
                ("total", "37i"),
            ],
        ));

        let executed = tap.counters().executed;
        assert_eq!(executed.attempted, 101);
        assert_eq!(executed.retryable, 13);
        assert_eq!(executed.processed, 63);
        assert_eq!(executed.blockhash_missing, 12);
        assert_eq!(executed.fee_payer_broke, 8);
        assert_eq!(executed.already_processed, 4);
        assert_eq!(
            executed.attempted.saturating_sub(executed.processed),
            38,
            "nothing from the error point was added to the outcomes"
        );
    }

    fn xdp_point(
        zero_copy: bool,
        driver: &str,
        vendor: &str,
        model: &str,
        kernel: &str,
    ) -> DataPoint {
        let mut point = DataPoint::new(XDP_NETWORK_CONFIG);
        point.add_tag("driver", driver);
        point.add_tag("zero_copy", &zero_copy.to_string());
        point.add_field_str("kernel_version", kernel);
        point.add_field_str("vendor", vendor);
        point.add_field_str("model", model);
        point
    }

    #[test]
    fn test_xdp_config_is_read_from_tags_and_fields() {
        // A tag keeps its value; a string field arrives wrapped in quotes for the line
        // protocol.
        let tap = MetricsTap::default();
        tap.observe_point(&xdp_point(
            true,
            "ice",
            "Intel Corporation",
            "Ethernet Controller E810-C for QSFP",
            "6.8.0-45-generic",
        ));

        let xdp = tap.xdp().expect("a reported config is held");
        assert!(xdp.zero_copy);
        assert_eq!(xdp.driver, "ice");
        assert_eq!(xdp.vendor, "Intel Corporation");
        assert_eq!(xdp.model, "Ethernet Controller E810-C for QSFP");
        assert_eq!(xdp.kernel_version, "6.8.0-45-generic");
    }

    #[test]
    fn test_there_is_no_xdp_config_where_none_was_ever_reported() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(SHREDS_TURBINE, &[("packets_count", "1i")]));
        assert!(tap.xdp().is_none());
    }

    #[test]
    fn test_missing_zero_copy_tag_reads_as_copy() {
        let mut point = DataPoint::new(XDP_NETWORK_CONFIG);
        point.add_tag("driver", "mlx5_core");
        point.add_field_str("model", "MT2892 Family");
        let tap = MetricsTap::default();
        tap.observe_point(&point);

        let xdp = tap
            .xdp()
            .expect("a config with no zero_copy tag is still a config");
        assert!(!xdp.zero_copy);
        assert_eq!(xdp.driver, "mlx5_core");
        assert_eq!(xdp.kernel_version, "");
    }

    #[test]
    fn test_the_latest_xdp_report_stands() {
        // Overwriting lets a config read once the PCI database is available win over an early read.
        let tap = MetricsTap::default();
        tap.observe_point(&xdp_point(false, "ice", "unknown", "unknown", "6.8.0"));
        tap.observe_point(&xdp_point(
            false,
            "ice",
            "Intel Corporation",
            "Ethernet Controller E810-C for QSFP",
            "6.8.0",
        ));
        assert_eq!(tap.xdp().unwrap().vendor, "Intel Corporation");
    }

    #[test]
    fn test_a_string_field_keeps_a_quote_that_was_part_of_the_value() {
        // The wrapper is one pair, not every quote at either end, and a quote
        // inside the value arrives escaped.
        assert_eq!(field_str(r#""6.8.0""#), "6.8.0");
        assert_eq!(field_str(r#""a \"b\" c""#), r#"a "b" c"#);
        assert_eq!(field_str("unquoted"), "unquoted");
    }

    #[test]
    fn test_the_stake_seen_in_gossip_is_read_in_lamports() {
        let tap = MetricsTap::default();
        assert!(tap.stake_in_gossip().is_none());
        tap.observe_point(&named(
            WFSM_GOSSIP,
            &[
                ("online_stake", "2350000000000000i"),
                ("offline_stake", "401650000000000000i"),
                ("total_activated_stake", "404000000000000000i"),
            ],
        ));
        assert_eq!(
            tap.stake_in_gossip(),
            Some(StakeInGossip {
                online: 2_350_000_000_000_000,
                total: 404_000_000_000_000_000,
            })
        );
    }

    #[test]
    fn test_a_stake_point_without_a_total_is_dropped() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(WFSM_GOSSIP, &[("online_stake", "5i")]));
        assert!(tap.stake_in_gossip().is_none());
    }

    #[test]
    fn test_the_latest_stake_count_stands() {
        let tap = MetricsTap::default();
        for online in ["1i", "2i"] {
            tap.observe_point(&named(
                WFSM_GOSSIP,
                &[("online_stake", online), ("total_activated_stake", "10i")],
            ));
        }
        assert_eq!(tap.stake_in_gossip().unwrap().online, 2);
    }

    #[test]
    fn test_gossip_reports_add_into_their_totals_and_levels() {
        let tap = MetricsTap::default();
        for _ in 0..2 {
            tap.observe_point(&named(
                "cluster_info_stats",
                &[
                    ("table_size", "231904i"),
                    ("num_pubkeys", "4112i"),
                    ("num_nodes", "4100i"),
                    ("num_nodes_staked", "1212i"),
                ],
            ));
            tap.observe_point(&named(
                "cluster_info_stats2",
                &[
                    ("purge_count", "352i"),
                    ("process_gossip_packets_time", "900i"),
                    ("handle_batch_push_messages_time", "600i"),
                ],
            ));
            tap.observe_point(&named(
                "cluster_info_stats4",
                &[
                    ("skip_push_message_shred_version", "14i"),
                    ("num_duplicate_push_messages", "8i"),
                ],
            ));
            tap.observe_point(&named(
                "cluster_info_stats5",
                &[
                    ("packets_received_count", "23100i"),
                    ("packets_received_push_messages_count", "23000i"),
                    ("packets_sent_pong_messages_count", "24i"),
                    ("num_unverifed_gossip_addrs", "3i"),
                ],
            ));
        }
        let counters = tap.counters();
        assert_eq!(
            counters.gossip.levels,
            GossipLevels {
                table_size: 231_904,
                pubkeys: 4_112,
                nodes: 4_100,
                staked_nodes: 1_212
            }
        );
        let gossip = counters.gossip.totals;
        assert_eq!(gossip.reports, 2);
        assert_eq!(gossip.expired, 704);
        assert_eq!((gossip.process_us, gossip.push_us), (1_800, 1_200));
        assert_eq!(
            (gossip.other_shred_version_push, gossip.duplicate_push),
            (28, 16)
        );
        assert_eq!(
            (
                gossip.received,
                gossip.received_push,
                gossip.sent_pong,
                gossip.unverified_addresses
            ),
            (46_200, 46_000, 48, 6)
        );
    }

    #[test]
    fn test_gossip_entries_count_by_type_and_in_total() {
        let tap = MetricsTap::default();
        tap.observe_point(&named(
            GOSSIP_ENTRIES,
            &[
                ("Vote-push", "3900i"),
                ("Vote-pull", "41i"),
                ("ContactInfo-push", "318i"),
                ("all-push", "4218i"),
                ("all-pull", "41i"),
            ],
        ));
        tap.observe_point(&named(
            GOSSIP_ENTRY_FAILS,
            &[
                ("Vote-push", "280i"),
                ("Vote-pull", "8i"),
                ("all-push", "280i"),
                ("all-pull", "8i"),
            ],
        ));
        let counters = tap.counters();
        let vote = GOSSIP_ENTRY_TYPES
            .iter()
            .position(|kind| *kind == "Vote")
            .unwrap();
        let contact = GOSSIP_ENTRY_TYPES
            .iter()
            .position(|kind| *kind == "ContactInfo")
            .unwrap();
        let entries = counters.gossip.entries;
        assert_eq!(
            (
                entries.push[vote],
                entries.pull[vote],
                entries.rejected[vote]
            ),
            (3_900, 41, 288)
        );
        assert_eq!(entries.push[contact], 318);
        let gossip = counters.gossip.totals;
        assert_eq!(
            (
                gossip.accepted_push,
                gossip.accepted_pull,
                gossip.rejected_push,
                gossip.rejected_pull
            ),
            (4_218, 41, 280, 8)
        );
    }

    #[test]
    fn test_gossip_entry_windows_difference_and_sum() {
        let mut a = GossipEntryTotals::default();
        let mut b = GossipEntryTotals::default();
        a.push[1] = 10;
        b.push[1] = 4;
        b.rejected[3] = 9;
        assert_eq!(a.since(&b).push[1], 6);
        assert_eq!(a.since(&b).rejected[3], 0, "saturates rather than wrapping");
        assert_eq!(a.plus(&b).push[1], 14);
    }
}
