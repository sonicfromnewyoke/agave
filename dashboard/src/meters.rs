//! The once-a-second readings: throughput, host, network, sockets, caches and
//! the TPU path. On their own thread, reading the working bank without bank
//! forks' lock.

use {
    crate::{
        collect::{CATCH_UP_SLOTS_PER_SECOND, FAILED_UNREAD, system_time_nanos},
        host_stats::{self, CpuUse, Filesystem, HostSnapshot, ResidentKinds},
        meter_source::{HostPaths, IngestAddresses, WorkingReading},
        metrics_tap::{
            AccountsFiles, AccountsTotals, AccountsUpkeepLevels, AccountsUpkeepTotals,
            BundleTotals, ExecutedTotals, GOSSIP_ENTRY_TYPES, GossipEntryTotals, GossipTotals,
            LedgerFiles, MemoryParts, MetricsTap, NetworkTotals, ProgramCacheTotals, QuicLevels,
            QuicTotals, ReplaySlotTimes, TapCounters, VerifyTotals, WindowedCounters, XdpConfig,
            push_capped,
        },
        net_ifaces::{self, InterfaceReading},
        net_stats::{self, Counted, InterfaceCounters, NetCounters, NetReading},
        proto::{Publisher, TOPIC_SUMMARY},
        remote::Shared,
        snapshot::{self, ArchiveBytes},
        thread_stats::{self, ThreadGroup, ThreadReading},
        udp_drops::{self, PortCounters, PortWindow},
    },
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_core::validator::ValidatorStartProgress,
    solana_ledger::blockstore_options::BLOCKSTORE_DIRECTORY_ROCKS_LEVEL,
    solana_program_runtime::loaded_programs::MAX_LOADED_ENTRY_COUNT,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant, SystemTime},
    },
};

pub const METER_INTERVAL: Duration = Duration::from_secs(1);

const CHART_HISTORY: usize = 300;

/// Leader slots' waterfalls and costs sent to a connecting client, as many as its blocks.
const SLOT_LISTS_SENT: usize = 500;

const THREADS_HISTORY: usize = 60;

/// The host and thread readings stop while nobody watches; one older than this spans such a gap.
const WATCH_GAP: Duration = Duration::from_secs(5);

const THREAD_ROWS: usize = 8;

/// Every thread's name is read again this often; between, names carry over from the last reading.
const THREAD_NAMES_REFRESH: Duration = Duration::from_secs(60);

const DROPS_WINDOW: Duration = Duration::from_secs(60);

const ACCOUNTS_CACHE_WINDOW: usize = 60;

const SHREDS_WINDOW: usize = 300;

const WATERFALL_WINDOW: usize = 300;

/// One sample is one slot's handful of loads, too few for a rate.
const PROGRAM_CACHE_WINDOW: usize = 60;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Shreds {
    pub received: u64,
    pub repaired: u64,
    pub repair_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Turbine {
    pub root: u64,
    pub layer_1: u64,
    pub layer_2: u64,
    pub layer_3: u64,
    pub xdp_dropped: u64,
    pub xdp: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountsCache {
    pub read: u64,
    pub hit_rate: f64,
    pub evictions: u64,
    pub cache_bytes: u64,
    pub cache_entries: u64,

    /// Only `from_storage` touches a file; counted in accounts because nothing counts bytes.
    pub from_write_cache: u64,
    pub from_read_cache: u64,
    pub from_storage: u64,

    pub stored_accounts: u64,
    pub stored_bytes: u64,
    pub window_seconds: f64,

    pub disk: Option<AccountsDisk>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountsDisk {
    pub used: u64,
    pub allocated: u64,
    pub fragmented: u64,
    pub storages: u64,
}

/// The index and write cache as they stand, and what clean, shrink and the index did over the
/// window.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountsDb {
    #[serde(flatten)]
    pub levels: AccountsUpkeepLevels,
    #[serde(flatten)]
    pub totals: AccountsUpkeepTotals,
    pub window_seconds: f64,
}

/// The counters reset per bank, so `looked_up` is the window's own total.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProgramCache {
    pub looked_up: u64,
    #[serde(flatten)]
    pub totals: ProgramCacheTotals,
    pub hit_rate: f64,
    pub peak_entries: Option<u64>,
    pub entry_limit: u64,
}

/// `None` while nothing has been asked, so an idle cache does not read as failing.
fn cache_rate(totals: &AccountsTotals) -> Option<(u64, f64)> {
    let asked = totals.cache_hits.saturating_add(totals.cache_misses);
    (asked > 0).then(|| (asked, totals.cache_hits as f64 / asked as f64))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tps {
    pub total: f64,
    pub vote: f64,
    pub non_vote_success: f64,
    pub non_vote_failed: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TpsSample {
    pub slot: Slot,
    pub timestamp_nanos: u64,
    #[serde(flatten)]
    pub tps: Tps,
}

/// In bytes per second.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Network {
    pub received_per_second: u64,
    pub sent_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NetInterface {
    #[serde(flatten)]
    pub reading: InterfaceReading,
    /// The most routes it carried in any hour of the last day, this one included.
    pub routes_peak: u32,
    /// `None` until a second reading is in, or after a counter went backwards.
    pub received_per_second: Option<u64>,
    pub sent_per_second: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Host {
    pub cores: usize,
    pub load_one: f64,
    pub load_five: f64,
    pub load_fifteen: f64,
    pub threads: u64,
    pub running: u64,
    pub cpu: Option<CpuUse>,
    pub cpu_governor: Option<String>,

    pub memory_total: u64,
    pub memory_available: u64,
    /// "Used" counts it, and sixty gigabytes of page cache is not a shortage.
    pub memory_reclaimable: u64,
    pub memory_free: u64,
    pub swap: Option<Swap>,
    pub process_resident: Option<u64>,
    pub process_resident_hour_ago: Option<u64>,
    pub process_kinds: Option<ResidentKinds>,
    pub memory_parts: MemoryParts,
    pub snapshot_device: Option<String>,

    pub filesystems: Vec<FilesystemUsage>,
    pub devices: Vec<DeviceLoad>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Swap {
    pub total: u64,
    pub used: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FilesystemUsage {
    pub name: String,
    pub path: String,
    #[serde(flatten)]
    pub usage: Filesystem,
    pub parts: Vec<DiskPart>,
    /// Directories named `*_to_be_deleted`, which only the thread that renamed them removes.
    pub leftovers: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DiskPart {
    pub key: DiskPartKey,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskPartKey {
    Shreds,
    History,
    Metadata,
    Live,
    Fragmented,
    Full,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeviceLoad {
    pub device: String,
    pub roles: Vec<String>,
    pub busy: f64,
    pub wait_ms: Option<f64>,
    pub operations_per_second: u64,
    pub read_per_second: u64,
    pub write_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NetworkSample {
    pub timestamp_nanos: u64,
    #[serde(flatten)]
    pub rates: Network,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadsSample {
    pub timestamp_nanos: u64,
    pub threads: usize,
    pub groups: Vec<ThreadGroup>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IngestPath {
    pub name: &'static str,
    pub port: u16,
    /// Always sent, including as zero, so a healthy row differs from an unmeasured one.
    pub drops_recent: u64,
    pub drops_total: u64,
    pub queued_bytes: u64,

    /// Packets the port handed over across the same window. `None` for a port with no receiver
    /// reporting one; `Some(0)` where the validator logs below info.
    pub received_recent: Option<u64>,
    pub received_total: Option<u64>,
    /// Always true: each receiver here counts packets, which the page takes its shares against.
    pub received_in_packets: bool,

    pub quic: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuicPort {
    pub name: &'static str,
    #[serde(flatten)]
    pub counts: QuicTotals,
    #[serde(flatten)]
    pub levels: QuicLevels,
    /// Datagrams the kernel discarded on this port over the same span. Whole datagrams, unlike the
    /// counts beside them, so never added to them.
    pub kernel_drops: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuicPaths {
    pub ports: Vec<QuicPort>,
    pub tpu_offhost: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IngestSummary {
    pub window_seconds: f64,
    pub paths: Vec<IngestPath>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EpochSpan {
    pub epoch: Epoch,
    pub elapsed_slots: u64,
    pub counted_slots: u64,
    pub slots_in_epoch: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LeaderTotals {
    epoch: Option<Epoch>,
    from_slot: Slot,
    verify: VerifyTotals,
    executed: ExecutedTotals,
    bundles: BundleTotals,
}

impl LeaderTotals {
    fn add(
        &mut self,
        at: WorkingReading,
        verify: VerifyTotals,
        executed: ExecutedTotals,
        bundles: BundleTotals,
    ) {
        if self.epoch != Some(at.epoch) {
            self.epoch = Some(at.epoch);
            self.from_slot = at.slot;
            self.verify = VerifyTotals::default();
            self.executed = ExecutedTotals::default();
            self.bundles = BundleTotals::default();
        }
        self.verify = self.verify.plus(&verify);
        self.executed = self.executed.plus(&executed);
        self.bundles = self.bundles.plus(&bundles);
    }

    /// `from_slot` is clamped to the epoch's first slot, since a bank read can land before the one
    /// that turned it.
    fn span(&self, at: WorkingReading) -> EpochSpan {
        let from_slot = self.from_slot.max(at.epoch_start_slot);
        EpochSpan {
            epoch: at.epoch,
            elapsed_slots: at
                .slot
                .saturating_sub(at.epoch_start_slot)
                .saturating_add(1)
                .min(at.slots_in_epoch),
            counted_slots: at
                .slot
                .saturating_sub(from_slot)
                .saturating_add(1)
                .min(at.slots_in_epoch),
            slots_in_epoch: at.slots_in_epoch,
        }
    }
}

/// In microseconds: means per slot, and two peaks that are the worst single slot's sums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReplayWindow {
    pub slots: usize,
    pub transactions: u64,

    // Replay's own thread. Disjoint spans; these add up.
    pub fetch: u64,
    pub confirming: u64,
    pub completing: u64,
    pub serial_peak: u64,

    // Verification jobs. Concurrent and each parallel inside, so these are
    // comparable to one another and to nothing else.
    pub poh_verify: u64,
    pub tx_verify: u64,
    pub dispatch: u64,

    // Thread time across the workers. These partition.
    pub execute: u64,
    pub bytecode: u64,
    pub serialising: u64,
    pub deserialising: u64,
    pub load: u64,
    pub store: u64,
    pub program_cache: u64,
    pub compiling: u64,
    pub program_cache_peak: u64,
    pub checking: u64,
    pub other: u64,
}

fn replay_window(slots: &[ReplaySlotTimes]) -> Option<ReplayWindow> {
    let count = u64::try_from(slots.len()).ok().filter(|n| *n > 0)?;
    let mean = |total: u64| total.checked_div(count).unwrap_or_default();
    let sum = |pick: fn(&ReplaySlotTimes) -> u64| {
        slots
            .iter()
            .fold(0u64, |total, slot| total.saturating_add(pick(slot)))
    };
    let peak = |pick: fn(&ReplaySlotTimes) -> u64| slots.iter().map(pick).max().unwrap_or_default();

    Some(ReplayWindow {
        slots: slots.len(),
        transactions: mean(sum(|s| s.transactions)),

        fetch: mean(sum(|s| s.fetch)),
        confirming: mean(sum(|s| s.confirming)),
        completing: mean(sum(|s| s.completing)),
        serial_peak: peak(ReplaySlotTimes::serial),

        poh_verify: mean(sum(|s| s.poh_verify)),
        tx_verify: mean(sum(|s| s.tx_verify)),
        dispatch: mean(sum(|s| s.dispatch)),

        execute: mean(sum(|s| s.execute)),
        bytecode: mean(sum(|s| s.bytecode)),
        serialising: mean(sum(|s| s.serialising)),
        deserialising: mean(sum(|s| s.deserialising)),
        load: mean(sum(|s| s.load)),
        store: mean(sum(|s| s.store)),
        program_cache: mean(sum(|s| s.program_cache)),
        compiling: mean(sum(|s| s.compiling)),
        program_cache_peak: peak(|s| s.program_cache),
        checking: mean(sum(|s| s.checking)),
        other: mean(sum(|s| s.other)),
    })
}

struct IngestPort {
    name: &'static str,
    port: u16,
    received: Option<u64>,
    quic: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct HostPath {
    name: String,
    path: PathBuf,
    device: Option<String>,
    filesystem: u64,
}

impl HostPath {
    fn new(name: String, path: PathBuf, filesystem: u64) -> Self {
        let device = host_stats::device_for(&path).ok().flatten();
        Self {
            name,
            path,
            device,
            filesystem,
        }
    }
}

/// Which filesystem each part of a row's split lives on.
#[derive(Debug, Clone, Default, PartialEq)]
struct DiskLayout {
    ledger: Option<u64>,
    /// `None` across several filesystems: the storage figures cover every accounts path.
    accounts: Option<u64>,
    /// Where a renamed accounts directory would sit.
    account_parents: Vec<(u64, PathBuf)>,
    full: Option<u64>,
    incremental: Option<u64>,
}

/// Where the blockstore lives: its rocksdb directory, which can be a mount of its own, named as the
/// ledger directory where it is not.
fn blockstore_path(ledger: &Path) -> Option<(PathBuf, u64)> {
    let ledger_id = host_stats::filesystem_id(ledger).ok();
    let rocksdb = ledger.join(BLOCKSTORE_DIRECTORY_ROCKS_LEVEL);
    match host_stats::filesystem_id(&rocksdb) {
        Ok(id) if ledger_id != Some(id) => Some((rocksdb, id)),
        _ => Some((ledger.to_path_buf(), ledger_id?)),
    }
}

/// The filesystems the card lists, one row each, which of them each part of a row's split lives
/// on, and the device of the snapshot archives.
fn resolve_disks(host: &HostPaths) -> (Vec<HostPath>, DiskLayout, Option<String>) {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    let blockstore = blockstore_path(&host.ledger);
    if let Some((path, id)) = blockstore.clone() {
        seen.insert(id);
        paths.push(HostPath::new("ledger".to_owned(), path, id));
    }

    let mut accounts = Vec::new();
    let mut account_ids = HashSet::new();
    let mut account_parents = Vec::new();
    for path in &host.accounts {
        let Ok(id) = host_stats::filesystem_id(path) else {
            continue;
        };
        account_ids.insert(id);
        if let Some(parent) = path.parent().map(Path::to_path_buf)
            && !account_parents.iter().any(|(_, held)| *held == parent)
        {
            account_parents.push((id, parent));
        }
        if seen.insert(id) {
            accounts.push((path.clone(), id));
        }
    }
    let numbered = accounts.len() > 1;
    for (index, (path, id)) in accounts.into_iter().enumerate() {
        let ordinal = index.saturating_add(1);
        let name = if numbered {
            format!("accounts {ordinal}")
        } else {
            "accounts".to_owned()
        };
        paths.push(HostPath::new(name, path, id));
    }

    let config = &host.snapshot_config;
    let full = host_stats::filesystem_id(&config.full_snapshot_archives_dir).ok();
    if let Some(id) = full
        && seen.insert(id)
    {
        let archives = config.full_snapshot_archives_dir.clone();
        paths.push(HostPath::new("snapshots".to_owned(), archives, id));
    }
    let layout = DiskLayout {
        ledger: blockstore.map(|(_, id)| id),
        accounts: match account_ids.len() {
            1 => account_ids.into_iter().next(),
            _ => None,
        },
        account_parents,
        full,
        incremental: host_stats::filesystem_id(&config.incremental_snapshot_archives_dir).ok(),
    };
    let snapshot_device = host_stats::device_for(&config.full_snapshot_archives_dir)
        .ok()
        .flatten();
    (paths, layout, snapshot_device)
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct DiskReadings {
    ledger: Option<LedgerFiles>,
    accounts: Option<AccountsFiles>,
    archives: Option<ArchiveBytes>,
}

/// In the order they are drawn.
fn disk_parts(filesystem: u64, layout: &DiskLayout, readings: &DiskReadings) -> Vec<DiskPart> {
    let part = |key, bytes| DiskPart { key, bytes };
    let mut parts = Vec::new();
    if layout.ledger == Some(filesystem)
        && let Some(files) = readings.ledger
    {
        parts.push(part(DiskPartKey::Shreds, files.shreds));
        parts.push(part(DiskPartKey::History, files.history));
        parts.push(part(DiskPartKey::Metadata, files.metadata));
    }
    if layout.accounts == Some(filesystem)
        && let Some(files) = readings.accounts
    {
        parts.push(part(DiskPartKey::Live, files.live));
        parts.push(part(DiskPartKey::Fragmented, files.fragmented));
    }
    if let Some(archives) = readings.archives {
        if layout.full == Some(filesystem) {
            parts.push(part(DiskPartKey::Full, archives.full));
        }
        if layout.incremental == Some(filesystem) {
            parts.push(part(DiskPartKey::Incremental, archives.incremental));
        }
    }
    parts
}

const LEFTOVER_SUFFIX: &str = "_to_be_deleted";

/// Each leftover directory and the filesystem it is on.
fn read_leftovers(layout: &DiskLayout) -> Vec<(u64, String)> {
    let mut leftovers = Vec::new();
    for (filesystem, parent) in &layout.account_parents {
        let Ok(entries) = std::fs::read_dir(parent) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(LEFTOVER_SUFFIX) {
                leftovers.push((*filesystem, name.into_owned()));
            }
        }
    }
    leftovers.sort();
    leftovers.dedup();
    leftovers
}

fn device_loads(
    paths: &[HostPath],
    current: &HostSnapshot,
    previous: &HostSnapshot,
    interval_ms: f64,
    seconds: f64,
) -> Vec<DeviceLoad> {
    let mut roles: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in paths {
        if let Some(device) = &path.device {
            roles
                .entry(device.clone())
                .or_default()
                .push(path.name.clone());
        }
    }

    roles
        .into_iter()
        .filter_map(|(device, roles)| {
            let now = current.disks.get(&device)?;
            let before = previous.disks.get(&device)?;
            let delta = now.since(before)?;
            Some(DeviceLoad {
                device,
                roles,
                busy: delta.busy(interval_ms).unwrap_or_default(),
                wait_ms: delta.wait_ms(),
                operations_per_second: (delta.operations() as f64 / seconds) as u64,
                read_per_second: (delta.read_bytes() as f64 / seconds) as u64,
                write_per_second: (delta.write_bytes() as f64 / seconds) as u64,
            })
        })
        .collect()
}

/// How many cores the process `pid` may run on.
fn num_cpus_of(pid: u32) -> usize {
    thread_stats::cores_allowed(u64::from(pid)).map_or_else(
        || std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
        |cores| cores.len(),
    )
}

/// Gossip reports every two seconds, so rates are read over several reports.
const GOSSIP_RATE_WINDOW: usize = 10;
const GOSSIP_MINUTE: usize = 60;

/// Gossip's `CRDS_UNIQUE_PUBKEY_CAPACITY`, which is crate-private: above it the table is trimmed to
/// staked nodes.
const GOSSIP_PUBKEY_CAPACITY: u64 = 8192;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Gossip {
    pub table: GossipTable,
    pub messages: Vec<GossipMessage>,
    pub entries: GossipEntries,
    pub pressure: GossipPressure,
    /// Milliseconds spent per second.
    pub time: GossipTime,
    /// Packets dropped either way over the last minute.
    pub dropped_last_minute: u64,
    /// Packets received and sent over the last minute, against which drops are judged.
    pub packets_last_minute: u64,
    /// Packets received and sent per second.
    pub packets_per_second: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipTable {
    pub entries: u64,
    pub pubkeys: u64,
    pub pubkey_capacity: u64,
    pub nodes: u64,
    pub staked_nodes: u64,
    pub expired_per_second: f64,
    pub evicted_last_minute: u64,
}

/// In packets per second.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipMessage {
    pub kind: &'static str,
    pub received: f64,
    pub sent: f64,
}

/// Per second.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipEntries {
    pub accepted_push: f64,
    pub accepted_pull: f64,
    pub duplicate_push: f64,
    pub redundant_pull: f64,
    pub rejected_push: f64,
    pub rejected_pull: f64,
    /// Busiest first.
    pub types: Vec<GossipEntryType>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipEntryType {
    pub kind: &'static str,
    pub push: f64,
    pub pull: f64,
    pub rejected: f64,
}

/// Per second.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipPressure {
    pub dropped_in: f64,
    pub dropped_out: f64,
    pub pull_no_budget: f64,
    pub pull_scan_exhausted: f64,
    /// Pull requests, pushed entries and pulled entries together.
    pub other_shred_version: f64,
    pub ping_check_failed: f64,
    pub unverified_addresses: f64,
    pub bad_prune_destination: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipTime {
    pub push: f64,
    pub pull_requests: f64,
    pub pull_responses: f64,
    pub ping_pong_prune: f64,
    pub verify: f64,
    pub other: f64,
}

/// Packets received and sent; drops are counted at both ends.
fn gossip_packets(totals: &GossipTotals) -> u64 {
    totals
        .received
        .saturating_add(totals.sent_push)
        .saturating_add(totals.sent_pull_requests)
        .saturating_add(totals.sent_pull_responses)
        .saturating_add(totals.sent_ping)
        .saturating_add(totals.sent_pong)
        .saturating_add(totals.sent_prune)
}

debounced_keys! {
    struct GossipMeter {
        published: Option<Gossip> = (TOPIC_SUMMARY, "gossip"),
        ;
        window: VecDeque<GossipTotals> = VecDeque::with_capacity(GOSSIP_MINUTE),
        entries: VecDeque<GossipEntryTotals> = VecDeque::with_capacity(GOSSIP_RATE_WINDOW),
    }
}

impl GossipMeter {
    /// Publishes nothing while no report has arrived for the rate window: gossip's points are
    /// info-level, so the page can say why rather than show zeros.
    fn tick(&mut self, previous: &TapCounters, current: &TapCounters, publisher: &Publisher) {
        let minute = sum_window(
            &mut self.window,
            current.gossip.totals.since(&previous.gossip.totals),
            GOSSIP_MINUTE,
        );
        let entries = sum_window(
            &mut self.entries,
            current.gossip.entries.since(&previous.gossip.entries),
            GOSSIP_RATE_WINDOW,
        );
        let recent = self.window.len().min(GOSSIP_RATE_WINDOW);
        let rates = self
            .window
            .iter()
            .skip(self.window.len().saturating_sub(recent))
            .fold(GossipTotals::default(), |total, sample| total.plus(sample));

        let gossip = (rates.reports > 0).then(|| {
            let seconds = recent as f64 * METER_INTERVAL.as_secs_f64();
            let per_second = |count: u64| count as f64 / seconds;
            let millis = |micros: u64| micros as f64 / 1_000.0 / seconds;
            let levels = &current.gossip.levels;
            let handled = rates
                .push_us
                .saturating_add(rates.pull_requests_us)
                .saturating_add(rates.pull_responses_us)
                .saturating_add(rates.ping_us)
                .saturating_add(rates.pong_us)
                .saturating_add(rates.prune_us);
            let mut types: Vec<GossipEntryType> = GOSSIP_ENTRY_TYPES
                .iter()
                .enumerate()
                .map(|(index, kind)| GossipEntryType {
                    kind,
                    push: per_second(entries.push[index]),
                    pull: per_second(entries.pull[index]),
                    rejected: per_second(entries.rejected[index]),
                })
                .collect();
            types.sort_by(|a, b| (b.push + b.pull).total_cmp(&(a.push + a.pull)));
            Gossip {
                table: GossipTable {
                    entries: levels.table_size,
                    pubkeys: levels.pubkeys,
                    pubkey_capacity: GOSSIP_PUBKEY_CAPACITY,
                    nodes: levels.nodes,
                    staked_nodes: levels.staked_nodes,
                    expired_per_second: per_second(rates.expired),
                    evicted_last_minute: minute.evicted,
                },
                messages: [
                    ("push", rates.received_push, rates.sent_push),
                    (
                        "pull_request",
                        rates.received_pull_requests,
                        rates.sent_pull_requests,
                    ),
                    (
                        "pull_response",
                        rates.received_pull_responses,
                        rates.sent_pull_responses,
                    ),
                    ("ping", rates.received_ping, rates.sent_ping),
                    ("pong", rates.received_pong, rates.sent_pong),
                    ("prune", rates.received_prune, rates.sent_prune),
                ]
                .into_iter()
                .map(|(kind, received, sent)| GossipMessage {
                    kind,
                    received: per_second(received),
                    sent: per_second(sent),
                })
                .collect(),
                entries: GossipEntries {
                    accepted_push: per_second(rates.accepted_push),
                    accepted_pull: per_second(rates.accepted_pull),
                    duplicate_push: per_second(rates.duplicate_push),
                    redundant_pull: per_second(rates.redundant_pull),
                    rejected_push: per_second(rates.rejected_push),
                    rejected_pull: per_second(rates.rejected_pull),
                    types,
                },
                pressure: GossipPressure {
                    dropped_in: per_second(rates.dropped_in),
                    dropped_out: per_second(rates.dropped_out),
                    pull_no_budget: per_second(rates.pull_no_budget),
                    pull_scan_exhausted: per_second(rates.pull_scan_exhausted),
                    other_shred_version: per_second(
                        rates
                            .other_shred_version_push
                            .saturating_add(rates.other_shred_version_pull_responses)
                            .saturating_add(rates.other_shred_version_pull_requests),
                    ),
                    ping_check_failed: per_second(rates.ping_check_failed),
                    unverified_addresses: per_second(rates.unverified_addresses),
                    bad_prune_destination: per_second(rates.bad_prune_destination),
                },
                time: GossipTime {
                    push: millis(rates.push_us),
                    pull_requests: millis(rates.pull_requests_us),
                    pull_responses: millis(rates.pull_responses_us),
                    ping_pong_prune: millis(
                        rates
                            .ping_us
                            .saturating_add(rates.pong_us)
                            .saturating_add(rates.prune_us),
                    ),
                    verify: millis(rates.verify_us),
                    other: millis(rates.process_us.saturating_sub(handled)),
                },
                dropped_last_minute: minute.dropped_in.saturating_add(minute.dropped_out),
                packets_last_minute: gossip_packets(&minute),
                packets_per_second: per_second(gossip_packets(&rates)),
            }
        });
        self.published.publish(publisher, gossip);
    }
}

pub struct Meters {
    remote: Shared,
    host_paths: HostPaths,
    /// The validator's process, whose `/proc` entries the host and thread panels read.
    pid: u32,
    /// Where the uptime counts from until the admin socket has given the validator's start.
    started: SystemTime,
    publisher: Arc<Publisher>,
    metrics_tap: Arc<MetricsTap>,
    last_tap: Option<TapCounters>,

    throughput: Throughput,
    network: NetworkMeter,
    interfaces: InterfaceMeter,
    host: HostMeter,
    sockets: SocketMeter,
    traffic: TrafficMeter,
    accounts: AccountsMeter,
    program_cache: ProgramCacheMeter,
    tpu: TpuMeter,
    threads: ThreadMeter,
    gossip: GossipMeter,
}

impl Meters {
    /// The meters reading the validator process `pid`, as the pollers keep it in `remote`.
    pub fn new(
        remote: Shared,
        host_paths: HostPaths,
        pid: u32,
        publisher: Arc<Publisher>,
        metrics_tap: Arc<MetricsTap>,
        failed_total: Arc<AtomicU64>,
    ) -> Self {
        Self {
            remote,
            host_paths,
            pid,
            started: SystemTime::now(),
            publisher,
            metrics_tap,
            last_tap: None,
            throughput: Throughput::new(failed_total),
            network: NetworkMeter::default(),
            interfaces: InterfaceMeter::default(),
            host: HostMeter::default(),
            sockets: SocketMeter::default(),
            traffic: TrafficMeter::default(),
            accounts: AccountsMeter::default(),
            program_cache: ProgramCacheMeter::default(),
            tpu: TpuMeter::default(),
            threads: ThreadMeter::default(),
            gossip: GossipMeter::default(),
        }
    }

    pub fn tick(&mut self) {
        self.collect_clock();

        let working = self.remote.read().unwrap().working;
        if let Some(working) = &working {
            self.throughput.tick(working, &self.publisher);
            self.tpu.epoch_now = Some(*working);
        }
        let alpenglow = working.is_some_and(|working| working.alpenglow);

        let interface_rates = self.network.tick(&self.publisher);
        // Read whether or not anyone watches, so the route peak covers the whole day.
        self.interfaces.tick(&self.publisher, interface_rates);
        self.tpu
            .xdp
            .publish(&self.publisher, self.metrics_tap.xdp());
        // The `/proc` walks run only while somebody is watching. The rest is
        // cheap and keeps the charts whole for a viewer connecting.
        let watched = self.publisher.subscriber_count() > 0;
        if watched {
            self.host.tick(
                &self.host_paths,
                self.pid,
                &self.metrics_tap,
                &self.publisher,
            );
            self.threads.tick(self.pid, &self.publisher);
        }
        // Until startup ends whether or not anyone watches, so the totals begin there.
        if watched || self.sockets.drops_baseline.is_none() {
            let (addresses, running) = {
                let remote = self.remote.read().unwrap();
                let running = matches!(remote.progress, Some(ValidatorStartProgress::Running));
                (remote.ingest_addresses(), running)
            };
            self.sockets.tick(
                addresses,
                &self.metrics_tap.counters(),
                running,
                alpenglow,
                &self.publisher,
            );
        }
        self.collect_from_metrics();
    }

    fn collect_clock(&self) {
        let now = SystemTime::now();
        self.publisher
            .publish(TOPIC_SUMMARY, "server_time_nanos", &system_time_nanos(now));
        let started = self.remote.read().unwrap().started;
        let uptime = now
            .duration_since(started.unwrap_or(self.started))
            .unwrap_or_default()
            .as_nanos() as u64;
        self.publisher
            .publish(TOPIC_SUMMARY, "uptime_nanos", &uptime);
    }

    fn collect_from_metrics(&mut self) {
        let current = self.metrics_tap.counters();
        let Some(previous) = self.last_tap.replace(current) else {
            return;
        };
        self.traffic.tick(&previous, &current, &self.publisher);
        self.collect_waterfall(&previous, &current);
        self.program_cache
            .tick(&previous, &current, &self.publisher);
        self.accounts.tick(&previous, &current, &self.publisher);
        self.gossip.tick(&previous, &current, &self.publisher);
    }

    fn collect_waterfall(&mut self, previous: &TapCounters, current: &TapCounters) {
        let tpu_offhost = self.sockets.sampled
            && !self.sockets.unavailable
            && !self.sockets.kernel_drops.contains_key("tpu");
        self.tpu.tick(
            &self.metrics_tap,
            previous,
            current,
            &self.sockets.kernel_drops,
            tpu_offhost,
            &self.publisher,
        );
    }
}

struct Throughput {
    /// The working bank last read, the running count of failed transactions along the working
    /// fork then, and when.
    last: Option<(WorkingReading, u64, Instant)>,
    failed_total: Arc<AtomicU64>,
    history: Vec<TpsSample>,
}

impl Throughput {
    fn new(failed_total: Arc<AtomicU64>) -> Self {
        Self {
            last: None,
            failed_total,
            history: Vec::with_capacity(CHART_HISTORY),
        }
    }

    fn tick(&mut self, working: &WorkingReading, publisher: &Publisher) {
        let errors = self.failed_total.load(Ordering::Relaxed);
        // Until a count is in there is no baseline.
        if errors == FAILED_UNREAD {
            return;
        }
        let now = Instant::now();
        let Some((previous, previous_errors, sampled_at)) =
            self.last.replace((*working, errors, now))
        else {
            return;
        };
        // A fork switch or a restart makes the counters incomparable; the same bank's are not.
        if working.slot < previous.slot
            || working.transaction_count < previous.transaction_count
            || errors < previous_errors
        {
            return;
        }
        let seconds = now.duration_since(sampled_at).as_secs_f64();
        if seconds <= 0.0 {
            return;
        }

        // While catching up, replay chews through slots far faster than the cluster
        // produces them, and one such sample would pin the chart's scale.
        let per_second = |now: u64, before: u64| now.saturating_sub(before) as f64 / seconds;
        if per_second(working.slot, previous.slot) > CATCH_UP_SLOTS_PER_SECOND {
            return;
        }

        let total = per_second(working.transaction_count, previous.transaction_count);
        let non_vote = per_second(
            working.non_vote_transaction_count,
            previous.non_vote_transaction_count,
        );
        let failed = per_second(errors, previous_errors);
        let tps = Tps {
            total,
            vote: (total - non_vote).max(0.0),
            non_vote_success: (non_vote - failed).max(0.0),
            non_vote_failed: failed.min(non_vote),
        };

        let sample = TpsSample {
            slot: working.slot,
            timestamp_nanos: system_time_nanos(SystemTime::now()),
            tps,
        };
        publisher.publish_ephemeral(TOPIC_SUMMARY, "tps_sample", &sample);

        push_history(
            &mut self.history,
            sample,
            CHART_HISTORY,
            publisher,
            "tps_history",
        );
    }
}

#[derive(Default)]
struct NetworkMeter {
    last: Option<(NetReading, Instant)>,
    history: Vec<NetworkSample>,
    /// Each interface's bytes a second over the same interval as the host total.
    interface_rates: HashMap<String, Network>,
    unavailable: bool,
}

impl NetworkMeter {
    /// Returns each interface's rates over the same interval as the host total it published.
    fn tick(&mut self, publisher: &Publisher) -> &HashMap<String, Network> {
        let failed = "network counters unavailable, panel disabled";
        match unless_failed(&mut self.unavailable, failed, net_stats::read) {
            Some(reading) => {
                if let Some(rates) = self.note(reading, Instant::now()) {
                    self.publish(publisher, rates);
                }
            }
            None => self.give_up(),
        }
        &self.interface_rates
    }

    /// The host total since the last reading, and each interface's rates over the same interval.
    fn note(&mut self, current: NetReading, now: Instant) -> Option<Network> {
        let previous = self.last.take();
        let rates = previous.as_ref().and_then(|(before, sampled_at)| {
            let elapsed = now.duration_since(*sampled_at);
            self.interface_rates =
                interface_rates(&before.interfaces, &current.interfaces, elapsed);
            network_rates(
                (before.total, before.counted),
                (current.total, current.counted),
                elapsed,
            )
        });
        self.last = Some((current, now));
        rates
    }

    /// For good, with no interface rates left to be shown as current.
    fn give_up(&mut self) {
        self.unavailable = true;
        self.interface_rates.clear();
    }

    fn publish(&mut self, publisher: &Publisher, rates: Network) {
        publisher.publish(TOPIC_SUMMARY, "network", &rates);

        let sample = NetworkSample {
            timestamp_nanos: system_time_nanos(SystemTime::now()),
            rates,
        };
        publisher.publish_ephemeral(TOPIC_SUMMARY, "network_sample", &sample);

        push_history(
            &mut self.history,
            sample,
            CHART_HISTORY,
            publisher,
            "network_history",
        );
    }
}

/// Bytes a second between two readings; none where a counter went backwards.
fn rates(before: NetCounters, after: NetCounters, seconds: f64) -> Option<Network> {
    // Counters are unsigned and wrap or reset when an interface goes down,
    // so a decrease is discarded rather than read as negative throughput.
    let received = after.received.checked_sub(before.received)?;
    let sent = after.sent.checked_sub(before.sent)?;
    Some(Network {
        received_per_second: (received as f64 / seconds) as u64,
        sent_per_second: (sent as f64 / seconds) as u64,
    })
}

/// The host total's rates since the previous reading; none where a counter went backwards or the
/// reading switched between the cards and every interface, as when the cards vanish.
fn network_rates(
    (previous, previous_counted): (NetCounters, Counted),
    (current, counted): (NetCounters, Counted),
    elapsed: Duration,
) -> Option<Network> {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 || counted != previous_counted {
        return None;
    }
    rates(previous, current, seconds)
}

/// Each interface's kind, state and routes are read this often. Its traffic comes every second,
/// with the host total.
const INTERFACE_INTERVAL: Duration = Duration::from_secs(5);

/// How far back an interface's highest route count reaches.
const ROUTE_PEAK_HOURS: u64 = 24;

#[derive(Default)]
struct InterfaceMeter {
    started: Option<Instant>,
    /// The last reading of each interface, with its highest route count of the last day.
    described: Option<(Instant, Vec<(InterfaceReading, u32)>)>,
    /// Per interface, its highest route count in each hour since the meter started, oldest first.
    peaks: HashMap<String, VecDeque<(u64, u32)>>,
    unavailable: bool,
}

impl InterfaceMeter {
    /// Published every second with the host total's interface rates, so the two agree.
    fn tick(&mut self, publisher: &Publisher, rates: &HashMap<String, Network>) {
        if self.unavailable {
            return;
        }
        let now = Instant::now();
        if self
            .described
            .as_ref()
            .is_none_or(|(at, _)| now.duration_since(*at) >= INTERFACE_INTERVAL)
        {
            let failed = "network interfaces unavailable";
            let Some(readings) = unless_failed(&mut self.unavailable, failed, net_ifaces::read)
            else {
                return;
            };
            self.described = Some((now, self.note_peaks(now, readings)));
        }
        let Some((_, described)) = &self.described else {
            return;
        };
        let interfaces: Vec<NetInterface> = described
            .iter()
            .filter_map(|(reading, peak)| {
                interface_shown(reading, *peak, rates.get(&reading.name).copied())
            })
            .collect();
        publisher.publish(TOPIC_SUMMARY, "net_interfaces", &interfaces);
    }

    fn note_peaks(
        &mut self,
        now: Instant,
        readings: Vec<InterfaceReading>,
    ) -> Vec<(InterfaceReading, u32)> {
        let hour = now
            .duration_since(*self.started.get_or_insert(now))
            .as_secs()
            .checked_div(3600)
            .unwrap_or_default();
        // Interfaces come and go, as containers' do; only those present keep a history.
        self.peaks
            .retain(|name, _| readings.iter().any(|reading| &reading.name == name));
        readings
            .into_iter()
            .map(|reading| {
                let peak = note_routes(
                    self.peaks.entry(reading.name.clone()).or_default(),
                    hour,
                    reading.routes,
                );
                (reading, peak)
            })
            .collect()
    }
}

/// Records a reading's route count against its hour and returns the highest of the last day.
fn note_routes(hours: &mut VecDeque<(u64, u32)>, hour: u64, routes: u32) -> u32 {
    match hours.back_mut() {
        Some((at, most)) if *at == hour => *most = (*most).max(routes),
        _ => hours.push_back((hour, routes)),
    }
    while hours
        .front()
        .is_some_and(|(at, _)| at.saturating_add(ROUTE_PEAK_HOURS) <= hour)
    {
        hours.pop_front();
    }
    hours.iter().map(|(_, most)| *most).max().unwrap_or(routes)
}

/// Each interface's bytes a second in and out between two readings; none for an interface new
/// since the earlier one or whose counter went backwards.
fn interface_rates(
    before: &InterfaceCounters,
    after: &InterfaceCounters,
    elapsed: Duration,
) -> HashMap<String, Network> {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 {
        return HashMap::new();
    }
    after
        .iter()
        .filter_map(|(name, counters)| {
            Some((name.clone(), rates(*before.get(name)?, *counters, seconds)?))
        })
        .collect()
}

/// `None` for an idle interface: down, with no routes and no traffic.
fn interface_shown(
    reading: &InterfaceReading,
    routes_peak: u32,
    rates: Option<Network>,
) -> Option<NetInterface> {
    let quiet =
        rates.is_none_or(|rates| rates.received_per_second == 0 && rates.sent_per_second == 0);
    if !reading.up && reading.routes == 0 && quiet {
        return None;
    }
    Some(NetInterface {
        reading: reading.clone(),
        routes_peak,
        received_per_second: rates.map(|rates| rates.received_per_second),
        sent_per_second: rates.map(|rates| rates.sent_per_second),
    })
}

#[derive(Default)]
struct HostMeter {
    last: Option<(HostSnapshot, Instant)>,
    unavailable: bool,
    disks: Option<(Vec<HostPath>, DiskLayout, Option<String>)>,
    resident: VecDeque<(Instant, u64)>,
}

const RESIDENT_HISTORY: Duration = Duration::from_secs(3600);
const RESIDENT_HOUR_AGO: Duration = Duration::from_secs(3300);

impl HostMeter {
    fn tick(&mut self, host: &HostPaths, pid: u32, tap: &MetricsTap, publisher: &Publisher) {
        let failed = "host counters unavailable, panel disabled";
        let Some(current) = unless_failed(&mut self.unavailable, failed, host_stats::read) else {
            return;
        };
        let now = Instant::now();
        let (paths, layout, snapshot_device) =
            self.disks.get_or_insert_with(|| resolve_disks(host));
        let process_memory = host_stats::process_memory_of(pid).ok();
        let process_resident = process_memory.map(|memory| memory.resident);
        if let Some(resident) = process_resident {
            self.resident.push_back((now, resident));
        }
        while self
            .resident
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > RESIDENT_HISTORY)
        {
            self.resident.pop_front();
        }
        let process_resident_hour_ago = self
            .resident
            .front()
            .filter(|(at, _)| now.duration_since(*at) >= RESIDENT_HOUR_AGO)
            .map(|(_, resident)| *resident);

        let Some((previous, sampled_at)) = self.last.take() else {
            self.last = Some((current, now));
            return;
        };
        let interval_ms = now.duration_since(sampled_at).as_secs_f64() * 1000.0;
        if interval_ms <= 0.0 || now.duration_since(sampled_at) > WATCH_GAP {
            self.last = Some((current, now));
            return;
        }
        let seconds = interval_ms / 1000.0;

        let swap_used = current
            .memory
            .swap_total
            .saturating_sub(current.memory.swap_free);
        let readings = DiskReadings {
            ledger: tap.ledger_files(),
            accounts: tap.accounts_files(),
            archives: snapshot::archive_bytes(&host.snapshot_config),
        };
        let leftovers = read_leftovers(layout);
        let host = Host {
            cores: num_cpus_of(pid),
            load_one: current.load.one,
            load_five: current.load.five,
            load_fifteen: current.load.fifteen,
            threads: current.load.threads,
            running: current.load.running,
            cpu: current
                .cpu
                .zip(previous.cpu)
                .and_then(|(now, before)| now.since(&before)?.shares()),
            cpu_governor: host_stats::cpu_governor(),
            memory_total: current.memory.total,
            memory_available: current.memory.available,
            memory_reclaimable: current.memory.reclaimable,
            memory_free: current.memory.free,
            swap: (current.memory.swap_total > 0).then_some(Swap {
                total: current.memory.swap_total,
                used: swap_used,
            }),
            process_resident,
            process_resident_hour_ago,
            process_kinds: process_memory.and_then(|memory| memory.kinds),
            memory_parts: tap.memory_parts(),
            snapshot_device: snapshot_device.clone(),
            filesystems: paths
                .iter()
                .filter_map(|path| {
                    Some(FilesystemUsage {
                        name: path.name.clone(),
                        path: path.path.to_string_lossy().into_owned(),
                        usage: host_stats::filesystem(&path.path).ok()?,
                        parts: disk_parts(path.filesystem, layout, &readings),
                        leftovers: leftovers
                            .iter()
                            .filter(|(filesystem, _)| *filesystem == path.filesystem)
                            .map(|(_, name)| name.clone())
                            .collect(),
                    })
                })
                .collect(),
            devices: device_loads(paths, &current, &previous, interval_ms, seconds),
        };
        self.last = Some((current, now));
        publisher.publish(TOPIC_SUMMARY, "host", &host);
    }
}

#[derive(Default)]
struct ThreadMeter {
    last: Option<(HashMap<u64, ThreadReading>, Instant)>,
    pinning: HashMap<u64, Option<BTreeSet<usize>>>,
    recent: VecDeque<Vec<(String, f64)>>,
    named_at: Option<Instant>,
    history: Vec<ThreadsSample>,
    unavailable: bool,
}

impl ThreadMeter {
    fn tick(&mut self, pid: u32, publisher: &Publisher) {
        let refresh_names = self
            .named_at
            .is_none_or(|at| at.elapsed() >= THREAD_NAMES_REFRESH);
        let known = if refresh_names {
            None
        } else {
            self.last.as_ref().map(|(by_tid, _)| by_tid)
        };
        let failed = "thread counters unavailable, panel disabled";
        let Some(current) = unless_failed(&mut self.unavailable, failed, || {
            thread_stats::read_of(pid, known)
        }) else {
            return;
        };
        if refresh_names {
            self.named_at = Some(Instant::now());
        }
        let cores = num_cpus_of(pid);
        for thread in &current {
            self.pinning.entry(thread.tid).or_insert_with(|| {
                thread_stats::cores_allowed(thread.tid).filter(|allowed| allowed.len() < cores)
            });
        }

        let now = Instant::now();
        // Resumed, it would average the gap into one second beside a minute long gone.
        if self
            .last
            .as_ref()
            .is_some_and(|(_, at)| now.duration_since(*at) > WATCH_GAP)
        {
            self.last = None;
            self.recent.clear();
            self.history.clear();
            publisher.publish(TOPIC_SUMMARY, "threads_history", &self.history);
        }
        let threads = current.len();
        let groups = self.last.take().and_then(|(previous, sampled_at)| {
            let interval = now.duration_since(sampled_at).as_nanos() as u64;
            (interval > 0)
                .then(|| thread_stats::group_shares(&previous, &current, &self.pinning, interval))
        });
        let by_tid: HashMap<u64, ThreadReading> = current
            .into_iter()
            .map(|thread| (thread.tid, thread))
            .collect();
        self.pinning.retain(|tid, _| by_tid.contains_key(tid));
        self.last = Some((by_tid, now));
        let Some(groups) = groups else {
            return;
        };

        let shares = groups
            .iter()
            .map(|group| (group.name.clone(), group.on_cpu))
            .collect();
        push_capped(&mut self.recent, shares, THREADS_HISTORY);
        let means = thread_stats::window_means(&self.recent);
        let sample = ThreadsSample {
            timestamp_nanos: system_time_nanos(SystemTime::now()),
            threads,
            groups: thread_stats::select_rows(groups, &means, THREAD_ROWS),
        };
        publisher.publish_ephemeral(TOPIC_SUMMARY, "threads_sample", &sample);
        push_history(
            &mut self.history,
            sample,
            THREADS_HISTORY,
            publisher,
            "threads_history",
        );
    }
}

debounced_keys! {
    struct SocketMeter {
        published: IngestSummary = (TOPIC_SUMMARY, "ingest_paths"),
        ;
        drops_window: PortWindow = PortWindow::new(DROPS_WINDOW),
        quic_drops_window: PortWindow =
            PortWindow::new(Duration::from_secs(WATERFALL_WINDOW as u64)),
        kernel_drops: HashMap<&'static str, u64> = HashMap::new(),
        /// Per-port drops when the validator finished starting, so the startup burst is not
        /// carried for the life of the process.
        drops_baseline: Option<HashMap<u16, u64>> = None,
        received_window: PortWindow = PortWindow::new(DROPS_WINDOW),
        received_baseline: Option<HashMap<u16, u64>> = None,
        /// Last counters seen per reported port: `/proc/net/udp` is not read atomically, so a
        /// bound socket can drop out of one snapshot.
        known_sockets: HashMap<u16, PortCounters> = HashMap::new(),
        unavailable: bool = false,
        /// Set once the table has been read, so an empty table before then does not say the TPU
        /// port is bound elsewhere.
        sampled: bool = false,
    }
}

impl SocketMeter {
    fn tick(
        &mut self,
        addresses: Option<IngestAddresses>,
        tap: &TapCounters,
        running: bool,
        alpenglow: bool,
        publisher: &Publisher,
    ) {
        let Some(addresses) = addresses else {
            return;
        };
        let failed = "socket counters unavailable, panel disabled";
        let Some(current) = unless_failed(&mut self.unavailable, failed, udp_drops::read) else {
            // Emptied: this latches, and a reading kept here would show on the TPU path
            // card as current for the rest of the process.
            self.kernel_drops.clear();
            return;
        };
        self.sampled = true;
        let now = Instant::now();
        let ports = ingest_ports_at(&addresses, tap, alpenglow);

        for port in &ports {
            if let Some(counters) = current.get(&port.port) {
                self.known_sockets.insert(port.port, *counters);
            }
        }

        let drops: HashMap<u16, u64> = ports
            .iter()
            .filter_map(|port| Some((port.port, self.known_sockets.get(&port.port)?.drops)))
            .collect();
        let received: HashMap<u16, u64> = ports
            .iter()
            .filter_map(|port| Some((port.port, port.received?)))
            .collect();

        if self.drops_baseline.is_none() && running {
            self.drops_baseline = Some(drops.clone());
            self.received_baseline = Some(received.clone());
        }
        self.drops_window.push(now, drops.clone());
        self.received_window.push(now, received);
        self.quic_drops_window.push(now, drops);
        let kernel_drops: HashMap<&'static str, u64> = ports
            .iter()
            .filter(|port| port.quic)
            .filter_map(|port| {
                let counters = self.known_sockets.get(&port.port)?;
                Some((
                    port.name,
                    self.quic_drops_window.since(port.port, counters.drops),
                ))
            })
            .collect();
        self.kernel_drops = kernel_drops;

        let paths: Vec<IngestPath> = ports
            .iter()
            .filter_map(|port| {
                let counters = self.known_sockets.get(&port.port)?;
                let dropped_by = at_baseline(self.drops_baseline.as_ref(), port.port);
                let received_by = at_baseline(self.received_baseline.as_ref(), port.port);
                Some(IngestPath {
                    name: port.name,
                    port: port.port,
                    drops_recent: self.drops_window.since(port.port, counters.drops),
                    // Saturating rather than wrapping: a socket rebound after
                    // the baseline was taken restarts below it.
                    drops_total: counters.drops.saturating_sub(dropped_by),
                    queued_bytes: counters.queued,
                    received_recent: port
                        .received
                        .map(|total| self.received_window.since(port.port, total)),
                    received_total: port.received.map(|total| total.saturating_sub(received_by)),
                    received_in_packets: true,
                    quic: port.quic,
                })
            })
            .collect();

        // Empty means none of the advertised ports is bound here, as behind a port
        // forward. Zeroed rows would report healthy on a failed lookup.
        if paths.is_empty() {
            return;
        }
        let summary = IngestSummary {
            window_seconds: self
                .drops_window
                .covers(now)
                .min(DROPS_WINDOW)
                .as_secs_f64(),
            paths,
        };
        self.published.publish(publisher, summary);
    }
}

fn ingest_ports_at(info: &IngestAddresses, tap: &TapCounters, alpenglow: bool) -> Vec<IngestPort> {
    let port = |name, addr: Option<SocketAddr>, received, quic| {
        Some(IngestPort {
            name,
            port: addr?.port(),
            received,
            quic,
        })
    };
    let network = &tap.network;
    [
        // Everything arriving on the TVU port is a shred, so the count the
        // shred receiver keeps is the count of what the port delivered.
        port("turbine", info.tvu_udp, Some(network.shreds_turbine), false),
        port("tpu", info.tpu_quic, None, true),
        port("tpu forwards", info.tpu_forwards_quic, None, true),
        // Under alpenglow votes no longer arrive on it.
        port(
            "tpu vote",
            info.tpu_vote_udp.filter(|_| !alpenglow),
            Some(network.packets_tpu_vote),
            false,
        ),
        port("gossip", info.gossip, Some(network.packets_gossip), false),
        // The one port that could have a count and does not: its receiver keeps
        // counters nothing reports. Reaching them means a change to `core`.
        port("serve repair", info.serve_repair_udp, None, false),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// In bytes per second; `None` until a sender has reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
struct EgressSplit {
    gossip_per_second: Option<u64>,
    repair_per_second: Option<u64>,
}

debounced_keys! {
    /// Shreds received and repaired, turbine's layers, and the bytes gossip and repair sent.
    struct TrafficMeter {
        shreds: Option<Shreds> = (TOPIC_SUMMARY, "shreds"),
        turbine: Option<Turbine> = (TOPIC_SUMMARY, "turbine"),
        egress: EgressSplit = (TOPIC_SUMMARY, "network_egress"),
        ;
        window: VecDeque<NetworkTotals> = VecDeque::with_capacity(SHREDS_WINDOW),
    }
}

impl TrafficMeter {
    fn tick(&mut self, previous: &TapCounters, current: &TapCounters, publisher: &Publisher) {
        let sample = current.network.since(&previous.network);
        let sum = sum_window(&mut self.window, sample, SHREDS_WINDOW);
        let repaired = sum.shreds_repair;
        let received = sum.shreds_turbine.saturating_add(repaired);
        let shreds = (received > 0).then(|| Shreds {
            received,
            repaired,
            repair_rate: repaired as f64 / received as f64,
        });
        self.shreds.publish(publisher, shreds);

        let turbine = Turbine {
            root: sum.turbine_root,
            layer_1: sum.turbine_layer_1,
            layer_2: sum.turbine_layer_2,
            layer_3: sum.turbine_layer_3,
            xdp_dropped: sum.xdp_dropped,
            xdp: current.retransmit_xdp,
        };
        let reported = turbine.xdp.is_some()
            || turbine
                .root
                .saturating_add(turbine.layer_1)
                .saturating_add(turbine.layer_2)
                .saturating_add(turbine.layer_3)
                > 0;
        self.turbine.publish(publisher, reported.then_some(turbine));

        let last = self.egress.last().copied().unwrap_or_default();
        let rate = |bytes: u64, millis: u64, was: Option<u64>| {
            bytes.saturating_mul(1000).checked_div(millis).or(was)
        };
        let egress = EgressSplit {
            gossip_per_second: rate(
                sample.gossip_sent_bytes,
                sample.gossip_sent_millis,
                last.gossip_per_second,
            ),
            repair_per_second: rate(
                sample.repair_sent_bytes,
                sample.repair_sent_millis,
                last.repair_per_second,
            ),
        };
        self.egress.publish(publisher, egress);
    }
}

debounced_keys! {
    struct AccountsMeter {
        published: Option<AccountsCache> = (TOPIC_SUMMARY, "accounts_cache"),
        upkeep: Option<AccountsDb> = (TOPIC_SUMMARY, "accounts_db"),
        ;
        window: VecDeque<AccountsTotals> = VecDeque::with_capacity(ACCOUNTS_CACHE_WINDOW),
        upkeep_window: VecDeque<AccountsUpkeepTotals> =
            VecDeque::with_capacity(ACCOUNTS_CACHE_WINDOW),
    }
}

impl AccountsMeter {
    fn tick(&mut self, previous: &TapCounters, current: &TapCounters, publisher: &Publisher) {
        let totals = sum_window(
            &mut self.window,
            current.accounts.totals.since(&previous.accounts.totals),
            ACCOUNTS_CACHE_WINDOW,
        );
        // What the window actually spans, not what it will span once full. A
        // rate taken against the full minute would read low for the first one.
        let window_seconds = self.window.len() as f64 * METER_INTERVAL.as_secs_f64();

        let levels = current.accounts.levels;
        let disk = (levels.storage_bytes > 0).then(|| AccountsDisk {
            used: levels.storage_alive_bytes,
            allocated: levels.storage_bytes,
            fragmented: levels
                .storage_bytes
                .saturating_sub(levels.storage_alive_bytes),
            storages: levels.storage_count,
        });

        let rate = cache_rate(&totals).map(|(read, hit_rate)| AccountsCache {
            read,
            hit_rate,
            evictions: totals.cache_evicts,
            cache_bytes: levels.cache_bytes,
            cache_entries: levels.cache_entries,
            from_write_cache: totals.loaded_from_write_cache,
            from_read_cache: totals.loaded_from_read_cache,
            from_storage: totals.loaded_from_storage,
            stored_accounts: totals.stored_accounts,
            stored_bytes: totals.stored_bytes,
            window_seconds,
            disk,
        });
        self.published.publish(publisher, rate);

        let totals = sum_window(
            &mut self.upkeep_window,
            current
                .accounts_upkeep
                .totals
                .since(&previous.accounts_upkeep.totals),
            ACCOUNTS_CACHE_WINDOW,
        );
        let levels = current.accounts_upkeep.levels;
        // The index reports first, ten seconds after the validator has started.
        let upkeep = (levels.index_entries > 0).then_some(AccountsDb {
            levels,
            totals,
            window_seconds,
        });
        self.upkeep.publish(publisher, upkeep);
    }
}

debounced_keys! {
    struct ProgramCacheMeter {
        published: Option<ProgramCache> = (TOPIC_SUMMARY, "program_cache"),
        ;
        window: VecDeque<ProgramCacheTotals> = VecDeque::with_capacity(PROGRAM_CACHE_WINDOW),
        levels: VecDeque<u64> = VecDeque::with_capacity(PROGRAM_CACHE_WINDOW),
    }
}

impl ProgramCacheMeter {
    fn tick(&mut self, previous: &TapCounters, current: &TapCounters, publisher: &Publisher) {
        let sample = current
            .program_cache
            .totals
            .since(&previous.program_cache.totals);
        let totals = sum_window(&mut self.window, sample, PROGRAM_CACHE_WINDOW);

        // The level is not differenced: it is where the cache stood, not what
        // happened, so it is kept as its own window and read as a peak.
        let level = current.program_cache.water_level;
        push_capped(&mut self.levels, level, PROGRAM_CACHE_WINDOW);
        let peak_entries = self.levels.iter().copied().max().filter(|peak| *peak > 0);

        let looked_up = totals.hits.saturating_add(totals.misses);
        let cache = (looked_up > 0).then(|| ProgramCache {
            looked_up,
            totals,
            hit_rate: totals.hits as f64 / looked_up as f64,
            peak_entries,
            entry_limit: MAX_LOADED_ENTRY_COUNT as u64,
        });
        self.published.publish(publisher, cache);
    }
}

debounced_keys! {
    struct TpuMeter {
        quic_paths: Option<QuicPaths> = (TOPIC_SUMMARY, "quic_paths"),
        xdp: Option<XdpConfig> = (TOPIC_SUMMARY, "xdp"),
        epoch_span: Option<EpochSpan> = (TOPIC_SUMMARY, "epoch_span"),
        verify: Option<VerifyTotals> = (TOPIC_SUMMARY, "verify"),
        executed: Option<ExecutedTotals> = (TOPIC_SUMMARY, "executed"),
        bundles: Option<BundleTotals> = (TOPIC_SUMMARY, "bundles"),
        replay: Option<ReplayWindow> = (TOPIC_SUMMARY, "replay"),
        ;
        quic_windows: [VecDeque<QuicTotals>; 3] =
            std::array::from_fn(|_| VecDeque::with_capacity(WATERFALL_WINDOW)),
        /// The vote port is listed from its first connection until an epoch
        /// passes without another: under alpenglow votes leave it for good.
        vote_quiet: bool = true,
        vote_offered_at_epoch: Option<(Epoch, u64)> = None,
        epoch_now: Option<WorkingReading> = None,
        leader_totals: LeaderTotals = LeaderTotals::default(),
        slot_lists_seen: Option<u64> = None,
    }
}

impl TpuMeter {
    fn tick(
        &mut self,
        tap: &MetricsTap,
        previous: &TapCounters,
        current: &TapCounters,
        kernel_drops: &HashMap<&'static str, u64>,
        tpu_offhost: bool,
        publisher: &Publisher,
    ) {
        if current.quic_vote.totals.offered > previous.quic_vote.totals.offered {
            self.vote_quiet = false;
        }
        if let Some(at) = self.epoch_now {
            let offered = current.quic_vote.totals.offered;
            match self.vote_offered_at_epoch {
                Some((epoch, before)) if epoch != at.epoch => {
                    self.vote_quiet = offered == before;
                    self.vote_offered_at_epoch = Some((at.epoch, offered));
                }
                Some(_) => {}
                None => self.vote_offered_at_epoch = Some((at.epoch, offered)),
            }
        }
        let vote_quiet = self.vote_quiet;

        // Present once any port has taken a connection, not within the window: behind a proxy the
        // only inbound QUIC is votes in leader slots.
        let quic = [current.quic, current.quic_forwards, current.quic_vote];
        let quic_before = [previous.quic, previous.quic_forwards, previous.quic_vote];
        let ports: Vec<QuicPort> = ["tpu", "tpu forwards", "tpu vote quic"]
            .into_iter()
            .zip(&mut self.quic_windows)
            .zip(quic.iter().zip(&quic_before))
            .map(|((name, window), (now, before))| QuicPort {
                name,
                counts: sum_window(window, now.totals.since(&before.totals), WATERFALL_WINDOW),
                levels: now.levels,
                kernel_drops: kernel_drops.get(name).copied(),
            })
            .filter(|port| port.name != "tpu vote quic" || !vote_quiet)
            .collect();
        let ever_offered = quic.iter().any(|port| port.totals.offered > 0);
        self.quic_paths.publish(
            publisher,
            ever_offered.then_some(QuicPaths { ports, tpu_offhost }),
        );

        // Summed over the epoch, not the window: these stages run only while
        // leader. Nothing is published until a bank has said which epoch.
        if let Some(at) = self.epoch_now {
            self.leader_totals.add(
                at,
                current.verify.since(&previous.verify),
                current.executed.since(&previous.executed),
                current.bundles.since(&previous.bundles),
            );
            let LeaderTotals {
                verify,
                executed,
                bundles,
                ..
            } = self.leader_totals;
            let span = self.leader_totals.span(at);

            self.epoch_span.publish(
                publisher,
                (verify.received > 0 || executed.attempted > 0 || bundles.received > 0)
                    .then_some(span),
            );
            self.verify
                .publish(publisher, (verify.received > 0).then_some(verify));
            self.executed
                .publish(publisher, (executed.attempted > 0).then_some(executed));
            self.bundles
                .publish(publisher, (bundles.received > 0).then_some(bundles));
        }

        // Joined by slot in the browser, since the produced block is captured on another thread.
        // The newest few are kept for a connecting client; a connected one is sent each slot that
        // changed. Read only when the tap's revision says a list moved.
        let revision = tap.slot_lists_revision();
        if self.slot_lists_seen != Some(revision) {
            self.slot_lists_seen = Some(revision);
            publisher.retain_only(
                TOPIC_SUMMARY,
                "slot_waterfalls",
                &tap.recent_waterfalls(SLOT_LISTS_SENT),
            );
            publisher.retain_only(
                TOPIC_SUMMARY,
                "slot_costs",
                &tap.recent_costs(SLOT_LISTS_SENT),
            );
            for waterfall in tap.take_changed_waterfalls() {
                publisher.publish_update(
                    TOPIC_SUMMARY,
                    "slot_waterfall",
                    waterfall.slot,
                    &waterfall,
                );
            }
            for cost in tap.take_changed_costs() {
                publisher.publish_update(TOPIC_SUMMARY, "slot_cost", cost.slot, &cost);
            }
        }

        self.replay
            .publish(publisher, replay_window(&tap.replay_slots()));
    }
}

/// What `read` gives, until it fails: the failure is logged once as `failed`, and from then on
/// nothing is read.
fn unless_failed<T>(
    unavailable: &mut bool,
    failed: &str,
    read: impl FnOnce() -> std::io::Result<T>,
) -> Option<T> {
    if *unavailable {
        return None;
    }
    read()
        .inspect_err(|err| {
            *unavailable = true;
            log::info!("dashboard: {failed}: {err}");
        })
        .ok()
}

fn sum_window<T: WindowedCounters>(window: &mut VecDeque<T>, sample: T, span: usize) -> T {
    push_capped(window, sample, span);
    window
        .iter()
        .fold(T::default(), |total, sample| total.plus(sample))
}

fn at_baseline(baseline: Option<&HashMap<u16, u64>>, port: u16) -> u64 {
    baseline
        .and_then(|baseline| baseline.get(&port))
        .copied()
        .unwrap_or(0)
}

fn push_history<T: Serialize>(
    history: &mut Vec<T>,
    sample: T,
    keep: usize,
    publisher: &Publisher,
    key: &'static str,
) {
    history.push(sample);
    history.drain(..history.len().saturating_sub(keep));
    publisher.retain_only(TOPIC_SUMMARY, key, history);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{collect::FAILED_UNREAD, net_ifaces::InterfaceKind, remote::Remote},
        agave_snapshots::snapshot_config::SnapshotConfig,
        solana_metrics::datapoint::DataPoint,
        std::{net::Ipv4Addr, sync::RwLock, thread::sleep, time::UNIX_EPOCH},
    };

    /// Every port a validator advertises.
    fn advertised() -> IngestAddresses {
        let at = |port| Some(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
        IngestAddresses {
            tvu_udp: at(8001),
            tpu_quic: at(8002),
            tpu_forwards_quic: at(8003),
            tpu_vote_udp: at(8004),
            gossip: at(8007),
            serve_repair_udp: at(8008),
        }
    }

    /// The validator as the meters read it; a test moves its working bank.
    struct Fixture {
        remote: Shared,
        publisher: Arc<Publisher>,
        failed_total: Arc<AtomicU64>,
    }

    fn fixture() -> Fixture {
        let remote = Remote {
            working: Some(at(0, 0)),
            ingest: Some(advertised()),
            progress: Some(ValidatorStartProgress::Running),
            started: Some(UNIX_EPOCH),
            ..Remote::default()
        };
        Fixture {
            remote: Arc::new(RwLock::new(remote)),
            publisher: Arc::new(Publisher::new()),
            failed_total: Arc::new(AtomicU64::new(FAILED_UNREAD)),
        }
    }

    impl Fixture {
        /// With a tap of its own rather than the process-wide one, which carries whatever the
        /// rest of the suite measured.
        fn meters(&self) -> Meters {
            let host_paths = HostPaths {
                ledger: PathBuf::from("/nonexistent"),
                accounts: Vec::new(),
                snapshot_config: SnapshotConfig::default(),
            };
            Meters::new(
                self.remote.clone(),
                host_paths,
                std::process::id(),
                self.publisher.clone(),
                Arc::new(MetricsTap::default()),
                self.failed_total.clone(),
            )
        }

        fn published_key(&self, topic: &str, key: &str) -> Option<String> {
            let needle = format!(r#""topic":"{topic}","key":"{key}""#);
            self.publisher
                .snapshot()
                .iter()
                .map(|message| message.to_string())
                .find(|message| message.contains(&needle))
        }

        /// The value retained under `summary.{key}`.
        fn published_value(&self, key: &str) -> Option<serde_json::Value> {
            let message = self.published_key("summary", key)?;
            let mut message: serde_json::Value = serde_json::from_str(&message).unwrap();
            Some(message["value"].take())
        }

        /// The working bank moves to `slot`, having processed nothing on the way.
        fn advance_to(&self, slot: Slot) -> WorkingReading {
            let mut remote = self.remote.write().unwrap();
            let working = remote.working.as_mut().unwrap();
            working.slot = slot;
            *working
        }
    }

    /// The newest sample of the retained chart series.
    fn last_tps(harness: &Fixture) -> Option<serde_json::Value> {
        harness
            .published_value("tps_history")?
            .as_array()?
            .last()
            .cloned()
    }

    /// The newest throughput sample after a tick, then another `apart` later with the working bank
    /// at `slot` and the failed count moved from `failed.0` to `failed.1`.
    fn rate_after(failed: (u64, u64), slot: Slot, apart: Duration) -> Option<serde_json::Value> {
        let harness = fixture();
        let mut meters = harness.meters();
        harness.failed_total.store(failed.0, Ordering::Relaxed);
        meters.tick();
        assert_eq!(
            last_tps(&harness),
            None,
            "a rate was reported from one reading"
        );
        sleep(apart);
        harness.advance_to(slot);
        harness.failed_total.store(failed.1, Ordering::Relaxed);
        meters.tick();
        last_tps(&harness)
    }

    fn quic_tap(offered: u64) -> TapCounters {
        let mut counters = TapCounters::default();
        counters.quic.totals.offered = offered;
        counters
    }

    const LEDGER_DISK: u64 = 1;
    const ACCOUNTS_DISK: u64 = 2;

    fn disk_readings() -> DiskReadings {
        DiskReadings {
            ledger: Some(LedgerFiles {
                shreds: 10,
                history: 20,
                metadata: 30,
            }),
            accounts: Some(AccountsFiles {
                live: 40,
                fragmented: 50,
            }),
            archives: Some(ArchiveBytes {
                full: 60,
                incremental: 70,
            }),
        }
    }

    fn split(parts: &[DiskPart]) -> Vec<(DiskPartKey, u64)> {
        parts.iter().map(|part| (part.key, part.bytes)).collect()
    }

    #[test]
    fn test_disk_parts_land_on_the_filesystem_each_lives_on() {
        use DiskPartKey::*;
        let layout = DiskLayout {
            ledger: Some(LEDGER_DISK),
            accounts: Some(ACCOUNTS_DISK),
            full: Some(LEDGER_DISK),
            incremental: Some(ACCOUNTS_DISK),
            ..DiskLayout::default()
        };
        let readings = disk_readings();
        assert_eq!(
            split(&disk_parts(LEDGER_DISK, &layout, &readings)),
            [(Shreds, 10), (History, 20), (Metadata, 30), (Full, 60)]
        );
        assert_eq!(
            split(&disk_parts(ACCOUNTS_DISK, &layout, &readings)),
            [(Live, 40), (Fragmented, 50), (Incremental, 70)]
        );
    }

    #[test]
    fn test_storage_figures_name_no_filesystem_across_several_accounts_paths() {
        let layout = DiskLayout {
            ledger: Some(LEDGER_DISK),
            accounts: None,
            ..DiskLayout::default()
        };
        assert_eq!(
            split(&disk_parts(ACCOUNTS_DISK, &layout, &disk_readings())),
            []
        );
    }

    #[test]
    fn test_a_part_not_yet_reported_is_left_out() {
        let layout = DiskLayout {
            ledger: Some(LEDGER_DISK),
            accounts: Some(LEDGER_DISK),
            full: Some(LEDGER_DISK),
            incremental: Some(LEDGER_DISK),
            ..DiskLayout::default()
        };
        assert_eq!(
            disk_parts(LEDGER_DISK, &layout, &DiskReadings::default()),
            []
        );
    }

    #[test]
    fn test_the_ledger_row_is_the_blockstores_filesystem() {
        let ledger = tempfile::TempDir::new().unwrap();
        let host = HostPaths {
            ledger: ledger.path().to_path_buf(),
            accounts: Vec::new(),
            snapshot_config: SnapshotConfig::default(),
        };
        let row = |host: &HostPaths| {
            let (paths, layout, _) = resolve_disks(host);
            assert_eq!(
                layout.ledger,
                Some(paths[0].filesystem),
                "its split is drawn on it"
            );
            (
                paths[0].name.clone(),
                paths[0].path.clone(),
                paths[0].filesystem,
            )
        };
        // On the ledger's own filesystem it is named as the ledger, made yet or not.
        let id = host_stats::filesystem_id(&host.ledger).unwrap();
        let named = ("ledger".to_owned(), host.ledger.clone(), id);
        assert_eq!(row(&host), named);
        let rocksdb = host.ledger.join(BLOCKSTORE_DIRECTORY_ROCKS_LEVEL);
        std::fs::create_dir(&rocksdb).unwrap();
        assert_eq!(row(&host), named);
        // `/dev` stands in for a mount of its own.
        std::fs::remove_dir(&rocksdb).unwrap();
        std::os::unix::fs::symlink("/dev", &rocksdb).unwrap();
        let mount = host_stats::filesystem_id(&rocksdb).unwrap();
        if mount != id {
            assert_eq!(row(&host), ("ledger".to_owned(), rocksdb, mount));
        }
    }

    #[test]
    fn test_only_directories_named_for_deletion_are_leftovers() {
        let dir = tempfile::TempDir::new().unwrap();
        for name in [
            "run",
            "snapshot",
            "run_to_be_deleted",
            "snapshot_to_be_deleted",
        ] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
        }
        let layout = DiskLayout {
            account_parents: vec![
                (ACCOUNTS_DISK, dir.path().to_path_buf()),
                (ACCOUNTS_DISK, dir.path().join("gone")),
            ],
            ..DiskLayout::default()
        };
        assert_eq!(
            read_leftovers(&layout),
            [
                (ACCOUNTS_DISK, "run_to_be_deleted".to_owned()),
                (ACCOUNTS_DISK, "snapshot_to_be_deleted".to_owned()),
            ]
        );
    }

    #[test]
    fn test_the_route_peak_is_the_highest_hour_of_the_last_day() {
        let mut hours = VecDeque::new();
        assert_eq!(note_routes(&mut hours, 0, 115), 115);
        assert_eq!(
            note_routes(&mut hours, 0, 2),
            115,
            "a wipe within the hour keeps its high"
        );
        assert_eq!(note_routes(&mut hours, 23, 2), 115);
        assert_eq!(
            note_routes(&mut hours, 24, 2),
            2,
            "a day later the hour that saw 115 has gone"
        );
        assert_eq!(hours.len(), 2);
    }

    fn reading(name: &str, up: bool, routes: u32) -> InterfaceReading {
        InterfaceReading {
            name: name.to_string(),
            kind: InterfaceKind::Physical,
            up,
            mtu: Some(1500),
            routes,
            member_of: None,
            inside: None,
        }
    }

    fn counters(received: u64, sent: u64) -> NetCounters {
        NetCounters { received, sent }
    }

    fn per_second(received_per_second: u64, sent_per_second: u64) -> Network {
        Network {
            received_per_second,
            sent_per_second,
        }
    }

    #[test]
    fn test_rates_per_interface_and_none_for_a_counter_that_went_backwards() {
        let before = HashMap::from([
            ("eno1".to_string(), counters(1_000, 2_000)),
            ("eno2".to_string(), counters(9_000, 9_000)),
        ]);
        let after = HashMap::from([
            ("eno1".to_string(), counters(6_000, 12_000)),
            ("eno2".to_string(), counters(500, 9_000)),
            ("doublezero0".to_string(), counters(1, 1)),
        ]);
        let rates = interface_rates(&before, &after, Duration::from_secs(5));
        assert_eq!(
            rates,
            HashMap::from([("eno1".to_string(), per_second(1_000, 2_000))])
        );
        assert!(interface_rates(&before, &after, Duration::ZERO).is_empty());
    }

    #[test]
    fn test_the_interfaces_get_their_rates_from_the_readings_the_host_total_used() {
        // One card carries everything, so its rate and the host's must match at every reading.
        let at = |received: u64, sent: u64, tunnel: u64, counted| NetReading {
            total: counters(received, sent),
            counted,
            interfaces: HashMap::from([
                ("eno1".to_string(), counters(received, sent)),
                ("doublezero0".to_string(), counters(tunnel, tunnel)),
            ]),
        };
        let mut meter = NetworkMeter::default();
        let start = Instant::now();
        assert_eq!(
            meter.note(at(10_000, 40_000, 7, Counted::Cards), start),
            None
        );
        assert!(
            meter.interface_rates.is_empty(),
            "nothing to compare a first reading with"
        );

        let second = start + Duration::from_millis(1_003);
        let host = meter
            .note(at(22_500, 55_640, 1_007, Counted::Cards), second)
            .unwrap();
        assert_eq!(meter.interface_rates["eno1"], host);
        assert_eq!(meter.interface_rates["doublezero0"], per_second(997, 997));

        // A reading that summed other interfaces has no host total, but each interface still moves.
        let third = second + Duration::from_secs(1);
        assert_eq!(
            meter.note(at(30_000, 60_000, 2_007, Counted::Every), third),
            None
        );
        assert_eq!(
            meter.interface_rates["doublezero0"],
            per_second(1_000, 1_000)
        );

        meter.give_up();
        assert!(
            meter.interface_rates.is_empty(),
            "no rate outlives the readings"
        );
    }

    #[test]
    fn test_the_interfaces_are_sent_every_tick_with_the_rates_handed_to_them() {
        let Some(name) = net_ifaces::read()
            .ok()
            .and_then(|readings| readings.into_iter().next())
            .map(|reading| reading.name)
        else {
            return;
        };
        let harness = fixture();
        let sent_for = || {
            let interfaces = harness.published_value("net_interfaces").unwrap();
            interfaces
                .as_array()
                .unwrap()
                .iter()
                .find(|interface| interface["name"] == name.as_str())
                .map(|interface| interface["received_per_second"].clone())
        };
        let mut meter = InterfaceMeter::default();
        meter.tick(
            &harness.publisher,
            &HashMap::from([(name.clone(), per_second(10, 20))]),
        );
        assert_eq!(sent_for(), Some(serde_json::json!(10)));
        // Well inside the five seconds between readings of the interfaces themselves.
        meter.tick(
            &harness.publisher,
            &HashMap::from([(name.clone(), per_second(30, 40))]),
        );
        assert_eq!(sent_for(), Some(serde_json::json!(30)));
    }

    #[test]
    fn test_only_an_interface_down_with_no_routes_and_no_traffic_is_left_out() {
        let idle = reading("docker0", false, 0);
        let quiet = Some(per_second(0, 0));
        assert_eq!(interface_shown(&idle, 0, quiet), None);
        assert_eq!(interface_shown(&idle, 0, None), None);
        assert!(interface_shown(&reading("docker0", false, 1), 1, quiet).is_some());
        assert!(interface_shown(&idle, 0, Some(per_second(10, 0))).is_some());
        let shown = interface_shown(&reading("doublezero0", true, 2), 115, None).unwrap();
        assert_eq!((shown.reading.routes, shown.routes_peak), (2, 115));
        assert_eq!(shown.received_per_second, None);
        let tunnel = InterfaceReading {
            kind: InterfaceKind::Tunnel,
            member_of: Some("bond0".to_string()),
            inside: Some("bond0".to_string()),
            ..reading("doublezero0", true, 115)
        };
        assert_eq!(interface_shown(&tunnel, 115, None).unwrap().reading, tunnel);
    }

    #[test]
    fn test_host_rates_skip_a_reading_that_summed_other_interfaces() {
        let second = Duration::from_secs(1);
        let cards = (counters(1_000, 2_000), Counted::Cards);
        assert_eq!(
            network_rates(cards, (counters(3_000, 2_500), Counted::Cards), second),
            Some(Network {
                received_per_second: 2_000,
                sent_per_second: 500,
            })
        );
        // A driver reload drops the cards, and the bond's lifetime totals are summed instead.
        let fallback = (
            counters(9_000_000_000_000, 9_000_000_000_000),
            Counted::Every,
        );
        assert_eq!(network_rates(cards, fallback, second), None);
        assert_eq!(network_rates(fallback, cards, second), None);
        assert_eq!(
            network_rates(cards, (counters(500, 2_500), Counted::Cards), second),
            None
        );
        assert_eq!(network_rates(cards, cards, Duration::ZERO), None);
    }

    /// Each second's cache hits, misses and evictions, summed over the window.
    fn window(samples: &[(u64, u64, u64)]) -> AccountsTotals {
        let mut window = VecDeque::new();
        samples.iter().fold(
            AccountsTotals::default(),
            |_, &(cache_hits, cache_misses, cache_evicts)| {
                let sample = AccountsTotals {
                    cache_hits,
                    cache_misses,
                    cache_evicts,
                    ..AccountsTotals::default()
                };
                sum_window(&mut window, sample, ACCOUNTS_CACHE_WINDOW)
            },
        )
    }

    fn replayed(set: impl Fn(&mut ReplaySlotTimes)) -> ReplaySlotTimes {
        let mut slot = ReplaySlotTimes::default();
        set(&mut slot);
        slot
    }

    #[test]
    fn test_a_replay_window_reports_the_mean_slot() {
        let window = replay_window(&[
            replayed(|s| {
                s.confirming = 10;
                s.execute = 100;
            }),
            replayed(|s| {
                s.confirming = 30;
                s.execute = 300;
            }),
        ])
        .unwrap();

        assert_eq!(window.slots, 2);
        assert_eq!(window.confirming, 20);
        assert_eq!(window.execute, 200);
    }

    #[test]
    fn test_a_peak_is_the_worst_slot_not_the_worst_of_each_field() {
        // The maxima land on different slots; adding them would describe a slot that
        // never happened.
        let window = replay_window(&[
            replayed(|s| {
                s.fetch = 50;
                s.confirming = 1;
            }),
            replayed(|s| {
                s.fetch = 1;
                s.confirming = 40;
            }),
        ])
        .unwrap();

        assert_eq!(window.serial_peak, 51, "the worse of the two slots");
        assert_ne!(window.serial_peak, 90, "not the two maxima added");
    }

    #[test]
    fn test_the_program_cache_carries_its_own_peak() {
        let mut slots = vec![replayed(|s| s.program_cache = 1_000); 9];
        slots.push(replayed(|s| s.program_cache = 45_000));
        let window = replay_window(&slots).unwrap();

        assert_eq!(window.program_cache, 5_400);
        assert_eq!(window.program_cache_peak, 45_000);
    }

    #[test]
    fn test_no_replayed_slots_is_no_panel() {
        assert!(replay_window(&[]).is_none());
    }

    #[test]
    fn test_the_rate_is_taken_over_the_whole_window() {
        // Taken one at a time the rate is 100%, 50%, 0; over the window it is the six
        // in nine it was.
        let (asked, rate) = cache_rate(&window(&[(4, 0, 0), (1, 1, 0), (1, 2, 0)])).unwrap();
        assert_eq!(asked, 9);
        assert!((rate - 6.0 / 9.0).abs() < f64::EPSILON, "{rate}");
    }

    #[test]
    fn test_evictions_are_summed_alongside() {
        let totals = window(&[(10, 1, 2), (10, 1, 3)]);
        assert!(cache_rate(&totals).is_some());
        assert_eq!(totals.cache_evicts, 5);
    }

    #[test]
    fn test_nothing_asked_reports_nothing() {
        // Not zero: a cache that has not been asked is not a cache that is failing.
        assert!(cache_rate(&window(&[])).is_none());
        assert!(cache_rate(&window(&[(0, 0, 0), (0, 0, 4)])).is_none());
    }

    #[test]
    fn test_each_advertised_port_is_listed_with_what_its_receiver_counts() {
        let tap = TapCounters {
            network: NetworkTotals {
                shreds_turbine: 900,
                packets_gossip: 40,
                packets_tpu_vote: 70,
                ..NetworkTotals::default()
            },
            ..TapCounters::default()
        };
        let listed = |alpenglow| {
            ingest_ports_at(&advertised(), &tap, alpenglow)
                .iter()
                .map(|port| (port.name, port.received, port.quic))
                .collect::<Vec<_>>()
        };
        // Only this join knows `shred_fetch_receiver` is the socket gossip advertises as `tvu`.
        // Elsewhere nothing rather than nought: no packets received alongside any drops works out
        // to every packet lost.
        let mut rows = vec![
            ("turbine", Some(900), false),
            // QUIC counts transactions, not packets.
            ("tpu", None, true),
            ("tpu forwards", None, true),
            ("tpu vote", Some(70), false),
            ("gossip", Some(40), false),
            // Its receiver keeps counters that nothing reports.
            ("serve repair", None, false),
        ];
        assert_eq!(listed(false), rows);
        // Under alpenglow the vote port is left out.
        rows.remove(3);
        assert_eq!(listed(true), rows);
    }

    #[test]
    fn test_a_port_with_no_baseline_yet_counts_from_its_own_start() {
        assert_eq!(at_baseline(None, 8001), 0);
        assert_eq!(at_baseline(Some(&HashMap::new()), 8001), 0);
        assert_eq!(at_baseline(Some(&HashMap::from([(8001, 42)])), 8001), 42);
    }

    fn turbine_tap(layers: [u64; 4], dropped: u64, xdp: Option<bool>) -> TapCounters {
        TapCounters {
            network: NetworkTotals {
                turbine_root: layers[0],
                turbine_layer_1: layers[1],
                turbine_layer_2: layers[2],
                turbine_layer_3: layers[3],
                xdp_dropped: dropped,
                ..NetworkTotals::default()
            },
            retransmit_xdp: xdp,
            ..TapCounters::default()
        }
    }

    #[test]
    fn test_the_turbine_layers_and_drops_are_differenced_over_the_window() {
        let publisher = Publisher::new();
        let mut meter = TrafficMeter::default();
        meter.tick(
            &turbine_tap([10, 100, 200, 0], 5, Some(true)),
            &turbine_tap([12, 140, 260, 1], 5, Some(true)),
            &publisher,
        );
        meter.tick(
            &turbine_tap([12, 140, 260, 1], 5, Some(true)),
            &turbine_tap([12, 150, 270, 1], 9, Some(true)),
            &publisher,
        );
        let sent = publisher.snapshot().pop().unwrap();
        assert!(sent.contains(r#""root":2"#), "{sent}");
        assert!(sent.contains(r#""layer_1":50"#), "{sent}");
        assert!(sent.contains(r#""layer_2":70"#), "{sent}");
        assert!(sent.contains(r#""xdp_dropped":4"#), "{sent}");
        assert!(sent.contains(r#""xdp":true"#), "{sent}");
    }

    #[test]
    fn test_the_accounts_db_carries_levels_as_read_and_counts_over_the_window() {
        let upkeep = |shrunk_bytes, index_entries| {
            let mut counters = TapCounters::default();
            counters.accounts_upkeep.totals.shrunk_bytes = shrunk_bytes;
            counters.accounts_upkeep.levels.index_entries = index_entries;
            counters
        };
        let publisher = Publisher::new();
        let mut meter = AccountsMeter::default();
        meter.tick(&upkeep(0, 0), &upkeep(500, 0), &publisher);
        let sent = publisher.snapshot().pop().unwrap();
        assert!(
            sent.contains(r#""key":"accounts_db","value":null"#),
            "{sent}"
        );

        meter.tick(&upkeep(500, 0), &upkeep(800, 900), &publisher);
        let sent = publisher.snapshot().pop().unwrap();
        assert!(sent.contains(r#""index_entries":900"#), "{sent}");
        assert!(sent.contains(r#""shrunk_bytes":800"#), "{sent}");
        assert!(sent.contains(r#""window_seconds":2.0"#), "{sent}");
    }

    #[test]
    fn test_no_turbine_figure_before_a_point_has_reported() {
        let publisher = Publisher::new();
        let mut meter = TrafficMeter::default();
        meter.tick(&TapCounters::default(), &TapCounters::default(), &publisher);
        let sent = publisher.snapshot().pop().unwrap();
        assert!(sent.contains(r#""value":null"#), "{sent}");
    }

    fn at(epoch: Epoch, slots_in: u64) -> WorkingReading {
        let epoch_start_slot = epoch.saturating_mul(432_000);
        WorkingReading {
            epoch,
            slot: epoch_start_slot.saturating_add(slots_in),
            epoch_start_slot,
            slots_in_epoch: 432_000,
            ..WorkingReading::default()
        }
    }

    fn verified(received: u64) -> VerifyTotals {
        VerifyTotals {
            received,
            verified: received,
            ..VerifyTotals::default()
        }
    }

    fn attempted(attempted: u64) -> ExecutedTotals {
        ExecutedTotals {
            attempted,
            succeeded: attempted,
            ..ExecutedTotals::default()
        }
    }

    fn bundled(received: u64, packets: u64) -> BundleTotals {
        BundleTotals { received, packets }
    }

    #[test]
    fn test_leader_totals_add_the_whole_epoch() {
        let mut totals = LeaderTotals::default();
        totals.add(at(842, 10), verified(100), attempted(40), bundled(6, 21));
        totals.add(at(842, 20), verified(0), attempted(0), bundled(0, 0));
        totals.add(at(842, 30), verified(70), attempted(25), bundled(4, 9));

        assert_eq!(totals.bundles, bundled(10, 30));
        assert_eq!(totals.verify.received, 170);
        assert_eq!(totals.executed.attempted, 65);
        assert_eq!(totals.executed.succeeded, 65);
    }

    #[test]
    fn test_the_leader_totals_start_over_when_the_epoch_turns() {
        // The leader schedule and the stake behind it are drawn per epoch, so a total
        // spanning two is a total of two schedules.
        let mut totals = LeaderTotals::default();
        totals.add(
            at(842, 400_000),
            verified(900),
            attempted(300),
            bundled(80, 240),
        );
        totals.add(at(843, 3), verified(11), attempted(4), bundled(1, 2));

        assert_eq!(totals.epoch, Some(843));
        assert_eq!(totals.bundles, bundled(1, 2));
        assert_eq!(totals.verify.received, 11);
        assert_eq!(totals.executed.attempted, 4);
        assert_eq!(totals.span(at(843, 3)).counted_slots, 1);
    }

    #[test]
    fn test_span_reports_the_counted_slots() {
        // A validator restarted part way through an epoch has totals honest about a
        // shorter span than the heading.
        let mut totals = LeaderTotals::default();
        totals.add(
            at(842, 300_000),
            verified(5),
            attempted(2),
            BundleTotals::default(),
        );
        let span = totals.span(at(842, 320_000));

        assert_eq!(span.elapsed_slots, 320_001);
        assert_eq!(span.counted_slots, 20_001);
        assert_eq!(span.slots_in_epoch, 432_000);
    }

    #[test]
    fn test_counted_span_stays_within_the_epoch() {
        // A bank read can land either side of the one that turned the epoch over.
        let totals = LeaderTotals {
            epoch: Some(842),
            from_slot: 842u64.saturating_mul(432_000).saturating_sub(50),
            ..LeaderTotals::default()
        };
        let span = totals.span(at(842, 10));

        assert_eq!(span.counted_slots, 11);
        assert!(span.counted_slots <= span.elapsed_slots);
    }

    #[test]
    fn test_leader_stages_carry_their_epoch() {
        let harness = fixture();
        let mut meters = harness.meters();
        meters.tpu.epoch_now = Some(at(842, 216_000));

        let previous = TapCounters::default();
        let current = TapCounters {
            verify: verified(4_000),
            executed: attempted(1_500),
            ..TapCounters::default()
        };
        meters.collect_waterfall(&previous, &current);

        let span = harness.published_key("summary", "epoch_span").unwrap();
        assert!(span.contains(r#""epoch":842"#), "{span}");
        assert!(span.contains(r#""slots_in_epoch":432000"#), "{span}");
        let verify = harness.published_key("summary", "verify").unwrap();
        assert!(verify.contains(r#""received":4000"#), "{verify}");
        let executed = harness.published_key("summary", "executed").unwrap();
        assert!(executed.contains(r#""attempted":1500"#), "{executed}");
    }

    #[test]
    fn test_leader_stages_wait_for_an_epoch() {
        let harness = fixture();
        let mut meters = harness.meters();
        assert!(meters.tpu.epoch_now.is_none());

        let current = TapCounters {
            verify: verified(900),
            ..TapCounters::default()
        };
        meters.collect_waterfall(&TapCounters::default(), &current);

        assert!(harness.published_key("summary", "verify").is_none());
        assert!(harness.published_key("summary", "epoch_span").is_none());
    }

    #[test]
    fn test_epoch_position_comes_from_the_working_bank() {
        let harness = fixture();
        let mut meters = harness.meters();
        let working = harness.advance_to(64);
        meters.tick();

        let position = meters.tpu.epoch_now.unwrap();
        assert_eq!(position.slot, 64);
        assert_eq!(position.epoch, working.epoch);
        assert!(position.epoch_start_slot <= 64);
    }

    #[test]
    fn test_no_xdp_is_published_where_the_validator_reported_none() {
        let harness = fixture();
        let mut meters = harness.meters();
        meters.tick();

        let published = harness.published_key("summary", "xdp").unwrap();
        assert!(published.contains(r#""value":null"#), "{published}");
    }

    #[test]
    fn test_a_reported_xdp_config_reaches_the_wire_whole() {
        let harness = fixture();
        let mut meters = harness.meters();
        let mut point = DataPoint::new("xdp-network-config");
        point.add_tag("driver", "ice");
        point.add_tag("zero_copy", "true");
        point.add_field_str("model", "Ethernet Controller E810-C for QSFP");
        meters.metrics_tap.observe_point(&point);
        meters.tick();

        let published = harness.published_key("summary", "xdp").unwrap();
        assert!(published.contains(r#""zero_copy":true"#), "{published}");
        assert!(published.contains(r#""driver":"ice""#), "{published}");
        assert!(
            published.contains(r#""model":"Ethernet Controller E810-C for QSFP""#),
            "{published}"
        );
    }

    #[test]
    fn test_the_path_card_stays_once_a_port_has_ever_been_used() {
        let harness = fixture();
        let mut meters = harness.meters();
        let used = quic_tap(40);
        meters.collect_waterfall(&used, &used);

        let published = harness.published_key("summary", "quic_paths").unwrap();
        assert!(!published.contains(r#""value":null"#), "{published}");
        assert!(published.contains(r#""offered":0"#), "{published}");
    }

    #[test]
    fn test_the_vote_port_is_listed_from_its_first_connection_to_a_quiet_epoch() {
        let harness = fixture();
        let mut meters = harness.meters();
        let vote = |offered| {
            let mut counters = quic_tap(1);
            counters.quic_vote.totals.offered = offered;
            counters
        };
        let listed = || {
            harness
                .published_key("summary", "quic_paths")
                .unwrap()
                .contains(r#""name":"tpu vote quic""#)
        };

        meters.tpu.epoch_now = Some(at(1, 100));
        meters.collect_waterfall(&vote(0), &vote(0));
        assert!(!listed(), "nothing has arrived");
        meters.collect_waterfall(&vote(0), &vote(3));
        assert!(listed(), "the first connection lists it");

        meters.tpu.epoch_now = Some(at(2, 100));
        meters.collect_waterfall(&vote(3), &vote(3));
        assert!(listed(), "the epoch had connections");

        meters.tpu.epoch_now = Some(at(3, 100));
        meters.collect_waterfall(&vote(3), &vote(3));
        assert!(!listed(), "an epoch passed with none");
        meters.collect_waterfall(&vote(3), &vote(4));
        assert!(listed(), "back at once when one arrives");
    }

    #[test]
    fn test_no_path_card_before_any_offer() {
        let harness = fixture();
        let mut meters = harness.meters();
        meters.collect_waterfall(&TapCounters::default(), &TapCounters::default());

        let published = harness.published_key("summary", "quic_paths").unwrap();
        assert!(published.contains(r#""value":null"#), "{published}");
    }

    #[test]
    fn test_an_advertised_tpu_bound_elsewhere_is_said_to_be() {
        for (sampled, unavailable, tpu_here, offhost) in [
            // Behind a relayer the socket join finds no TPU port here.
            (true, false, false, true),
            (true, false, true, false),
            // Once the socket table is unreadable every port looks absent, and the honest
            // answer is that we cannot tell.
            (false, true, false, false),
            // The socket meter waits for a viewer, so an empty table may never have been read.
            (false, false, false, false),
        ] {
            let harness = fixture();
            let mut meters = harness.meters();
            meters.sockets.sampled = sampled;
            meters.sockets.unavailable = unavailable;
            if tpu_here {
                meters.sockets.kernel_drops.insert("tpu", 0);
            }
            let used = quic_tap(1);
            meters.collect_waterfall(&used, &used);
            let paths = harness.published_value("quic_paths").unwrap();
            assert_eq!(paths["tpu_offhost"], offhost, "{paths}");
        }
    }

    #[test]
    fn test_a_tick_always_publishes_the_clock() {
        let harness = fixture();
        let mut meters = harness.meters();
        meters.tick();

        assert!(
            harness
                .published_key("summary", "server_time_nanos")
                .is_some()
        );
        assert!(harness.published_key("summary", "uptime_nanos").is_some());
    }

    #[test]
    fn test_a_second_with_no_new_bank_reports_a_rate_of_nought() {
        assert_eq!(
            rate_after((0, 0), 0, Duration::from_millis(20)).unwrap()["total"],
            0.0
        );
    }

    #[test]
    fn test_throughput_needs_two_samples() {
        assert!(
            rate_after((0, 0), 1, Duration::from_millis(250)).is_some(),
            "two readings should have produced a rate"
        );
    }

    #[test]
    fn test_no_rate_is_reported_before_the_collectors_first_count() {
        assert_eq!(
            rate_after(
                (FAILED_UNREAD, FAILED_UNREAD),
                1,
                Duration::from_millis(250)
            ),
            None,
            "a rate was reported before a failed count was in"
        );
    }

    #[test]
    fn test_a_failed_count_that_goes_back_skips_the_sample() {
        // The count follows the working fork, so a fork switch can lower it.
        assert_eq!(
            rate_after((5, 2), 1, Duration::from_millis(250)),
            None,
            "a sample was taken across a fork switch"
        );
    }

    #[test]
    fn test_a_replayed_burst_is_not_reported_as_throughput() {
        // Thirty slots between back-to-back ticks stay above the guard for ticks up to 1.5 s
        // apart, so a loaded machine cannot slow the rate under it.
        assert_eq!(
            rate_after((0, 0), 30, Duration::ZERO),
            None,
            "replay throughput was reported as cluster throughput"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_the_proc_walks_wait_for_a_viewer() {
        let harness = fixture();
        let mut meters = harness.meters();
        meters.tick();
        sleep(Duration::from_millis(20));
        meters.tick();
        assert!(
            harness
                .published_key("summary", "threads_history")
                .is_none(),
            "the thread walk ran with nobody watching"
        );

        let _viewer = harness.publisher.subscribe();
        meters.tick();
        assert!(
            harness
                .published_key("summary", "threads_history")
                .is_none(),
            "the threads were published before a second reading"
        );
        sleep(Duration::from_millis(20));
        meters.tick();
        let published = harness.published_key("summary", "threads_history").unwrap();
        assert!(published.contains(r#""groups":["#), "{published}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_a_thread_reading_from_before_a_gap_starts_the_minute_again() {
        let harness = fixture();
        let mut meter = ThreadMeter::default();
        meter.tick(std::process::id(), &harness.publisher);
        sleep(Duration::from_millis(20));
        meter.tick(std::process::id(), &harness.publisher);
        assert_eq!(meter.history.len(), 1);

        let before_gap = Instant::now().checked_sub(WATCH_GAP * 2).unwrap();
        meter.last.as_mut().unwrap().1 = before_gap;
        meter.tick(std::process::id(), &harness.publisher);
        assert!(meter.history.is_empty() && meter.recent.is_empty());
        assert!(
            harness
                .published_key("summary", "threads_history")
                .unwrap()
                .contains("[]")
        );
        sleep(Duration::from_millis(20));
        meter.tick(std::process::id(), &harness.publisher);
        assert_eq!(meter.history.len(), 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_thread_names_carry_over_until_the_refresh() {
        let harness = fixture();
        let mut meter = ThreadMeter::default();
        meter.tick(std::process::id(), &harness.publisher);
        let named = |meter: &ThreadMeter, name: &str| {
            meter
                .last
                .as_ref()
                .unwrap()
                .0
                .values()
                .any(|reading| reading.name == name)
        };
        for reading in meter.last.as_mut().unwrap().0.values_mut() {
            reading.name = "carried".to_string();
        }
        meter.tick(std::process::id(), &harness.publisher);
        assert!(named(&meter, "carried"));

        meter.named_at = Instant::now().checked_sub(THREAD_NAMES_REFRESH);
        meter.tick(std::process::id(), &harness.publisher);
        assert!(!named(&meter, "carried"));
    }

    fn gossip_tap(reports: u64, received_push: u64, process_us: u64, push_us: u64) -> TapCounters {
        let mut counters = TapCounters::default();
        counters.gossip.totals = GossipTotals {
            reports,
            received_push,
            process_us,
            push_us,
            evicted: reports,
            ..GossipTotals::default()
        };
        counters
    }

    #[test]
    fn test_no_gossip_figure_until_a_report_arrives() {
        let publisher = Publisher::new();
        let mut meter = GossipMeter::default();
        meter.tick(&TapCounters::default(), &TapCounters::default(), &publisher);
        let sent = publisher.snapshot().pop().unwrap();
        assert!(sent.contains(r#""value":null"#), "{sent}");
    }

    #[test]
    fn test_gossip_rates_are_read_over_the_window() {
        let publisher = Publisher::new();
        let mut meter = GossipMeter::default();
        // One report every other second, each carrying 2,000 pushes.
        let mut previous = TapCounters::default();
        for second in 1..=12u64 {
            let reports = second / 2;
            let current = gossip_tap(reports, reports * 2_000, reports * 900, reports * 600);
            meter.tick(&previous, &current, &publisher);
            previous = current;
        }
        let sent = publisher.snapshot().pop().unwrap();
        assert!(
            sent.contains(r#""kind":"push","received":1000.0"#),
            "{sent}"
        );
        // 600 µs of pushes and 300 more of other processing per report, a report every 2 s.
        assert!(sent.contains(r#""push":0.3"#), "{sent}");
        assert!(sent.contains(r#""other":0.15"#), "{sent}");
        assert!(sent.contains(r#""evicted_last_minute":6"#), "{sent}");
    }

    #[test]
    fn test_gossip_drops_are_judged_against_packets_both_ways() {
        let publisher = Publisher::new();
        let mut meter = GossipMeter::default();
        // A report every second: 900 packets in, 100 out, 3 dropped.
        let tap = |reports: u64| {
            let mut counters = TapCounters::default();
            counters.gossip.totals = GossipTotals {
                reports,
                received: reports * 900,
                sent_push: reports * 60,
                sent_prune: reports * 40,
                dropped_in: reports * 3,
                ..GossipTotals::default()
            };
            counters
        };
        let mut previous = TapCounters::default();
        for second in 1..=GOSSIP_MINUTE as u64 {
            let current = tap(second);
            meter.tick(&previous, &current, &publisher);
            previous = current;
        }
        let sent = publisher.snapshot().pop().unwrap();
        let minute = GOSSIP_MINUTE as u64;
        assert!(
            sent.contains(&format!(
                r#""dropped_last_minute":{},"packets_last_minute":{},"packets_per_second":1000.0"#,
                minute * 3,
                minute * 1_000
            )),
            "{sent}"
        );
    }

    #[test]
    fn test_gossip_that_stops_reporting_is_withdrawn() {
        let publisher = Publisher::new();
        let mut meter = GossipMeter::default();
        let reported = gossip_tap(1, 10, 0, 0);
        meter.tick(&TapCounters::default(), &reported, &publisher);
        for _ in 0..GOSSIP_RATE_WINDOW {
            meter.tick(&reported, &reported, &publisher);
        }
        let sent = publisher.snapshot().pop().unwrap();
        assert!(sent.contains(r#""value":null"#), "{sent}");
    }
}
