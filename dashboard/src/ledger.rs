//! The validator's ledger, followed as a RocksDB secondary on one handle. The follower's thread
//! opens it, takes in what the validator has written since the last pass and reads the slots and
//! blocks off it ([`crate::ledger_feed`]); the reader's takes the entries of our blocks, for their
//! transaction versions, and of every rooted block, for the fee payers. A transaction's result
//! comes from RPC's status cache.
//!
//! A secondary keeps every file of the ledger open, so one that stops taking in the validator's
//! writes holds the files the validator has purged since, and the ledger's disk fills. Only this
//! process ending frees them: a pass that does not come back ends it, and each pass tells systemd
//! the process is alive, so a unit with a watchdog ends one that is stopped whole.

use {
    crate::{
        collect::{
            Replies,
            fee_payers::{BlockInputs, FeePayerSummary, FeePayers, tallied, tally_block},
        },
        frames::Frames,
        ledger_feed::Follower,
        metrics_tap::MetricsTap,
        proto::{Debounced, Publisher, TOPIC_SUMMARY},
        remote::Shared,
        versions,
    },
    agave_feature_set::FeatureSet,
    solana_clock::{Epoch, Slot},
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::{AccessType, BlockstoreOptions},
    },
    solana_pubkey::Pubkey,
    solana_rpc_client::rpc_client::RpcClient,
    solana_rpc_client_api::request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
    solana_signature::Signature,
    solana_time_utils::timestamp,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        collections::HashMap,
        ffi::OsStr,
        io,
        os::unix::net::{SocketAddr, UnixDatagram},
        path::PathBuf,
        sync::{
            Arc, RwLock,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

const FOLLOW_EVERY: Duration = Duration::from_millis(50);
const READ_EVERY: Duration = Duration::from_secs(1);
/// Passes that may fail to take in the validator's writes before the ledger is opened again.
const FAILURES_ALLOWED: u32 = 3;
/// How long a pass may take before the process ends. Opening replays the validator's write-ahead
/// log, which takes far longer than a pass.
const PASS_LIMIT_MILLIS: u64 = 30_000;
const OPEN_LIMIT_MILLIS: u64 = 600_000;

/// Between the follower's reports to systemd, whose watchdogs are counted in seconds.
const ALIVE_EVERY: Duration = Duration::from_secs(1);

/// systemd's notification socket, as it names it to a unit's process in `NOTIFY_SOCKET`: a path,
/// or after `@` a name in Linux's abstract namespace.
fn notify_socket(name: &OsStr) -> Option<(UnixDatagram, SocketAddr)> {
    let address = match name.as_encoded_bytes().strip_prefix(b"@") {
        #[cfg(target_os = "linux")]
        Some(name) => {
            <SocketAddr as std::os::linux::net::SocketAddrExt>::from_abstract_name(name).ok()?
        }
        #[cfg(not(target_os = "linux"))]
        Some(_) => return None,
        None => SocketAddr::from_pathname(name).ok()?,
    };
    Some((UnixDatagram::unbound().ok()?, address))
}

/// The secondary both threads read.
type Handle = RwLock<Option<Arc<Blockstore>>>;

pub fn spawn(
    ledger: PathBuf,
    remote: Shared,
    replies: Arc<Replies>,
    publisher: Arc<Publisher>,
    tap: Arc<MetricsTap>,
    frames: Frames,
) -> io::Result<()> {
    let handle = Arc::new(Handle::default());
    let due = Arc::new(AtomicU64::new(u64::MAX));
    let mut follow = Follow {
        ledger,
        handle: handle.clone(),
        follower: Follower::new(tap, remote.clone()),
        remote: remote.clone(),
        frames,
        pid: None,
        failures: 0,
        refused: false,
        due: due.clone(),
        systemd: std::env::var_os("NOTIFY_SOCKET").and_then(|name| notify_socket(&name)),
        alive_at: Instant::now(),
    };
    thread::Builder::new()
        .name("solDashSaFollow".to_string())
        .spawn(move || {
            loop {
                let started = Instant::now();
                follow.pass();
                thread::sleep(FOLLOW_EVERY.saturating_sub(started.elapsed()));
            }
        })?;
    thread::Builder::new()
        .name("solDashSaStall".to_string())
        .spawn(move || {
            loop {
                thread::sleep(READ_EVERY);
                if timestamp() > due.load(Ordering::Relaxed) {
                    log::error!(
                        "dashboard: a pass over the ledger did not come back; stopping, so the \
                         files it holds open are freed for the validator"
                    );
                    std::process::exit(1);
                }
            }
        })?;
    let mut reader = Reader {
        handle,
        remote,
        replies,
        publisher,
        fee_payers: FeePayers::default(),
        counted_to: None,
        summary: Debounced::new(TOPIC_SUMMARY, "fee_payers"),
        features: None,
    };
    thread::Builder::new()
        .name("solDashSaLedger".to_string())
        .spawn(move || {
            loop {
                reader.pass();
                thread::sleep(READ_EVERY);
            }
        })?;
    Ok(())
}

struct Follow {
    ledger: PathBuf,
    handle: Arc<Handle>,
    follower: Follower,
    remote: Shared,
    frames: Frames,
    /// The validator whose ledger is open.
    pid: Option<u32>,
    failures: u32,
    /// Whether the ledger's refusal to open has been logged.
    refused: bool,
    /// When the pass under way must be over, in milliseconds.
    due: Arc<AtomicU64>,
    systemd: Option<(UnixDatagram, SocketAddr)>,
    alive_at: Instant,
}

impl Follow {
    fn pass(&mut self) {
        self.due.store(
            timestamp().saturating_add(PASS_LIMIT_MILLIS),
            Ordering::Relaxed,
        );
        if let Some((socket, address)) = &self.systemd
            && self.alive_at.elapsed() >= ALIVE_EVERY
        {
            self.alive_at = Instant::now();
            let _ = socket.send_to_addr(b"WATCHDOG=1", address);
        }
        // Another validator may have started on another ledger in the same directory.
        let pid = self.remote.read().unwrap().pid;
        if let Some(pid) = pid
            && self.pid.replace(pid).is_some_and(|before| before != pid)
        {
            *self.handle.write().unwrap() = None;
        }
        // Read before the match, so the read lock is not held where the opened ledger is stored.
        let held = self.handle.read().unwrap().clone();
        let blockstore = match held {
            Some(blockstore) => blockstore,
            None => {
                self.due.store(
                    timestamp().saturating_add(OPEN_LIMIT_MILLIS),
                    Ordering::Relaxed,
                );
                let options = BlockstoreOptions {
                    access_type: AccessType::Secondary,
                    ..BlockstoreOptions::default()
                };
                // Opening makes the directories it does not find; only a ledger the validator
                // has made is followed.
                let opened = if self.ledger.join("rocksdb").join("CURRENT").exists() {
                    Blockstore::open_with_options(&self.ledger, options)
                        .map_err(|err| err.to_string())
                } else {
                    Err("no ledger there yet".to_string())
                };
                match opened {
                    Ok(blockstore) => {
                        self.refused = false;
                        let blockstore = Arc::new(blockstore);
                        *self.handle.write().unwrap() = Some(blockstore.clone());
                        blockstore
                    }
                    Err(err) => {
                        if !std::mem::replace(&mut self.refused, true) {
                            log::warn!("dashboard: the ledger did not open: {err}");
                        }
                        thread::sleep(READ_EVERY);
                        return;
                    }
                }
            }
        };
        match blockstore.try_catch_up_with_primary() {
            Ok(()) => {
                self.failures = 0;
                let frames = &self.frames;
                self.follower
                    .pass(&blockstore, &mut |frame| frames.send(frame));
            }
            Err(err) => {
                self.failures = self.failures.saturating_add(1);
                log::warn!("dashboard: the ledger did not catch up: {err}");
                if self.failures >= FAILURES_ALLOWED {
                    self.failures = 0;
                    *self.handle.write().unwrap() = None;
                }
            }
        }
    }
}

/// The slot's entries as replay reads them: from where an UpdateParent marker moved its start, so
/// the abandoned start is not counted.
fn block_entries(
    blockstore: &Blockstore,
    slot: Slot,
) -> solana_ledger::blockstore::Result<Vec<solana_entry::entry::Entry>> {
    let start = blockstore
        .meta(slot)?
        .map_or(0, |meta| u64::from(meta.replay_fec_set_index));
    blockstore.get_slot_entries(slot, start)
}

/// A rooted block's part in the fee payers' tally.
struct Rooted {
    slot: Slot,
    epoch: Epoch,
    ours: bool,
    lamports_per_signature: u64,
    /// The transactions whose results the tally reads, with their payers.
    tallied: Vec<(VersionedTransaction, Pubkey)>,
}

struct Reader {
    handle: Arc<Handle>,
    remote: Shared,
    replies: Arc<Replies>,
    publisher: Arc<Publisher>,
    fee_payers: FeePayers,
    /// The newest root read.
    counted_to: Option<Slot>,
    summary: Debounced<Option<FeePayerSummary>>,
    features: Option<(Epoch, FeatureSet)>,
}

impl Reader {
    fn pass(&mut self) {
        let Some(blockstore) = self.handle.read().unwrap().clone() else {
            return;
        };
        self.fill_versions(&blockstore);
        let address = self.remote.read().unwrap().rpc_address;
        if let Some(address) = address {
            self.count_rooted(&blockstore, &RpcClient::new(format!("http://{address}")));
        }
        self.summary
            .publish(&self.publisher, self.fee_payers.summary());
    }

    /// Our produced blocks' versions, once each is whole in the ledger.
    fn fill_versions(&self, blockstore: &Blockstore) {
        let pending = self.replies.produced.read().unwrap().without_versions();
        let floor = blockstore.lowest_slot();
        for slot in pending {
            if slot < floor || !blockstore.is_full(slot) {
                continue;
            }
            let Ok(entries) = block_entries(blockstore, slot) else {
                continue;
            };
            let tally = versions::tally(entries.iter().flat_map(|entry| &entry.transactions));
            self.replies
                .produced
                .write()
                .unwrap()
                .set_versions(slot, tally);
        }
    }

    /// Every block rooted since the last pass, in order. Their transactions' results are asked of
    /// RPC together, up to [`MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS`] and up to each block of
    /// ours, whose payers are followed in the blocks after it.
    fn count_rooted(&mut self, blockstore: &Blockstore, rpc: &RpcClient) {
        let from = match self.counted_to {
            Some(slot) => slot.saturating_add(1),
            None => {
                // The handle's own newest root is the one it opened with.
                self.counted_to = blockstore
                    .reversed_rooted_slot_iterator(Slot::MAX)
                    .ok()
                    .and_then(|mut roots| roots.next());
                return;
            }
        };
        let Ok(roots) = blockstore.rooted_slot_iterator(from) else {
            return;
        };
        let (mut batch, mut asked) = (Vec::new(), 0usize);
        for slot in roots {
            let Some(block) = self.read_rooted(blockstore, slot) else {
                break;
            };
            asked = asked.saturating_add(block.tallied.len());
            let due = block.ours || asked >= MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS;
            batch.push(block);
            if due {
                if !self.count(rpc, std::mem::take(&mut batch)) {
                    return;
                }
                asked = 0;
            }
        }
        self.count(rpc, batch);
    }

    /// `None` until the slot is full and the inputs are in: the epoch schedule, our leader slots
    /// and the fee rate.
    fn read_rooted(&self, blockstore: &Blockstore, slot: Slot) -> Option<Rooted> {
        if !blockstore.is_full(slot) {
            return None;
        }
        let entries = block_entries(blockstore, slot).ok()?;
        let (epoch, ours, lamports_per_signature) = {
            let remote = self.remote.read().unwrap();
            (
                remote.epoch_schedule.as_ref()?.get_epoch(slot),
                remote.is_ours(slot)?,
                remote.lamports_per_signature?,
            )
        };
        let followed = |payer: &_| self.fee_payers.followed(epoch, payer);
        let tallied = tallied(&entries, ours, followed)
            .map(|(transaction, payer)| (transaction.clone(), *payer))
            .collect();
        Some(Rooted {
            slot,
            epoch,
            ours,
            lamports_per_signature,
            tallied,
        })
    }

    /// Counts `batch` in order, once RPC gave its transactions' results; false if it did not.
    fn count(&mut self, rpc: &RpcClient, batch: Vec<Rooted>) -> bool {
        let signatures: Vec<Signature> = batch
            .iter()
            .flat_map(|block| &block.tallied)
            .filter_map(|(transaction, _)| transaction.signatures.first().copied())
            .collect();
        let mut results: HashMap<Signature, (Slot, bool)> = HashMap::new();
        for chunk in signatures.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS) {
            let Ok(statuses) = rpc.get_signature_statuses(chunk) else {
                return false;
            };
            results.extend(
                chunk
                    .iter()
                    .zip(statuses.value)
                    .filter_map(|(signature, status)| {
                        let status = status?;
                        Some((*signature, (status.slot, status.err.is_some())))
                    }),
            );
        }
        for block in batch {
            if self
                .features
                .as_ref()
                .is_none_or(|(held, _)| *held != block.epoch)
            {
                self.features = Some((block.epoch, self.feature_set(block.slot)));
            }
            let Some((_, feature_set)) = &self.features else {
                return false;
            };
            let inputs = BlockInputs {
                slot: block.slot,
                epoch: block.epoch,
                ours: block.ours,
                lamports_per_signature: block.lamports_per_signature,
                feature_set,
            };
            // A result is the block's only if RPC found the transaction in it.
            let failed = |transaction: &VersionedTransaction| {
                let (slot, failed) = results.get(transaction.signatures.first()?)?;
                (*slot == block.slot).then_some(*failed)
            };
            let tallied = block
                .tallied
                .iter()
                .map(|(transaction, payer)| (transaction, payer));
            let (tally, paid) = tally_block(&inputs, tallied, failed);
            self.fee_payers.add(block.slot, tally);
            if let Some(paid) = paid {
                self.replies.produced.write().unwrap().set_fees(paid);
            }
            self.counted_to = Some(block.slot);
        }
        true
    }

    /// The features active at `slot`, as RPC last read their accounts.
    fn feature_set(&self, slot: Slot) -> FeatureSet {
        let mut feature_set = FeatureSet::default();
        for (id, activated) in &self.remote.read().unwrap().features {
            if *activated <= slot {
                feature_set.activate(id, *activated);
            }
        }
        feature_set
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote::{EpochLeaders, Remote, THEM, US},
        serde_json::{Value, json},
        solana_entry::entry::Entry,
        solana_epoch_schedule::EpochSchedule,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::{entries_to_test_shreds, make_slot_entries},
            get_tmp_ledger_path_auto_delete,
        },
        solana_rpc_client::mock_sender::MocksMap,
        solana_rpc_client_api::request::RpcRequest,
    };

    #[test]
    fn test_the_results_of_blocks_rooted_together_are_asked_in_one_read() {
        let ledger = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger.path()).unwrap());
        // Slot 1 is ours; 2 and 3 are another's, where our block's payer pays too.
        let (payer, to) = (Keypair::new(), Pubkey::new_unique());
        for slot in 1..=3 {
            let transfer = solana_system_transaction::transfer(&payer, &to, slot, Hash::default());
            let entries = [Entry {
                num_hashes: 1,
                hash: Hash::default(),
                transactions: vec![VersionedTransaction::from(transfer)],
            }];
            let shreds = entries_to_test_shreds(&entries, slot, slot - 1, true, 0);
            blockstore.insert_shreds(shreds, true).unwrap();
        }
        blockstore.set_roots([1, 2, 3].iter()).unwrap();
        let leaders = EpochLeaders::for_tests(32, |slot| if slot == 1 { US } else { THEM });
        let remote: Shared = Arc::new(RwLock::new(Remote {
            epoch_schedule: Some(EpochSchedule::custom(32, 32, false)),
            identity: Some(US),
            schedules: [(0, Arc::new(leaders))].into(),
            lamports_per_signature: Some(5_000),
            ..Remote::default()
        }));
        let mut reader = Reader {
            handle: Arc::new(RwLock::new(Some(blockstore.clone()))),
            remote,
            replies: Arc::default(),
            publisher: Arc::new(Publisher::new()),
            fee_payers: FeePayers::default(),
            counted_to: Some(0),
            summary: Debounced::new(TOPIC_SUMMARY, "fee_payers"),
            features: None,
        };
        let status = |slot: Slot, err: Value| {
            let status = match &err {
                Value::Null => json!({"Ok": null}),
                err => json!({"Err": err}),
            };
            json!({"slot": slot, "confirmations": null, "status": status, "err": err})
        };
        let answer = |statuses: Vec<Value>| json!({"context": {"slot": 3}, "value": statuses});
        // One read each: a further one would get the mock's own answer, of another slot.
        let rpc = RpcClient::new_mock_with_mocks_map(
            "succeeds",
            MocksMap::from_iter([
                (
                    RpcRequest::GetSignatureStatuses,
                    answer(vec![status(1, Value::Null)]),
                ),
                (
                    RpcRequest::GetSignatureStatuses,
                    answer(vec![
                        status(2, Value::Null),
                        status(3, json!("AccountInUse")),
                    ]),
                ),
            ]),
        );
        reader.count_rooted(&blockstore, &rpc);
        assert_eq!(reader.counted_to, Some(3));
        let summary = reader.fee_payers.summary().unwrap();
        let epoch = &summary.epochs[0];
        assert_eq!((epoch.blocks, epoch.transactions, epoch.failed), (1, 1, 0));
        // Followed after our block, and read in one call for both blocks.
        assert_eq!((epoch.away_transactions, epoch.away_failed), (2, 1));
    }

    #[test]
    fn test_systemd_is_told_at_the_socket_it_names() {
        // A socket's path is short: the system's temporary directory, not the ledgers'.
        let path = std::env::temp_dir().join(format!("dash-notify-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let listener = UnixDatagram::bind(&path).unwrap();
        let (socket, address) = notify_socket(path.as_os_str()).unwrap();
        socket.send_to_addr(b"WATCHDOG=1", &address).unwrap();
        let mut told = [0u8; 16];
        let length = listener.recv(&mut told).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(&told[..length], b"WATCHDOG=1");
    }

    #[test]
    fn test_a_secondary_reads_what_the_validator_wrote_once_caught_up() {
        let ledger = get_tmp_ledger_path_auto_delete!();
        let primary = Blockstore::open(ledger.path()).unwrap();
        let secondary = Blockstore::open_with_options(
            ledger.path(),
            BlockstoreOptions {
                access_type: AccessType::Secondary,
                ..BlockstoreOptions::default()
            },
        )
        .unwrap();
        let (shreds, entries) = make_slot_entries(1, 0, 8);
        primary.insert_shreds(shreds, false).unwrap();
        primary.set_roots([1].iter()).unwrap();
        assert!(!secondary.is_full(1));
        secondary.try_catch_up_with_primary().unwrap();
        assert!(secondary.is_full(1));
        assert_eq!(secondary.get_slot_entries(1, 0).unwrap(), entries);
        // The handle's own newest root stays the one it opened with; the rows are current.
        let mut roots = secondary.reversed_rooted_slot_iterator(Slot::MAX).unwrap();
        assert_eq!(roots.next(), Some(1));
    }
}
