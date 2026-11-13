//! The validator's slots and blocks as its ledger records them, read each pass off the secondary:
//! first shreds and parents from the slot metas, blocks and footers from the shreds of full slots,
//! the confirmed and rooted slots from the rows votor and replay write. Nothing here runs inside
//! the validator.
//!
//! The ledger holds no record of a bank freezing, so a block is sent once its slot is known frozen:
//! rooted or confirmed, which only a frozen bank can be, or reported replayed by the metrics.

use {
    crate::{
        certs::FooterView,
        frames::{Frame, FrozenBlock, Status},
        metrics_tap::MetricsTap,
        remote::Shared,
    },
    solana_clock::Slot,
    solana_entry::block_component::{
        BlockComponent, BlockFooterV1, VersionedBlockFooter, VersionedBlockMarker,
    },
    solana_ledger::{blockstore::Blockstore, blockstore_meta::SlotMeta},
    std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// Metas read past the root each pass, and as many before the newest slot where the two are
/// further apart: a validator catching up holds thousands between.
const METAS_READ: u64 = 256;
/// Roots read in one pass. Further behind than this the validator is replaying its ledger at
/// startup, and the follower starts again from the newest root.
const ROOTS_READ: usize = 256;
/// How far below the root a rooted slot is still waited for. The validator roots a block of its
/// own as soon as it is finalized, which can be before its last shreds are in the ledger.
const UNFILLED_KEPT: u64 = 64;
/// How long a slot's root is held back for its block, which the slot feed takes first.
const ROOT_HELD: Duration = Duration::from_secs(2);

/// A full slot's block, read once and held until the slot is known frozen.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Block {
    blockhash: String,
    transactions: u64,
    entries: u64,
}

/// What has been sent of a slot above the root.
#[derive(Debug, Default)]
struct Sent {
    first_shred: bool,
    /// The parent the slot's bank was reported made on.
    bank_parent: Option<Slot>,
    parent: Option<Slot>,
    held: Option<Block>,
    block: bool,
}

pub struct Follower {
    tap: Arc<MetricsTap>,
    remote: Shared,
    pid: Option<u32>,
    /// The newest root read.
    root: Option<Slot>,
    /// Roots read and not yet sent, oldest first, with when each was read.
    rooted: VecDeque<(Slot, Instant)>,
    confirmed: Slot,
    sent: BTreeMap<Slot, Sent>,
    /// Slots at or above the root known frozen by their place in the chain.
    frozen: BTreeSet<Slot>,
}

impl Follower {
    pub fn new(tap: Arc<MetricsTap>, remote: Shared) -> Self {
        Self {
            tap,
            remote,
            pid: None,
            root: None,
            rooted: VecDeque::new(),
            confirmed: 0,
            sent: BTreeMap::new(),
            frozen: BTreeSet::new(),
        }
    }

    /// Sends what the ledger has gained since the last pass.
    pub fn pass(&mut self, blockstore: &Blockstore, sink: &mut impl FnMut(Frame)) {
        let pid = self.remote.read().unwrap().pid;
        if let Some(pid) = pid
            && self.pid != Some(pid)
        {
            // Another validator's ledger is another chain.
            if self.pid.is_some() {
                *self = Self::new(self.tap.clone(), self.remote.clone());
            }
            self.pid = Some(pid);
            sink(Frame::Validator { pid });
        }
        let Some(root) = self.read_roots(blockstore, sink) else {
            return;
        };
        self.read_confirmed(blockstore, root, sink);
        self.read_unfilled(blockstore, root, sink);
        self.read_metas(blockstore, root, sink);
        self.send_rooted(sink);
        self.remote.write().unwrap().finalized = Some(root);
        let floor = root.saturating_sub(UNFILLED_KEPT);
        self.sent
            .retain(|slot, sent| *slot > root || (*slot > floor && !sent.block));
        self.frozen = self.frozen.split_off(&floor);
    }

    /// The newest root, after sending each slot rooted since the last pass with its block.
    fn read_roots(
        &mut self,
        blockstore: &Blockstore,
        sink: &mut impl FnMut(Frame),
    ) -> Option<Slot> {
        let newest = || {
            blockstore
                .reversed_rooted_slot_iterator(Slot::MAX)
                .ok()?
                .next()
        };
        let rooted: Vec<Slot> = match self.root {
            Some(root) => blockstore
                .rooted_slot_iterator(root.saturating_add(1))
                .ok()?
                .take(ROOTS_READ.saturating_add(1))
                .collect(),
            None => Vec::new(),
        };
        if self.root.is_none() || rooted.len() > ROOTS_READ {
            let root = newest()?;
            self.sent.clear();
            self.frozen.clear();
            self.rooted.clear();
            self.frozen.insert(root);
            self.root = Some(root);
            sink(status(root, None, Status::Rooted));
            return Some(root);
        }
        for slot in rooted {
            self.frozen.insert(slot);
            // Held until its block is sent, which waits for the slot to fill.
            self.sent.entry(slot).or_default();
            if let Ok(Some(meta)) = blockstore.meta(slot) {
                self.note_meta(blockstore, slot, &meta, sink);
            }
            self.rooted.push_back((slot, Instant::now()));
            self.root = Some(slot);
        }
        self.root
    }

    /// Sends each root once its slot's block and footer are sent, in order, as the slot feed
    /// takes a slot's footer before its root.
    fn send_rooted(&mut self, sink: &mut impl FnMut(Frame)) {
        while let Some((slot, read)) = self.rooted.front().copied() {
            let unsent = self.sent.get(&slot).is_some_and(|sent| !sent.block);
            if unsent && read.elapsed() < ROOT_HELD {
                return;
            }
            self.rooted.pop_front();
            sink(status(slot, None, Status::Rooted));
        }
    }

    /// The slots at or below the root whose blocks are still to send.
    fn read_unfilled(&mut self, blockstore: &Blockstore, root: Slot, sink: &mut impl FnMut(Frame)) {
        let unfilled: Vec<Slot> = self.sent.range(..=root).map(|(slot, _)| *slot).collect();
        for slot in unfilled {
            if let Ok(Some(meta)) = blockstore.meta(slot) {
                self.note_meta(blockstore, slot, &meta, sink);
            }
        }
    }

    /// Votor's newest row: the slot it finalized, whose ancestors are frozen with it.
    fn read_confirmed(
        &mut self,
        blockstore: &Blockstore,
        root: Slot,
        sink: &mut impl FnMut(Frame),
    ) {
        let newest = blockstore
            .reversed_optimistic_slots_iterator()
            .ok()
            .and_then(|mut rows| rows.next());
        let Some((slot, ..)) = newest else {
            return;
        };
        if slot <= self.confirmed {
            return;
        }
        self.confirmed = slot;
        let mut at = slot;
        while at > root && self.frozen.insert(at) {
            // A slot not yet seen by a pass names its parent in its meta.
            let parent = self.sent.get(&at).and_then(|sent| sent.parent).or_else(|| {
                blockstore
                    .meta(at)
                    .ok()
                    .flatten()
                    .and_then(|meta| meta.parent_slot)
            });
            let Some(parent) = parent else {
                break;
            };
            at = parent;
        }
        sink(status(slot, None, Status::Confirmed));
    }

    fn read_metas(&mut self, blockstore: &Blockstore, root: Slot, sink: &mut impl FnMut(Frame)) {
        let from = root.saturating_add(1);
        let Ok(Some(highest)) = blockstore.highest_slot() else {
            return;
        };
        let near = from.saturating_add(METAS_READ);
        let tip = highest.saturating_sub(METAS_READ).max(near);
        for (first, last) in [
            (from, near.min(highest.saturating_add(1))),
            (tip, Slot::MAX),
        ] {
            let Ok(metas) = blockstore.slot_meta_iterator(first) else {
                return;
            };
            let metas: Vec<(Slot, SlotMeta)> = metas.take_while(|(slot, _)| *slot < last).collect();
            for (slot, meta) in metas {
                self.note_meta(blockstore, slot, &meta, sink);
            }
        }
    }

    /// Whether `slot`'s bank is known frozen, its block being whole in the ledger. A block of
    /// ours is whole only once frozen: its footer carries the bank's hash.
    fn known_frozen(&self, slot: Slot, full: bool) -> bool {
        self.frozen.contains(&slot)
            || self.tap.replayed(slot).is_some()
            || (full && self.remote.read().unwrap().is_ours(slot) == Some(true))
    }

    /// Sends what `meta` shows of its slot for the first time.
    fn note_meta(
        &mut self,
        blockstore: &Blockstore,
        slot: Slot,
        meta: &SlotMeta,
        sink: &mut impl FnMut(Frame),
    ) {
        // A meta made for a slot only named as a parent holds no shreds.
        if meta.received == 0 {
            return;
        }
        let root = self.root.unwrap_or_default();
        let parent = meta.parent_slot;
        // A slot holds shreds only once its parent's block is whole.
        let parent_frozen =
            parent.is_some_and(|parent| parent <= root || self.known_frozen(parent, true));
        let frozen = self.known_frozen(slot, meta.is_full());
        let sent = self.sent.entry(slot).or_default();
        sent.parent = parent;
        if !std::mem::replace(&mut sent.first_shred, true) {
            let first = Status::FirstShredReceived {
                at_millis: meta.first_shred_timestamp,
            };
            sink(status(slot, parent, first));
        }
        // Replay makes a slot's bank once its parent is frozen and a shred of it is in. An
        // UpdateParent marker moves the slot to another parent, and replay makes its bank again.
        if parent_frozen && sent.bank_parent != parent {
            sent.bank_parent = parent;
            sink(status(slot, parent, Status::CreatedBank));
        }
        if sent.block {
            return;
        }
        if !meta.is_full() {
            // A slot switched to another version of its block fills again.
            sent.held = None;
            return;
        }
        if sent.held.is_none()
            && let Some((block, footer)) = read_block(blockstore, slot, meta)
        {
            sent.held = Some(block);
            if let Some(footer) = footer {
                sink(footer);
            }
        }
        let (Some(parent_slot), true) = (parent, frozen) else {
            return;
        };
        let Some(block) = sent.held.take() else {
            return;
        };
        sent.block = true;
        sink(Frame::Block(FrozenBlock {
            slot,
            parent_slot,
            blockhash: block.blockhash,
            executed_transactions: block.transactions,
            entries: block.entries,
        }));
    }
}

fn status(slot: Slot, parent: Option<Slot>, status: Status) -> Frame {
    Frame::Slot {
        slot,
        parent,
        status,
    }
}

/// The slot's block as replay reads it: from where an UpdateParent marker moved its start, if one
/// did, so the entries of the abandoned start are not counted.
fn read_block(
    blockstore: &Blockstore,
    slot: Slot,
    meta: &SlotMeta,
) -> Option<(Block, Option<Frame>)> {
    let start = u64::from(meta.replay_fec_set_index);
    let (components, _, full) = blockstore
        .get_slot_components_with_shred_info(slot, start, false)
        .ok()?;
    if !full {
        return None;
    }
    let (block, footer) = summarize(&components)?;
    let footer = footer.map(|footer| Frame::Footer {
        slot,
        footer: FooterView::from(footer),
    });
    Some((block, footer))
}

/// `None` for a block without an entry.
fn summarize(components: &[BlockComponent]) -> Option<(Block, Option<&BlockFooterV1>)> {
    let mut blockhash = None;
    let mut transactions = 0u64;
    let mut entries = 0u64;
    let mut footer = None;
    for component in components {
        match component {
            BlockComponent::EntryBatch(batch) => {
                for entry in batch {
                    if !entry.is_tick() {
                        entries = entries.saturating_add(1);
                    }
                    transactions = transactions
                        .saturating_add(u64::try_from(entry.transactions.len()).unwrap_or(0));
                    blockhash = Some(entry.hash);
                }
            }
            BlockComponent::BlockMarker(VersionedBlockMarker::V1(marker)) => {
                if let Some(VersionedBlockFooter::V1(found)) = marker.as_block_footer() {
                    footer = Some(found);
                }
            }
        }
    }
    let block = Block {
        blockhash: blockhash?.to_string(),
        transactions,
        entries,
    };
    Some((block, footer))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote::{EpochLeaders, Remote, THEM, US},
        solana_entry::{block_component::BlockFinalizationCert, entry::Entry},
        solana_epoch_schedule::EpochSchedule,
        solana_ledger::blockstore::make_slot_entries,
        solana_metrics::datapoint::DataPoint,
        std::sync::RwLock,
    };

    const ENTRIES: u64 = 4;

    struct Harness {
        blockstore: Blockstore,
        follower: Follower,
        tap: Arc<MetricsTap>,
        remote: Shared,
        /// Last, so it is removed once the ledger is closed.
        _ledger: tempfile::TempDir,
    }

    fn harness() -> Harness {
        let ledger = tempfile::tempdir().unwrap();
        let tap = Arc::new(MetricsTap::default());
        let remote: Shared = Arc::new(RwLock::new(Remote::default()));
        let harness = Harness {
            blockstore: Blockstore::open(ledger.path()).unwrap(),
            follower: Follower::new(tap.clone(), remote.clone()),
            tap,
            remote,
            _ledger: ledger,
        };
        harness.root(&[0]);
        harness
    }

    impl Harness {
        fn fill(&self, slot: Slot, parent: Slot) -> Vec<Entry> {
            let (shreds, entries) = make_slot_entries(slot, parent, ENTRIES);
            // As the validator inserts its own blocks' shreds: a slot already rooted takes them.
            self.blockstore.insert_shreds(shreds, true).unwrap();
            entries
        }

        fn root(&self, slots: &[Slot]) {
            self.blockstore.set_roots(slots.iter()).unwrap();
        }

        fn metrics(&self, name: &'static str, fields: &[(&'static str, u64)]) {
            let mut point = DataPoint::new(name);
            for (field, value) in fields {
                point.fields.push((field, format!("{value}i")));
            }
            self.tap.observe_external(&point);
        }

        fn pass(&mut self) -> Vec<Frame> {
            let mut frames = Vec::new();
            self.follower
                .pass(&self.blockstore, &mut |frame| frames.push(frame));
            frames
        }
    }

    /// Matched by kind: a first shred's stamp is the ledger's.
    const FIRST_SHRED: Status = Status::FirstShredReceived { at_millis: 0 };

    fn slots(frames: &[Frame], wanted: Status) -> Vec<(Slot, Option<Slot>)> {
        let kind = std::mem::discriminant(&wanted);
        frames
            .iter()
            .filter_map(|frame| match frame {
                Frame::Slot {
                    slot,
                    parent,
                    status,
                } if std::mem::discriminant(status) == kind => Some((*slot, *parent)),
                _ => None,
            })
            .collect()
    }

    /// Each block's slot and parent.
    fn blocks(frames: &[Frame]) -> Vec<(Slot, Slot)> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                Frame::Block(block) => Some((block.slot, block.parent_slot)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_a_block_is_sent_once_its_slot_is_rooted() {
        let mut harness = harness();
        let entries = harness.fill(1, 0);

        let frames = harness.pass();
        assert_eq!(slots(&frames, Status::Rooted), [(0, None)]);
        assert_eq!(slots(&frames, FIRST_SHRED), [(1, Some(0))]);
        assert_eq!(slots(&frames, Status::CreatedBank), [(1, Some(0))]);
        assert_eq!(blocks(&frames), []);
        assert_eq!(harness.pass(), []);

        harness.root(&[1]);
        let frames = harness.pass();
        assert_eq!(slots(&frames, Status::Rooted), [(1, None)]);
        let [
            Frame::Block(FrozenBlock {
                slot: 1,
                parent_slot: 0,
                blockhash,
                executed_transactions: 0,
                // Ticks alone: no entry holds a transaction.
                entries: 0,
            }),
            _,
        ] = &frames[..]
        else {
            panic!("{frames:?}");
        };
        assert_eq!(*blockhash, entries.last().unwrap().hash.to_string());
        assert_eq!(harness.pass(), []);
    }

    #[test]
    fn test_a_slot_rooted_before_its_shreds_are_in_is_waited_for() {
        let mut harness = harness();
        harness.pass();
        harness.root(&[1, 2]);
        harness.fill(2, 1);
        let frames = harness.pass();
        // The roots wait for the first slot's block, so each block comes before its root.
        assert_eq!(slots(&frames, Status::Rooted), []);
        assert_eq!(blocks(&frames), [(2, 1)]);

        harness.fill(1, 0);
        let frames = harness.pass();
        assert_eq!(slots(&frames, FIRST_SHRED), [(1, Some(0))]);
        assert_eq!(blocks(&frames), [(1, 0)]);
        assert_eq!(slots(&frames, Status::Rooted), [(1, None), (2, None)]);
        assert_eq!(harness.pass(), []);
    }

    #[test]
    fn test_a_slot_the_metrics_report_replayed_is_frozen() {
        let mut harness = harness();
        harness.fill(1, 0);
        assert_eq!(blocks(&harness.pass()), []);

        harness.metrics(
            "replay-slot-stats",
            &[("slot", 1), ("bank_complete_time_us", 5)],
        );
        assert_eq!(blocks(&harness.pass()), [(1, 0)]);
    }

    #[test]
    fn test_a_confirmed_slots_ancestors_are_frozen_with_it() {
        let mut harness = harness();
        harness.fill(1, 0);
        let entries = harness.fill(2, 1);
        let frames = harness.pass();
        // The bank of a slot is made once its parent is frozen.
        assert_eq!(slots(&frames, Status::CreatedBank), [(1, Some(0))]);

        harness
            .blockstore
            .insert_optimistic_slot(2, &entries[0].hash, 0)
            .unwrap();
        let frames = harness.pass();
        assert_eq!(slots(&frames, Status::Confirmed), [(2, None)]);
        assert_eq!(blocks(&frames), [(1, 0), (2, 1)]);
        assert_eq!(slots(&frames, Status::CreatedBank), [(2, Some(1))]);
    }

    #[test]
    fn test_a_slot_confirmed_before_a_pass_saw_it_freezes_its_ancestors() {
        let mut harness = harness();
        harness.pass();
        harness.fill(1, 0);
        let entries = harness.fill(2, 1);
        harness
            .blockstore
            .insert_optimistic_slot(2, &entries[0].hash, 0)
            .unwrap();
        assert_eq!(blocks(&harness.pass()), [(1, 0), (2, 1)]);
    }

    #[test]
    fn test_a_block_of_ours_is_frozen_once_whole() {
        let mut harness = harness();
        {
            // Slot 1 is ours and slot 2 another leader's.
            let schedule = EpochSchedule::without_warmup();
            let leaders = EpochLeaders::for_tests(schedule.get_slots_in_epoch(0), |slot| {
                if slot == 1 { US } else { THEM }
            });
            let mut remote = harness.remote.write().unwrap();
            remote.identity = Some(US);
            remote.schedules = [(0, Arc::new(leaders))].into();
            remote.epoch_schedule = Some(schedule);
        }
        harness.fill(1, 0);
        harness.fill(2, 1);
        let frames = harness.pass();
        // Its footer carries the bank's hash, so it is whole only once frozen. Another leader's
        // block is whole before replay has run it.
        assert_eq!(blocks(&frames), [(1, 0)]);
        assert_eq!(
            slots(&frames, Status::CreatedBank),
            [(1, Some(0)), (2, Some(1))]
        );
    }

    #[test]
    fn test_a_slot_only_named_as_a_parent_has_no_first_shred() {
        let mut harness = harness();
        harness.fill(3, 2);
        let frames = harness.pass();
        assert_eq!(slots(&frames, FIRST_SHRED), [(3, Some(2))]);
        assert_eq!(slots(&frames, Status::CreatedBank), []);
    }

    #[test]
    fn test_too_many_roots_at_once_start_again_from_the_newest() {
        let mut harness = harness();
        harness.pass();
        let rooted: Vec<Slot> = (1..=300).collect();
        harness.root(&rooted);
        assert_eq!(slots(&harness.pass(), Status::Rooted), [(300, None)]);
    }

    #[test]
    fn test_another_validator_starts_the_chain_again() {
        let mut harness = harness();
        harness.remote.write().unwrap().pid = Some(7);
        let frames = harness.pass();
        assert_eq!(frames[0], Frame::Validator { pid: 7 });
        assert_eq!(harness.pass(), []);

        harness.remote.write().unwrap().pid = Some(8);
        let frames = harness.pass();
        assert_eq!(frames[0], Frame::Validator { pid: 8 });
        assert_eq!(slots(&frames, Status::Rooted), [(0, None)]);
    }

    #[test]
    fn test_a_block_is_its_entries_and_its_footer() {
        let (_, mut entries) = make_slot_entries(1, 0, 3);
        entries[1].transactions.resize_with(2, Default::default);
        let blockhash = entries[2].hash.to_string();
        let footer = BlockFooterV1 {
            bank_hash: entries[0].hash,
            block_producer_time_nanos: 77,
            block_user_agent: b"agave".to_vec(),
            block_final_cert: Some(BlockFinalizationCert::new_for_tests()),
            skip_reward_cert: None,
            notar_reward_cert: None,
        };
        let components = [
            BlockComponent::EntryBatch(entries),
            BlockComponent::BlockMarker(VersionedBlockMarker::from_block_footer(footer)),
        ];
        let (block, footer) = summarize(&components).unwrap();
        assert_eq!(
            block,
            Block {
                blockhash,
                transactions: 2,
                // The two ticks around it hold none.
                entries: 1,
            }
        );
        // A certificate without a notarization aggregate finalized fast.
        assert_eq!(
            FooterView::from(footer.unwrap()),
            FooterView {
                producer_time_nanos: 77,
                notar: None,
                skip: None,
                fast: Some(true),
            }
        );
        assert!(summarize(&components[1..]).is_none());
    }
}
