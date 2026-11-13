//! Detail for the blocks this validator produced, captured while the block's
//! bank is still in bank forks: the cost tracker and collected fees go with
//! the bank when it is dropped after rooting.

use {
    crate::{
        metrics_tap::{Bundles, LeaderTiming, Recurrence, SlotCost, SlotWaterfall, StageTimes},
        slots::BlockDetail,
        turns::LeaderTurn,
        versions::TxVersions,
    },
    serde::{Serialize, Serializer},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, BTreeSet},
        net::IpAddr,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Execution {
    /// Thread time, not wall time.
    pub non_vote: StageTimes,
    pub workers: u64,
    pub longest_batch: u64,
    pub votes: Option<StageTimes>,
    pub window_millis: u64,
}

/// `transactions` and `non_vote_transactions` are differences against the parent; the rest are the
/// bank's own.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProducedBlock {
    pub slot: Slot,
    pub slot_time_millis: Option<u64>,
    pub blockhash: String,
    pub duration_nanos: Option<u64>,
    #[serde(flatten)]
    pub detail: BlockDetail,
    /// Absent on a stock validator or under BAM.
    pub bundles: Option<Bundles>,
    pub versions: Option<TxVersions>,
    pub execution: Option<Execution>,
    pub certificate: Option<BlockCertificate>,
    /// Where the slot's time went on our side; its parts land over the slot's last moments.
    pub leading: Option<LeaderTiming>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BlockCertificate {
    pub rewards: Slot,
    #[serde(serialize_with = "base58_or_null")]
    pub leader: Option<Pubkey>,
    pub leader_name: Option<String>,
    /// No fewer notarize votes than skip votes.
    pub notarized: bool,
    pub paid: u32,
    pub ranks: u32,
    pub stake_paid: f64,
    pub notar: u32,
    pub skip: u32,
    pub ours_in: bool,
    pub usual: Option<u32>,
    pub left_out: Vec<Validator>,
}

/// A validator as a list shows it, by its name and gossip. `release` is left out of the lists
/// that show no client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Validator {
    #[serde(serialize_with = "base58")]
    pub identity: Pubkey,
    pub name: Option<String>,
    #[serde(flatten)]
    pub release: Option<Release>,
    pub ip: Option<IpAddr>,
    pub country: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Release {
    pub client: Option<String>,
    pub version: Option<String>,
}

/// Base58 text, as `to_string` gives, written straight into the encoder.
fn base58<S: Serializer>(key: &Pubkey, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(key)
}

fn base58_or_null<S: Serializer>(key: &Option<Pubkey>, serializer: S) -> Result<S::Ok, S::Error> {
    match key {
        Some(key) => serializer.collect_str(key),
        None => serializer.serialize_none(),
    }
}

/// The previous epoch's blocks stay until this share of the new one has passed, so the page is not
/// empty just after a boundary.
pub const PREVIOUS_EPOCH_KEPT_PERCENT: u64 = 20;

/// Well past 1.2 epochs of the largest stake's slots; a guard for a floor never set.
const MAX_HELD_BLOCKS: usize = 32_768;

/// The bundle stage reports within a few slots, and the tap keeps its reports about this long.
const BUNDLE_REACH: usize = 512;

/// The leader reports land within a second of the slot; the tap keeps them far longer.
const LEADING_REACH: usize = 64;

/// Blocks in one page of figures, a few tens of kilobytes on the wire.
pub const FIGURES_PAGE: usize = 1024;

/// The widest span one detail request covers.
pub const MAX_DETAIL_SLOTS: u64 = 64;

/// The oldest slot kept, given the epoch's first slot, the previous epoch's, and how far in the
/// newest root is.
pub fn held_from(first: Slot, previous_first: Slot, index: u64, slots_in_epoch: u64) -> Slot {
    let passed = index.saturating_mul(100);
    if first == previous_first
        || passed >= slots_in_epoch.saturating_mul(PREVIOUS_EPOCH_KEPT_PERCENT)
    {
        first
    } else {
        previous_first
    }
}

/// The figures the list, its sort and its summary need, as one array per block: slot, time,
/// transactions, block cost, cost limit, total fees, priority fees, tips and duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BlockFigures(
    Slot,
    Option<u64>,
    u64,
    u64,
    u64,
    u64,
    u64,
    Option<u64>,
    Option<u64>,
);

impl From<&ProducedBlock> for BlockFigures {
    fn from(block: &ProducedBlock) -> Self {
        Self(
            block.slot,
            block.slot_time_millis,
            block.detail.transactions,
            block.detail.block_cost,
            block.detail.block_cost_limit,
            block.detail.total_fees,
            block.detail.priority_fees,
            block.detail.tips,
            block.duration_nanos,
        )
    }
}

/// A turn as the list's divider shows it: first, last, produced, drained and the previous drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TurnHead(Slot, Slot, u64, u64, Option<u64>);

impl From<&LeaderTurn> for TurnHead {
    fn from(turn: &LeaderTurn) -> Self {
        Self(
            turn.first,
            turn.last,
            turn.produced,
            turn.drained_millis,
            turn.since_millis,
        )
    }
}

/// Newest first. `next` is where the following page starts, `None` once the oldest is sent.
#[derive(Debug, Serialize)]
pub struct FiguresPage {
    pub figures: Vec<BlockFigures>,
    pub turns: Vec<TurnHead>,
    pub held: usize,
    pub floor: Slot,
    pub next: Option<Slot>,
}

/// A held block's cost, with how often its costliest account was the costliest over the blocks held.
#[derive(Debug, Serialize)]
pub struct CostDetail {
    #[serde(flatten)]
    pub cost: SlotCost,
    pub recurrence: Option<Recurrence>,
}

/// A fee payer among those who paid the most in a block, in lamports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopFeePayer {
    #[serde(serialize_with = "base58")]
    pub payer: Pubkey,
    pub transactions: u64,
    pub failed: u64,
    pub priority: u64,
    pub paid: u64,
}

/// A transaction among those that paid the most in a block, in lamports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopFeeTransaction {
    pub signature: String,
    #[serde(serialize_with = "base58")]
    pub payer: Pubkey,
    /// Absent where the status cache held no result.
    pub failed: Option<bool>,
    pub priority: u64,
    pub paid: u64,
}

/// Who paid the most in one of our blocks, over its non-vote transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockFees {
    pub slot: Slot,
    pub payer_count: u64,
    pub payers: Vec<TopFeePayer>,
    pub transactions: Vec<TopFeeTransaction>,
}

/// Everything the page draws for a span of our slots once opened.
#[derive(Debug, Serialize)]
pub struct ProducedDetail {
    pub blocks: Vec<ProducedBlock>,
    pub turns: Vec<LeaderTurn>,
    pub waterfalls: Vec<SlotWaterfall>,
    pub costs: Vec<CostDetail>,
    /// Sent only when one slot is asked for; a turn's drawer doesn't draw them.
    pub fees: Option<BlockFees>,
}

/// Our blocks and leader turns for the current epoch, and the previous one for a while after the
/// boundary; see [`held_from`].
#[derive(Debug, Default)]
pub struct ProducedStore {
    blocks: BTreeMap<Slot, ProducedBlock>,
    /// By first slot.
    turns: BTreeMap<Slot, LeaderTurn>,
    /// Blocks added or filled in since `take_changed`.
    changed: BTreeSet<Slot>,
    /// Apart from the blocks, so they stay out of the list and the live feed.
    fees: BTreeMap<Slot, BlockFees>,
    floor: Slot,
}

impl ProducedStore {
    pub fn contains(&self, slot: Slot) -> bool {
        self.blocks.contains_key(&slot)
    }

    /// The newest `count`, oldest first.
    pub fn recent_refs(&self, count: usize) -> Vec<&ProducedBlock> {
        let mut recent: Vec<&ProducedBlock> = self.blocks.values().rev().take(count).collect();
        recent.reverse();
        recent
    }

    /// The blocks added or filled in since the last call, oldest first; one since dropped is left out.
    pub fn take_changed(&mut self) -> Vec<ProducedBlock> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .filter_map(|slot| self.blocks.get(&slot).cloned())
            .collect()
    }

    /// Returns false for a slot already held or below the floor: only the first sighting has the
    /// block's figures.
    pub fn insert(&mut self, block: ProducedBlock) -> bool {
        if block.slot < self.floor || self.contains(block.slot) {
            return false;
        }
        self.changed.insert(block.slot);
        crate::metrics_tap::insert_capped(&mut self.blocks, block.slot, block, MAX_HELD_BLOCKS);
        // Fees are held only beside a held block.
        if let Some(oldest) = self.blocks.keys().next() {
            self.fees = self.fees.split_off(oldest);
        }
        true
    }

    /// Ignored for a block not held.
    pub fn set_fees(&mut self, fees: BlockFees) {
        if self.contains(fees.slot) {
            self.fees.insert(fees.slot, fees);
        }
    }

    pub fn fees(&self, slot: Slot) -> Option<BlockFees> {
        self.fees.get(&slot).cloned()
    }

    /// Drops what is older than `floor`, and refuses it from then on.
    pub fn hold_from(&mut self, floor: Slot) {
        if floor == self.floor {
            return;
        }
        self.floor = floor;
        self.blocks = self.blocks.split_off(&floor);
        self.turns = self.turns.split_off(&floor);
        self.changed = self.changed.split_off(&floor);
        self.fees = self.fees.split_off(&floor);
    }

    /// Drops the blocks above `after` up to `root` that did not root, and recounts their turns.
    /// Returns whether any went.
    pub fn drop_unrooted(
        &mut self,
        after: Slot,
        root: Slot,
        is_root: impl Fn(Slot) -> bool,
    ) -> bool {
        if after >= root {
            return false;
        }
        let gone: Vec<Slot> = self
            .blocks
            .range(after.saturating_add(1)..=root)
            .map(|(slot, _)| *slot)
            .filter(|slot| !is_root(*slot))
            .collect();
        for slot in &gone {
            self.blocks.remove(slot);
            self.fees.remove(slot);
            if let Some((_, turn)) = self.turns.range_mut(..=*slot).next_back()
                && turn.last >= *slot
            {
                turn.produced = self.blocks.range(turn.first..=turn.last).count() as u64;
            }
        }
        !gone.is_empty()
    }

    pub fn fill_bundles(&mut self, landed: impl Fn(Slot) -> Option<Bundles>) -> bool {
        let mut changed = false;
        for block in self.blocks.values_mut().rev().take(BUNDLE_REACH) {
            if block.bundles.is_none()
                && let Some(bundles) = landed(block.slot)
            {
                block.bundles = Some(bundles);
                self.changed.insert(block.slot);
                changed = true;
            }
        }
        changed
    }

    /// Timed again when this block, or the one before it, fills.
    pub fn set_duration(&mut self, slot: Slot, nanos: u64) -> bool {
        match self.blocks.get_mut(&slot) {
            Some(block) if block.duration_nanos != Some(nanos) => {
                block.duration_nanos = Some(nanos);
                self.changed.insert(slot);
                true
            }
            _ => false,
        }
    }

    /// Replaced whenever the tap holds more, since the slot's reports arrive one at a time.
    pub fn fill_leading(&mut self, timing: impl Fn(Slot) -> Option<LeaderTiming>) -> bool {
        let mut changed = false;
        for block in self.blocks.values_mut().rev().take(LEADING_REACH) {
            let held = timing(block.slot);
            if held.is_some() && held != block.leading {
                block.leading = held;
                self.changed.insert(block.slot);
                changed = true;
            }
        }
        changed
    }

    /// The blocks still waiting on their reward certificate.
    pub fn without_certificate(&self) -> Vec<Slot> {
        self.blocks
            .values()
            .filter(|block| block.certificate.is_none())
            .map(|block| block.slot)
            .collect()
    }

    pub fn without_versions(&self) -> Vec<Slot> {
        self.blocks
            .values()
            .filter(|block| block.versions.is_none())
            .map(|block| block.slot)
            .collect()
    }

    /// A held block's tips, measured again.
    pub fn set_tips(&mut self, slot: Slot, tips: u64) {
        if let Some(block) = self.blocks.get_mut(&slot)
            && block.detail.tips != Some(tips)
        {
            block.detail.tips = Some(tips);
            self.changed.insert(slot);
        }
    }

    pub fn set_versions(&mut self, slot: Slot, versions: TxVersions) -> bool {
        self.set_once(slot, versions, |block| &mut block.versions)
    }

    pub fn set_certificate(&mut self, slot: Slot, certificate: BlockCertificate) -> bool {
        self.set_once(slot, certificate, |block| &mut block.certificate)
    }

    pub fn set_execution(&mut self, slot: Slot, execution: Execution) -> bool {
        self.set_once(slot, execution, |block| &mut block.execution)
    }

    /// Sets a field still empty on a held block, and marks the block changed.
    fn set_once<T>(
        &mut self,
        slot: Slot,
        value: T,
        field: impl FnOnce(&mut ProducedBlock) -> &mut Option<T>,
    ) -> bool {
        let empty = self.blocks.get_mut(&slot).map(field);
        let Some(held) = empty.filter(|held| held.is_none()) else {
            return false;
        };
        *held = Some(value);
        self.changed.insert(slot);
        true
    }

    /// How many of `first..=last` produced a block.
    pub fn produced_in(&self, first: Slot, last: Slot) -> u64 {
        self.blocks.range(first..=last).count() as u64
    }

    /// The slots of `first..=last` held with a block, in order.
    pub fn slots_in(&self, first: Slot, last: Slot) -> impl Iterator<Item = Slot> + '_ {
        self.blocks.range(first..=last).map(|(slot, _)| *slot)
    }

    pub fn add_turn(&mut self, turn: LeaderTurn) {
        if turn.first >= self.floor {
            self.turns.insert(turn.first, turn);
        }
    }

    /// The newest `count`, oldest first.
    pub fn recent_turns(&self, count: usize) -> Vec<LeaderTurn> {
        let mut recent: Vec<LeaderTurn> = self.turns.values().rev().take(count).cloned().collect();
        recent.reverse();
        recent
    }

    /// Up to [`FIGURES_PAGE`] blocks below `before`, or from the newest, with the turns they fall in.
    pub fn figures(&self, before: Option<Slot>) -> FiguresPage {
        let below = before.unwrap_or(Slot::MAX);
        let figures: Vec<BlockFigures> = self
            .blocks
            .range(..below)
            .rev()
            .take(FIGURES_PAGE)
            .map(|(_, block)| BlockFigures::from(block))
            .collect();
        let next = (figures.len() == FIGURES_PAGE)
            .then(|| figures.last().map(|oldest| oldest.0))
            .flatten()
            .filter(|oldest| self.blocks.range(..oldest).next().is_some());
        let turns = match (figures.last(), figures.first()) {
            (Some(oldest), Some(newest)) => self
                .turns
                .range(..=newest.0)
                .rev()
                .take_while(|(_, turn)| turn.last >= oldest.0)
                .map(|(_, turn)| TurnHead::from(turn))
                .collect(),
            _ => Vec::new(),
        };
        FiguresPage {
            figures,
            turns,
            held: self.blocks.len(),
            floor: self.floor,
            next,
        }
    }

    /// The blocks and turns in `first..=last`, clamped to [`MAX_DETAIL_SLOTS`].
    pub fn detail(&self, first: Slot, last: Slot) -> (Vec<ProducedBlock>, Vec<LeaderTurn>) {
        let last = last.min(first.saturating_add(MAX_DETAIL_SLOTS.saturating_sub(1)));
        if last < first {
            return (Vec::new(), Vec::new());
        }
        let blocks = self
            .blocks
            .range(first..=last)
            .map(|(_, block)| block.clone())
            .collect();
        let turns = self
            .turns
            .range(..=last)
            .rev()
            .take_while(|(_, turn)| turn.last >= first)
            .map(|(_, turn)| turn.clone())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        (blocks, turns)
    }
}

/// A block with nought everywhere, for tests elsewhere too.
#[cfg(test)]
pub(crate) fn sample_block(slot: Slot) -> ProducedBlock {
    ProducedBlock {
        slot,
        slot_time_millis: None,
        blockhash: format!("hash{slot}"),
        duration_nanos: None,
        detail: BlockDetail::default(),
        bundles: None,
        versions: None,
        execution: None,
        certificate: None,
        leading: None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            meters::QuicPort,
            metrics_tap::{ExecutedTotals, QuicLevels, QuicTotals, VerifyTotals},
        },
        std::net::{Ipv4Addr, Ipv6Addr},
    };

    fn certificate(rewards: Slot) -> BlockCertificate {
        BlockCertificate {
            rewards,
            leader: None,
            leader_name: None,
            notarized: true,
            paid: 103,
            ranks: 112,
            stake_paid: 0.986,
            notar: 101,
            skip: 2,
            ours_in: true,
            usual: Some(103),
            left_out: Vec::new(),
        }
    }

    fn execution() -> Execution {
        Execution {
            non_vote: StageTimes {
                load_execute: 512_000,
                ..StageTimes::default()
            },
            workers: 4,
            longest_batch: 14_800,
            votes: None,
            window_millis: 402,
        }
    }

    fn turn(first: Slot, last: Slot) -> LeaderTurn {
        LeaderTurn {
            first,
            last,
            produced: 0,
            drained_millis: 0,
            since_millis: None,
            quic: QuicPort {
                name: "tpu",
                counts: QuicTotals::default(),
                levels: QuicLevels::default(),
                kernel_drops: None,
            },
            verify: VerifyTotals::default(),
            executed: ExecutedTotals::default(),
        }
    }

    fn slots<'a>(blocks: impl IntoIterator<Item = &'a ProducedBlock>) -> Vec<Slot> {
        blocks.into_iter().map(|block| block.slot).collect()
    }

    #[test]
    fn test_each_block_added_or_filled_in_is_taken_once() {
        let mut ring = ProducedStore::default();
        ring.insert(sample_block(11));
        ring.insert(sample_block(10));
        assert_eq!(slots(&ring.take_changed()), [10, 11]);
        assert!(ring.take_changed().is_empty());

        ring.set_versions(11, TxVersions::default());
        ring.set_certificate(11, certificate(3));
        ring.set_execution(12, execution());
        assert_eq!(slots(&ring.take_changed()), [11]);
        assert!(ring.recent_refs(usize::MAX)[1].certificate.is_some());
    }

    #[test]
    fn test_versions_a_certificate_and_execution_are_set_once_and_only_on_a_held_block() {
        let mut ring = ProducedStore::default();
        ring.insert(sample_block(10));
        let tally = TxVersions {
            legacy: 312,
            v0: 41,
            v1: 0,
        };
        assert!(!ring.set_versions(11, tally), "not a block we hold");
        assert!(!ring.set_certificate(11, certificate(3)));
        assert!(!ring.set_execution(11, execution()), "not a block we hold");
        assert!(ring.set_versions(10, tally));
        assert!(ring.set_certificate(10, certificate(2)));
        assert!(ring.set_execution(10, execution()));
        assert!(
            !ring.set_versions(10, TxVersions::default()),
            "already read"
        );
        assert!(!ring.set_certificate(10, certificate(2)));
        assert!(!ring.set_execution(10, execution()), "already set");
        let held = ring.recent_refs(usize::MAX)[0];
        assert_eq!(held.versions, Some(tally));
        assert_eq!(held.certificate.as_ref().map(|c| c.rewards), Some(2));
        assert_eq!(held.execution, Some(execution()));
    }

    #[test]
    fn test_a_certificate_encodes_keys_and_addresses_as_text() {
        let (leader, missing) = (Pubkey::new_unique(), Pubkey::new_unique());
        let led = BlockCertificate {
            leader: Some(leader),
            left_out: vec![
                Validator {
                    identity: missing,
                    name: Some("Lantern".to_string()),
                    release: None,
                    ip: Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))),
                    country: Some("NL"),
                },
                Validator {
                    identity: missing,
                    name: None,
                    release: None,
                    ip: Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))),
                    country: None,
                },
                Validator {
                    identity: missing,
                    name: None,
                    release: None,
                    ip: None,
                    country: None,
                },
            ],
            ..certificate(9)
        };
        assert_eq!(
            serde_json::to_string(&led).unwrap(),
            format!(
                r#"{{"rewards":9,"leader":"{leader}","leader_name":null,"notarized":true,"paid":103,"ranks":112,"stake_paid":0.986,"notar":101,"skip":2,"ours_in":true,"usual":103,"left_out":[{{"identity":"{missing}","name":"Lantern","ip":"192.0.2.10","country":"NL"}},{{"identity":"{missing}","name":null,"ip":"2001:db8::1","country":null}},{{"identity":"{missing}","name":null,"ip":null,"country":null}}]}}"#
            )
        );
        let unled = serde_json::to_string(&certificate(9)).unwrap();
        assert!(unled.contains(r#""leader":null"#), "{unled}");
    }

    #[test]
    fn test_bundles_are_filled_in_once_the_stage_reports() {
        let mut ring = ProducedStore::default();
        ring.insert(sample_block(10));
        ring.insert(sample_block(11));
        let landed = |slot: Slot| {
            (slot == 11).then_some(Bundles {
                sanitized: 17,
                executed: 14,
            })
        };
        assert!(ring.fill_bundles(landed));
        assert_eq!(ring.recent_refs(usize::MAX)[0].bundles, None);
        assert_eq!(
            ring.recent_refs(usize::MAX)[1].bundles,
            Some(Bundles {
                sanitized: 17,
                executed: 14,
            })
        );
        assert!(!ring.fill_bundles(landed), "nothing left to fill");
    }

    #[test]
    fn test_a_blocks_leader_timing_is_replaced_as_its_parts_land() {
        let mut ring = ProducedStore::default();
        ring.insert(sample_block(10));
        let part = LeaderTiming {
            open_millis: Some(400),
            ..LeaderTiming::default()
        };
        assert!(ring.fill_leading(|_| Some(part)));
        assert!(!ring.fill_leading(|_| Some(part)), "nothing new");
        let whole = LeaderTiming {
            broadcast_micros: Some(411_000),
            ..part
        };
        assert!(ring.fill_leading(|_| Some(whole)));
        assert_eq!(ring.recent_refs(usize::MAX)[0].leading, Some(whole));
        assert!(
            !ring.fill_leading(|_| None),
            "a report gone from the tap leaves the block as it was"
        );
        assert_eq!(ring.recent_refs(usize::MAX)[0].leading, Some(whole));
    }

    #[test]
    fn test_fees_are_held_with_their_block_and_named_in_base58() {
        let payer = Pubkey::new_unique();
        let fees = |slot| BlockFees {
            slot,
            payer_count: 1,
            payers: vec![TopFeePayer {
                payer,
                transactions: 1,
                failed: 0,
                priority: 5,
                paid: 5_005,
            }],
            transactions: vec![TopFeeTransaction {
                signature: "sig".to_owned(),
                payer,
                failed: None,
                priority: 5,
                paid: 5_005,
            }],
        };
        let mut ring = ProducedStore::default();
        ring.insert(sample_block(10));
        ring.insert(sample_block(20));
        ring.set_fees(fees(10));
        ring.set_fees(fees(20));
        ring.set_fees(fees(30));
        assert_eq!(ring.fees(10), Some(fees(10)));
        assert_eq!(ring.fees(30), None, "not a block we hold");

        let json = serde_json::to_value(fees(10)).unwrap();
        assert_eq!(json["payers"][0]["payer"], payer.to_string());
        assert_eq!(json["transactions"][0]["payer"], payer.to_string());
        assert!(json["transactions"][0]["failed"].is_null());

        ring.hold_from(11);
        assert_eq!(ring.fees(10), None);
        assert_eq!(ring.fees(20), Some(fees(20)));
    }

    #[test]
    fn test_slot_is_recorded_once() {
        let mut ring = ProducedStore::default();
        assert!(ring.insert(sample_block(10)));
        // Only the first sighting of a frozen bank holds the block's own figures.
        assert!(!ring.insert(sample_block(10)));
        assert_eq!(ring.recent_refs(usize::MAX).len(), 1);
    }

    #[test]
    fn test_blocks_are_held_oldest_first_however_they_arrive() {
        let mut ring = ProducedStore::default();
        for slot in [12, 10, 13, 11] {
            ring.insert(sample_block(slot));
        }
        assert_eq!(slots(ring.recent_refs(usize::MAX)), vec![10, 11, 12, 13]);
    }

    #[test]
    fn test_the_previous_epoch_is_kept_for_a_fifth_of_the_next() {
        assert_eq!(held_from(1_000, 0, 199, 1_000), 0);
        assert_eq!(held_from(1_000, 0, 200, 1_000), 1_000);
        assert_eq!(
            held_from(0, 0, 5, 1_000),
            0,
            "the first epoch has none before it"
        );
    }

    #[test]
    fn test_a_block_that_did_not_root_is_dropped_and_its_turn_recounted() {
        let mut store = ProducedStore::default();
        for slot in 100..104 {
            assert!(store.insert(sample_block(slot)));
        }
        store.set_fees(BlockFees {
            slot: 102,
            payer_count: 0,
            payers: Vec::new(),
            transactions: Vec::new(),
        });
        let mut ended = turn(100, 103);
        ended.produced = store.produced_in(100, 103);
        store.add_turn(ended);

        assert!(!store.drop_unrooted(0, 101, |_| true));
        assert!(store.drop_unrooted(101, 103, |slot| slot != 102));
        assert!(!store.contains(102));
        assert_eq!(store.fees(102), None);
        assert_eq!(store.recent_turns(1)[0].produced, 3);
        assert!(!store.drop_unrooted(103, 103, |_| false));
    }

    #[test]
    fn test_a_floor_drops_older_blocks_and_turns_and_refuses_them() {
        let mut ring = ProducedStore::default();
        for slot in [8, 9, 12, 13] {
            ring.insert(sample_block(slot));
        }
        ring.add_turn(turn(8, 9));
        ring.add_turn(turn(12, 13));
        ring.take_changed();
        ring.set_versions(9, TxVersions::default());
        ring.hold_from(12);
        assert_eq!(slots(ring.recent_refs(usize::MAX)), [12, 13]);
        assert!(
            ring.take_changed().is_empty(),
            "the change to 9 went with it"
        );
        assert_eq!(ring.recent_turns(8).len(), 1);
        assert!(!ring.insert(sample_block(10)), "below the floor");
        ring.add_turn(turn(10, 11));
        assert_eq!(ring.recent_turns(8).len(), 1);
    }

    #[test]
    fn test_figures_come_newest_first_in_pages_with_their_turns() {
        let mut ring = ProducedStore::default();
        let count = FIGURES_PAGE as u64 + 3;
        for slot in 0..count {
            ring.insert(sample_block(slot));
        }
        ring.add_turn(turn(0, 3));
        ring.add_turn(turn(count - 4, count - 1));
        let first = ring.figures(None);
        assert_eq!(first.figures.len(), FIGURES_PAGE);
        assert_eq!(first.figures[0].0, count - 1);
        assert_eq!(first.held, count as usize);
        assert_eq!(first.next, Some(3));
        // Slot 3 is the page's oldest, so the turn it falls in comes with it too.
        assert_eq!(
            first.turns,
            [
                TurnHead(count - 4, count - 1, 0, 0, None),
                TurnHead(0, 3, 0, 0, None)
            ]
        );
        let second = ring.figures(first.next);
        assert_eq!(
            second.figures.iter().map(|f| f.0).collect::<Vec<_>>(),
            [2, 1, 0]
        );
        assert_eq!(second.next, None);
        assert_eq!(second.turns, [TurnHead(0, 3, 0, 0, None)]);
    }

    #[test]
    fn test_a_full_last_page_has_no_next() {
        let mut ring = ProducedStore::default();
        for slot in 0..FIGURES_PAGE as u64 {
            ring.insert(sample_block(slot));
        }
        assert_eq!(ring.figures(None).next, None);
    }

    #[test]
    fn test_detail_is_clamped_and_carries_the_turns_it_touches() {
        let mut ring = ProducedStore::default();
        for slot in 0..200 {
            ring.insert(sample_block(slot));
        }
        ring.add_turn(turn(4, 7));
        ring.add_turn(turn(8, 11));
        let (blocks, turns) = ring.detail(6, 500);
        assert_eq!(blocks.len(), MAX_DETAIL_SLOTS as usize);
        assert_eq!(turns.iter().map(|t| t.first).collect::<Vec<_>>(), [4, 8]);
        assert!(ring.detail(9, 8).0.is_empty());
    }

    #[test]
    fn test_bundles_reach_only_the_newest_blocks() {
        let mut ring = ProducedStore::default();
        for slot in 0..(BUNDLE_REACH as u64 + 1) {
            ring.insert(sample_block(slot));
        }
        let landed = |_slot: Slot| {
            Some(Bundles {
                sanitized: 1,
                executed: 1,
            })
        };
        ring.fill_bundles(landed);
        let held = ring.recent_refs(usize::MAX);
        assert_eq!(held[0].bundles, None);
        assert!(held[1].bundles.is_some());
    }
}
