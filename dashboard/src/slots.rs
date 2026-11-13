//! Rolling history of recent slots, backing the slot strip and the schedule.

use {
    crate::certs::Reward,
    serde::{Serialize, Serializer},
    solana_clock::Slot,
    std::collections::BTreeMap,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotLevel {
    #[default]
    Incomplete,
    Completed,
    OptimisticallyConfirmed,
    Rooted,
    Finalized,
    Skipped,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SlotEntry {
    pub slot: Slot,
    pub level: SlotLevel,
    pub mine: bool,
    #[serde(serialize_with = "with_replay")]
    pub block: Option<BlockDetail>,
    /// In nanoseconds, from the last earlier block to fill here to this one filling, so it lines up
    /// with the slot's own work rather than the one before it.
    pub duration_nanos: Option<u64>,
    pub time_millis: Option<u64>,
    pub shreds: Option<ShredArrival>,
    /// `None` for a bank this validator built, which replay never timed.
    pub replayed_millis: Option<u64>,
    /// `None` until the reward certificate is seen, and always under TowerBFT.
    pub reward: Option<Reward>,
    pub left_out: Option<u16>,
}

/// Apart from the block because a slot fills before it freezes, and a dead slot never freezes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ShredArrival {
    pub count: u64,
    pub repaired: u64,
    /// Rebuilt here from erasure coding rather than received.
    pub recovered: u64,
    pub full_millis: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct BlockDetail {
    pub transactions: u64,
    pub non_vote_transactions: u64,
    /// The bank's own counter, reset per bank, so already per block.
    pub failed_transactions: u64,
    pub entries: u64,
    pub block_cost: u64,
    pub block_cost_limit: u64,
    pub account_cost_limit: u64,
    /// Base and priority together: `total_transaction_fee` adds the two despite its name.
    pub total_fees: u64,
    pub priority_fees: u64,
    pub tips: Option<u64>,
    /// Sent with the slot's entry only, as the last of the block's figures.
    #[serde(skip)]
    pub replay_micros: Option<u64>,
}

/// A slot's block with its replay time, which a produced block leaves out.
fn with_replay<S: Serializer>(
    block: &Option<BlockDetail>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Replayed<'a> {
        #[serde(flatten)]
        block: &'a BlockDetail,
        replay_micros: Option<u64>,
    }
    block
        .as_ref()
        .map(|block| Replayed {
            block,
            replay_micros: block.replay_micros,
        })
        .serialize(serializer)
}

impl SlotEntry {
    /// When the block became full here, on the blockstore's clock.
    pub fn full_at(&self) -> Option<u64> {
        Some(
            self.time_millis?
                .saturating_add(self.shreds.as_ref()?.full_millis),
        )
    }
}

const OWN_SLOTS_KEPT: usize = 64;

pub struct SlotRing {
    entries: BTreeMap<Slot, SlotEntry>,
    capacity: usize,
}

impl SlotRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity: capacity.max(1),
        }
    }

    pub fn get(&self, slot: Slot) -> Option<&SlotEntry> {
        self.entries.get(&slot)
    }

    pub fn recent(&self, count: usize) -> Vec<SlotEntry> {
        let skip = self.entries.len().saturating_sub(count);
        self.entries.values().skip(skip).cloned().collect()
    }

    pub fn overview(&self, count: usize) -> Vec<SlotEntry> {
        let recent = self.recent(count);
        let floor = recent.first().map_or(Slot::MAX, |entry| entry.slot);
        let mut overview: Vec<SlotEntry> = self
            .entries
            .values()
            .filter(|entry| entry.mine && entry.slot < floor)
            .cloned()
            .collect();
        overview.extend(recent);
        overview
    }

    /// Returns the entry only if something changed, so idle polling sends nothing.
    pub fn update(&mut self, slot: Slot, update: impl FnOnce(&mut SlotEntry)) -> Option<SlotEntry> {
        let before = self.entries.get(&slot).cloned();
        let entry = self.entries.entry(slot).or_insert_with(|| SlotEntry {
            slot,
            ..SlotEntry::default()
        });
        update(entry);

        // A level never moves backwards: replay and the commitment cache are sampled
        // independently.
        if let Some(before) = &before
            && entry.level < before.level
        {
            entry.level = before.level;
        }

        let changed = before.as_ref() != Some(&*entry);
        let result = changed.then(|| entry.clone());

        self.prune();
        result
    }

    fn prune(&mut self) {
        if self.entries.len() <= self.capacity {
            return;
        }
        // Split by ownership: skipping ours oldest first would delete newer slots to make room.
        let mut own = Vec::new();
        let mut rest = Vec::new();
        for (&slot, entry) in &self.entries {
            if entry.mine { &mut own } else { &mut rest }.push(slot);
        }
        // Our own slots count against the ring's capacity, or the prune guard stays tripped.
        let drop_own = own.len().saturating_sub(OWN_SLOTS_KEPT);
        let kept_own = own.len().saturating_sub(drop_own);
        let drop_rest = rest
            .len()
            .saturating_sub(self.capacity.saturating_sub(kept_own));
        for slot in own
            .into_iter()
            .take(drop_own)
            .chain(rest.into_iter().take(drop_rest))
        {
            self.entries.remove(&slot);
        }
    }

    pub fn promote(&mut self, up_to: Slot, level: SlotLevel) -> Vec<SlotEntry> {
        let candidates: Vec<Slot> = self
            .entries
            .iter()
            .filter(|(slot, entry)| {
                **slot <= up_to
                    && entry.level < level
                    // A slot that was never produced does not become rooted.
                    && entry.level != SlotLevel::Incomplete
            })
            .map(|(&slot, _)| slot)
            .collect();
        candidates
            .into_iter()
            .filter_map(|slot| self.update(slot, |entry| entry.level = level))
            .collect()
    }

    pub fn set_mine(&mut self, slot: Slot, mine: bool) -> Option<SlotEntry> {
        self.update(slot, |entry| entry.mine = mine)
    }

    /// The slots below `up_to` never completed here, oldest first.
    pub fn incomplete_below(&self, up_to: Slot) -> Vec<Slot> {
        self.entries
            .range(..up_to)
            .filter(|(_, entry)| entry.level == SlotLevel::Incomplete)
            .map(|(&slot, _)| slot)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_slot_records_whether_it_is_ours_and_marking_it_twice_reports_no_change() {
        let mut ring = SlotRing::new(16);
        assert!(ring.set_mine(7, true).expect("a change").mine);
        assert!(ring.set_mine(7, true).is_none());
    }

    #[test]
    fn test_the_largest_slot_snapshots_fit_the_message_ceiling() {
        // Worst case throughout: a full ring, every counter at its ceiling. The 512
        // mirrors `SLOT_OVERVIEW_LEN` in `collect`.
        let worst = |count: usize| -> Vec<SlotEntry> {
            (0..count as u64)
                .map(|index| SlotEntry {
                    slot: 428_804_675 + index,
                    level: SlotLevel::OptimisticallyConfirmed,
                    mine: true,
                    time_millis: Some(u64::MAX),
                    shreds: Some(ShredArrival {
                        count: u64::MAX,
                        repaired: u64::MAX,
                        recovered: u64::MAX,
                        full_millis: u64::MAX,
                    }),
                    replayed_millis: Some(u64::MAX),
                    reward: Some(Reward::NoCertificate),
                    left_out: Some(u16::MAX),
                    block: Some(BlockDetail {
                        transactions: u64::MAX,
                        non_vote_transactions: u64::MAX,
                        failed_transactions: u64::MAX,
                        entries: u64::MAX,
                        block_cost: u64::MAX,
                        block_cost_limit: u64::MAX,
                        account_cost_limit: u64::MAX,
                        total_fees: u64::MAX,
                        priority_fees: u64::MAX,
                        tips: Some(u64::MAX),
                        replay_micros: Some(u64::MAX),
                    }),
                    duration_nanos: Some(u64::MAX),
                })
                .collect()
        };

        let encoded = crate::proto::encode("slot", "overview", &worst(512 + OWN_SLOTS_KEPT));
        crate::proto::assert_under("worst-case overview", &encoded, crate::proto::MAX_MESSAGE);
    }

    fn ours(ring: &SlotRing) -> Vec<Slot> {
        ring.entries
            .values()
            .filter(|entry| entry.mine)
            .map(|entry| entry.slot)
            .collect()
    }

    #[test]
    fn test_our_own_slots_outlive_the_window() {
        let mut ring = SlotRing::new(8);
        for slot in 1..=4 {
            ring.update(slot, |entry| entry.mine = true);
        }
        for slot in 5..=200 {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }
        assert_eq!(ours(&ring), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_own_retention_is_bounded() {
        let led = (OWN_SLOTS_KEPT as Slot).saturating_add(100);
        let mut ring = SlotRing::new(8);
        for slot in 1..=led {
            ring.update(slot, |entry| entry.mine = true);
        }
        for slot in led.saturating_add(1)..=led.saturating_add(200) {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }
        let kept = ours(&ring);
        assert_eq!(kept.len(), OWN_SLOTS_KEPT);
        assert_eq!(kept.last(), Some(&led));
        assert_eq!(kept.first(), Some(&101));
    }

    #[test]
    fn test_keeping_our_own_costs_the_oldest_ordinary_slot() {
        let mut ring = SlotRing::new(8);
        ring.update(1, |entry| entry.mine = true);
        for slot in 2..=100 {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }
        // Seven ordinary slots and the one of ours, filling the capacity. What the
        // retained slot costs is the oldest ordinary one.
        let recent: Vec<Slot> = ring.recent(8).iter().map(|entry| entry.slot).collect();
        assert_eq!(recent, vec![1, 94, 95, 96, 97, 98, 99, 100]);
    }

    #[test]
    fn test_pruning_settles_where_the_guard_will_leave_it_alone() {
        // `prune` returns early at `capacity`, so it has to prune to at most that.
        let mut ring = SlotRing::new(256);
        for slot in 1..=80 {
            ring.update(slot, |entry| entry.mine = true);
        }
        for slot in 81..=2_000 {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }

        let settled = ring.entries.len();
        assert!(
            settled <= ring.capacity,
            "settled at {settled}, above the {} the guard returns early at",
            ring.capacity
        );

        ring.prune();
        assert_eq!(ring.entries.len(), settled);
    }

    #[test]
    fn test_overview_carries_own_slots_before_the_window() {
        let mut ring = SlotRing::new(512);
        for slot in [1, 2] {
            ring.update(slot, |entry| entry.mine = true);
        }
        for slot in 3..=100 {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }
        let slots: Vec<Slot> = ring.overview(10).iter().map(|entry| entry.slot).collect();

        assert_eq!(&slots[..2], &[1, 2]);
        assert_eq!(slots.last(), Some(&100));
        assert!(
            slots.windows(2).all(|pair| pair[0] < pair[1]),
            "the overview must stay ordered and hold nothing twice: {slots:?}"
        );
    }

    #[test]
    fn test_an_own_slot_inside_the_window_is_not_sent_twice() {
        let mut ring = SlotRing::new(512);
        for slot in 1..=20 {
            ring.update(slot, |entry| entry.level = SlotLevel::Rooted);
        }
        ring.update(18, |entry| entry.mine = true);

        let slots: Vec<Slot> = ring.overview(5).iter().map(|entry| entry.slot).collect();
        assert_eq!(slots, vec![16, 17, 18, 19, 20]);
    }

    #[test]
    fn test_level_never_regresses() {
        let mut ring = SlotRing::new(16);
        ring.update(10, |entry| entry.level = SlotLevel::Rooted);
        assert!(
            ring.update(10, |entry| entry.level = SlotLevel::Completed)
                .is_none()
        );
        assert_eq!(ring.get(10).unwrap().level, SlotLevel::Rooted);
    }

    #[test]
    fn test_update_reports_only_real_changes() {
        let mut ring = SlotRing::new(16);
        assert!(
            ring.update(1, |entry| entry.level = SlotLevel::Completed)
                .is_some()
        );
        assert!(
            ring.update(1, |entry| entry.level = SlotLevel::Completed)
                .is_none()
        );
    }

    #[test]
    fn test_ring_is_bounded() {
        let mut ring = SlotRing::new(4);
        for slot in 0..10 {
            ring.update(slot, |entry| entry.level = SlotLevel::Completed);
        }
        assert_eq!(ring.entries.len(), 4);
        assert!(ring.get(0).is_none());
        assert!(ring.get(9).is_some());
    }

    #[test]
    fn test_promote_advances_replayed_slots_only() {
        let mut ring = SlotRing::new(16);
        ring.update(1, |entry| entry.level = SlotLevel::Completed);
        ring.update(2, |_| {}); // never replayed
        ring.update(3, |entry| entry.level = SlotLevel::Completed);

        let promoted = ring.promote(2, SlotLevel::Rooted);
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].slot, 1);
        assert_eq!(ring.get(2).unwrap().level, SlotLevel::Incomplete);
        assert_eq!(ring.get(3).unwrap().level, SlotLevel::Completed);
    }

    #[test]
    fn test_promote_does_not_demote() {
        let mut ring = SlotRing::new(16);
        ring.update(1, |entry| entry.level = SlotLevel::Finalized);
        assert!(ring.promote(1, SlotLevel::Rooted).is_empty());
        assert_eq!(ring.get(1).unwrap().level, SlotLevel::Finalized);
    }
}
