//! A flat history of what each recent slot contained: fixed-size rows with
//! only the columns the schedule page draws, an epoch deep.

use {
    crate::{certs::Reward, slots::SlotEntry},
    serde::{Serialize, Serializer},
    solana_clock::Slot,
};

/// An epoch, two days at 400 ms, in thirty-eight megabytes, allocated at startup since the server
/// answers range queries before the slot feed has run.
pub const PACKED_SLOTS: usize = 432_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PackedSlot {
    pub level: u8,
    pub flags: u16,
    pub votes: u32,
    pub non_votes: u32,
    /// Saturating into `u32`, seventy times the current block limit.
    pub compute: u32,
    /// In lamports; base is `fees - priority_fees`.
    pub fees: u64,
    pub priority_fees: u64,
    pub tips: u64,
    pub replay_micros: u32,
    pub time_millis: u64,
    pub shreds: u32,
    pub repaired: u32,
    pub full_millis: u32,
    pub replayed_millis: u32,
    pub left_out: u16,
    /// Saturating into `u16`, which keeps the row at its size.
    pub recovered: u16,
    pub failed: u32,
    /// Basis points of stake in the notarize reward certificate a later footer carried.
    pub notarized: u16,
}

/// A full span of mainnet-sized rows is under half the frame ceiling; a test holds it there.
pub const MAX_RANGE_SLOTS: usize = 4096;

/// One slot on the wire, a JSON array: level, flags, votes, non-votes, compute, fees, priority
/// fees, tips, time, replay, shreds, repaired, full, replayed, left out, recovered, failed. The
/// frontend mirrors it; `notarized` is not sent.
impl Serialize for PackedSlot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        [
            u64::from(self.level),
            u64::from(self.flags),
            u64::from(self.votes),
            u64::from(self.non_votes),
            u64::from(self.compute),
            self.fees,
            self.priority_fees,
            self.tips,
            self.time_millis,
            u64::from(self.replay_micros),
            u64::from(self.shreds),
            u64::from(self.repaired),
            u64::from(self.full_millis),
            u64::from(self.replayed_millis),
            u64::from(self.left_out),
            u64::from(self.recovered),
            u64::from(self.failed),
        ]
        .serialize(serializer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotRange {
    /// Every row after it is one slot on, so slot numbers are never sent.
    pub first_slot: Slot,
    pub rows: Vec<Option<PackedSlot>>,
}

pub const HAS_BLOCK: u16 = 1;
pub const HAS_CLOCK: u16 = 1 << 1;
/// Nought is a real reading: the searchers passed that leader by.
pub const HAS_TIPS: u16 = 1 << 2;
pub const HAS_REPLAY: u16 = 1 << 3;
pub const HAS_SHREDS: u16 = 1 << 4;
pub const HAS_REPLAYED: u16 = 1 << 5;
/// Two bits for the reward certificate's verdict on this node's vote: unseen,
/// paid, unpaid, or none because no rooted block carried one.
pub const REWARD_SHIFT: u16 = 6;
pub const REWARD_MASK: u16 = 0b11 << REWARD_SHIFT;
pub const REWARD_PAID: u16 = 1 << REWARD_SHIFT;
pub const REWARD_UNPAID: u16 = 2 << REWARD_SHIFT;
pub const REWARD_NONE: u16 = 3 << REWARD_SHIFT;
pub const HAS_NOTARIZED: u16 = 1 << 8;
/// The block's footer carried a finalization certificate, the fast one where `FAST_FINAL` is set.
pub const HAS_FINAL_CERT: u16 = 1 << 9;
pub const FAST_FINAL: u16 = 1 << 10;

fn reward_bits(reward: Option<Reward>) -> u16 {
    match reward {
        None => 0,
        Some(Reward::Paid) => REWARD_PAID,
        Some(Reward::Unpaid) => REWARD_UNPAID,
        Some(Reward::NoCertificate) => REWARD_NONE,
    }
}

/// The slot is kept beside its row so a row from a lap ago cannot answer for a current one.
pub struct SlotHistory {
    rows: Vec<(Slot, PackedSlot)>,
    /// Slots below this were replayed while catching up after a start.
    pub live_from: Slot,
}

impl SlotHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: vec![(0, PackedSlot::default()); capacity.max(1)],
            live_from: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.rows.len()
    }

    pub fn get(&self, slot: Slot) -> Option<&PackedSlot> {
        let (held, row) = self.rows.get(self.index(slot))?;
        // Unfilled rows hold slot 0, so slot 0 itself is never reported.
        (*held == slot && slot != 0).then_some(row)
    }

    pub fn range(&self, first_slot: Slot, count: usize) -> SlotRange {
        let count = count.min(MAX_RANGE_SLOTS);
        let rows = (0..count as u64)
            .map(|offset| self.get(first_slot.saturating_add(offset)).copied())
            .collect();
        SlotRange { first_slot, rows }
    }

    pub fn record(&mut self, entry: &SlotEntry) {
        let row = self.row(entry.slot);
        row.level = entry.level as u8;
        if let Some(block) = &entry.block {
            row.flags |= HAS_BLOCK;
            row.votes = clamp(
                block
                    .transactions
                    .saturating_sub(block.non_vote_transactions),
            );
            row.non_votes = clamp(block.non_vote_transactions);
            row.failed = clamp(block.failed_transactions);
            row.compute = clamp(block.block_cost);
            row.fees = block.total_fees;
            row.priority_fees = block.priority_fees;
            if let Some(tips) = block.tips {
                row.flags |= HAS_TIPS;
                row.tips = tips;
            }
            if let Some(micros) = block.replay_micros {
                row.flags |= HAS_REPLAY;
                row.replay_micros = clamp(micros);
            }
        }
        if let Some(shreds) = &entry.shreds {
            row.flags |= HAS_SHREDS;
            row.shreds = clamp(shreds.count);
            row.repaired = clamp(shreds.repaired);
            row.recovered = u16::try_from(shreds.recovered).unwrap_or(u16::MAX);
            row.full_millis = clamp(shreds.full_millis);
        }
        if let Some(millis) = entry.replayed_millis {
            row.flags |= HAS_REPLAYED;
            row.replayed_millis = clamp(millis);
        }
        row.flags = (row.flags & !REWARD_MASK) | reward_bits(entry.reward);
        row.left_out = entry.left_out.unwrap_or(0);
    }

    /// One row written directly, for tests elsewhere in the crate.
    #[cfg(test)]
    pub(crate) fn record_test_row(&mut self, slot: Slot, fill: impl FnOnce(&mut PackedSlot)) {
        fill(self.row(slot));
    }

    pub fn record_notarized(&mut self, slot: Slot, basis_points: u16) {
        let row = self.row(slot);
        row.flags |= HAS_NOTARIZED;
        row.notarized = basis_points;
    }

    pub fn record_final_path(&mut self, slot: Slot, fast: bool) {
        let row = self.row(slot);
        row.flags = (row.flags & !FAST_FINAL) | HAS_FINAL_CERT | if fast { FAST_FINAL } else { 0 };
    }

    pub fn record_time(&mut self, slot: Slot, millis: u64) {
        let row = self.row(slot);
        row.flags |= HAS_CLOCK;
        row.time_millis = millis;
    }

    /// Every figure as large as a real slot's gets, for the tests that bound a reply's size.
    #[cfg(test)]
    pub(crate) fn record_worst_case(&mut self, slot: Slot) {
        *self.row(slot) = PackedSlot {
            level: 4,
            flags: HAS_BLOCK | HAS_CLOCK | HAS_TIPS | HAS_REPLAY | HAS_SHREDS | HAS_REPLAYED,
            votes: 99_999,
            non_votes: 99_999,
            compute: u32::MAX,
            fees: 9_999_999_999_999,
            priority_fees: 9_999_999_999_999,
            tips: 9_999_999_999_999,
            replay_micros: 9_999_999,
            time_millis: 1_999_999_999_999,
            shreds: 99_999,
            repaired: 99_999,
            full_millis: 999_999,
            replayed_millis: 999_999,
            left_out: 9_999,
            recovered: 9_999,
            failed: 99_999,
            notarized: 10_000,
        };
    }

    fn row(&mut self, slot: Slot) -> &mut PackedSlot {
        let index = self.index(slot);
        let held = &mut self.rows[index];
        if held.0 != slot {
            *held = (slot, PackedSlot::default());
        }
        &mut held.1
    }

    /// Without a bare remainder: the workspace denies `arithmetic_side_effects`.
    fn index(&self, slot: Slot) -> usize {
        let capacity = self.rows.len() as u64;
        usize::try_from(slot.checked_rem(capacity).unwrap_or(0)).unwrap_or(0)
    }
}

fn clamp(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::slots::{BlockDetail, ShredArrival, SlotLevel},
    };

    fn entry(slot: Slot) -> SlotEntry {
        SlotEntry {
            slot,
            level: SlotLevel::Rooted,
            ..SlotEntry::default()
        }
    }

    #[test]
    fn test_the_reward_verdict_packs_into_two_bits() {
        let mut history = SlotHistory::new(64);
        for (reward, bits) in [
            (Some(Reward::Paid), REWARD_PAID),
            (Some(Reward::Unpaid), REWARD_UNPAID),
            (Some(Reward::NoCertificate), REWARD_NONE),
            (None, 0),
        ] {
            let mut seen = entry(10);
            seen.reward = reward;
            history.record(&seen);
            assert_eq!(history.get(10).expect("recorded").flags & REWARD_MASK, bits);
        }
    }

    fn with_block(slot: Slot, transactions: u64, non_vote: u64) -> SlotEntry {
        SlotEntry {
            block: Some(BlockDetail {
                transactions,
                non_vote_transactions: non_vote,
                block_cost: 41_827_311,
                block_cost_limit: 60_000_000,
                account_cost_limit: 12_000_000,
                total_fees: 104_600_000,
                ..BlockDetail::default()
            }),
            ..entry(slot)
        }
    }

    #[test]
    fn test_a_slot_reads_back_what_was_recorded_for_it() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, 9_500, 8_752));
        history.record_time(900, 1_756_000_000_123);

        let row = history.get(900).expect("recorded");
        assert_eq!(row.votes, 748);
        assert_eq!(row.non_votes, 8_752);
        assert_eq!(row.compute, 41_827_311);
        assert_eq!(row.fees, 104_600_000);
        assert_eq!(row.time_millis, 1_756_000_000_123);
        assert_eq!(row.flags, HAS_BLOCK | HAS_CLOCK);
    }

    #[test]
    fn test_the_two_writers_do_not_clear_each_other() {
        // The block arrives when the bank freezes and the clock when the
        // blockstore is walked, on different ticks and in either order.
        for clock_first in [true, false] {
            let mut history = SlotHistory::new(64);
            if clock_first {
                history.record_time(900, 42);
                history.record(&with_block(900, 10, 4));
            } else {
                history.record(&with_block(900, 10, 4));
                history.record_time(900, 42);
            }
            let row = history.get(900).expect("recorded");
            assert_eq!(row.time_millis, 42, "clock_first={clock_first}");
            assert_eq!(row.non_votes, 4, "clock_first={clock_first}");
            assert_eq!(row.flags, HAS_BLOCK | HAS_CLOCK);
        }
    }

    #[test]
    fn test_a_slot_a_lap_ago_is_not_mistaken_for_this_one() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 5_000, 4_000));
        history.record(&with_block(74, 9, 4));

        assert!(history.get(10).is_none());
        assert_eq!(history.get(74).expect("recorded").non_votes, 4);
    }

    #[test]
    fn test_a_lapped_row_keeps_nothing_of_the_slot_it_held() {
        // Cleared rather than overwritten, so the old slot's counts cannot answer for the new one.
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 5_000, 4_000));
        history.record_time(10, 99);
        history.record(&entry(74));

        let row = history.get(74).expect("recorded");
        assert_eq!(row.flags, 0);
        assert_eq!(row.non_votes, 0);
        assert_eq!(row.time_millis, 0);
    }

    #[test]
    fn test_a_range_is_positional_and_holds_a_gap_open() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(10, 10, 4));
        history.record(&with_block(12, 20, 9));

        let range = history.range(10, 3);
        assert_eq!(range.first_slot, 10);
        assert_eq!(range.rows.len(), 3);
        assert!(range.rows[0].is_some());
        assert!(range.rows[1].is_none(), "slot 11 was never recorded");
        assert_eq!(range.rows[2].expect("slot 12").non_votes, 9);
    }

    #[test]
    fn test_a_range_off_the_history_is_all_holes_and_clamped_rather_than_refused() {
        let history = SlotHistory::new(64);
        let range = history.range(10, MAX_RANGE_SLOTS.saturating_add(1_000));
        assert_eq!(range.rows.len(), MAX_RANGE_SLOTS);
        let range = history.range(900, 4);
        assert_eq!(range.rows.len(), 4);
        assert!(range.rows.iter().all(Option::is_none));
    }

    #[test]
    fn test_range_column_order() {
        // The one place the wire order is pinned.
        let mut history = SlotHistory::new(64);
        let mut block = with_block(10, 9_500, 8_752);
        block.block.as_mut().unwrap().failed_transactions = 37;
        history.record(&block);
        history.record_time(10, 1_756_000_000_123);

        let row = history.range(10, 1).rows[0].expect("recorded");
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            serde_json::json!([
                SlotLevel::Rooted as u8,
                HAS_BLOCK | HAS_CLOCK,
                748,
                8_752,
                41_827_311,
                104_600_000,
                0,
                0,
                1_756_000_000_123_u64,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                37
            ])
        );
    }

    #[test]
    fn test_footer_readings_set_their_flags_and_keep_the_rest() {
        let mut history = SlotHistory::new(16);
        history.record(&entry(5));
        history.record_notarized(5, 9_950);
        history.record_final_path(5, true);
        let row = *history.get(5).unwrap();
        assert_eq!(row.notarized, 9_950);
        assert_eq!(
            row.flags & (HAS_NOTARIZED | HAS_FINAL_CERT | FAST_FINAL),
            HAS_NOTARIZED | HAS_FINAL_CERT | FAST_FINAL
        );
        history.record_final_path(5, false);
        history.record(&entry(5));
        let row = *history.get(5).unwrap();
        assert_eq!(
            row.flags & (HAS_FINAL_CERT | FAST_FINAL),
            HAS_FINAL_CERT,
            "a later read replaces the path"
        );
        assert_eq!(
            row.flags & HAS_NOTARIZED,
            HAS_NOTARIZED,
            "a refreshed entry keeps the reading"
        );
    }

    #[test]
    fn test_a_held_row_stays_eighty_eight_bytes() {
        // 432,000 of them is the thirty-eight megabytes the ring is sized by.
        assert_eq!(std::mem::size_of::<(Slot, PackedSlot)>(), 88);
    }

    #[test]
    fn test_shred_arrival_and_replay_end_travel_with_their_flags() {
        let mut history = SlotHistory::new(64);
        let mut filled = entry(50);
        filled.shreds = Some(ShredArrival {
            count: 928,
            repaired: 63,
            recovered: 12,
            full_millis: 921,
        });
        history.record(&filled);
        let mut replayed = with_block(51, 100, 10);
        replayed.shreds = Some(ShredArrival {
            count: 1_203,
            repaired: 0,
            recovered: 70_000,
            full_millis: 341,
        });
        replayed.replayed_millis = Some(393);
        history.record(&replayed);

        let dead = history.get(50).expect("recorded");
        assert_eq!(dead.flags, HAS_SHREDS);
        assert_eq!(
            (dead.shreds, dead.repaired, dead.recovered, dead.full_millis),
            (928, 63, 12, 921)
        );
        assert_eq!(dead.replayed_millis, 0);

        let live = history.get(51).expect("recorded");
        assert_eq!(live.flags, HAS_BLOCK | HAS_SHREDS | HAS_REPLAYED);
        assert_eq!(live.recovered, u16::MAX);
        assert_eq!(live.full_millis, 341);
        assert_eq!(live.replayed_millis, 393);
    }

    #[test]
    fn test_full_range_fits_the_message_ceiling() {
        // Every figure as large as a real slot's gets; the worst case the types
        // allow does not fit and never did.
        let mut history = SlotHistory::new(MAX_RANGE_SLOTS);
        for slot in 0..MAX_RANGE_SLOTS as Slot {
            let mut big = with_block(slot, 99_999, 99_999);
            let block = big.block.as_mut().unwrap();
            block.failed_transactions = 9_999;
            block.block_cost = u64::from(u32::MAX);
            block.total_fees = 9_999_999_999_999;
            block.priority_fees = 9_999_999_999_999;
            block.tips = Some(9_999_999_999_999);
            block.replay_micros = Some(9_999_999);
            big.shreds = Some(ShredArrival {
                count: 99_999,
                repaired: 99_999,
                recovered: 9_999,
                full_millis: 999_999,
            });
            big.replayed_millis = Some(999_999);
            history.record(&big);
            history.record_time(slot, 1_999_999_999_999);
        }

        let encoded = crate::proto::encode("slot", "range", &history.range(0, MAX_RANGE_SLOTS));
        crate::proto::assert_under("a full range", &encoded, crate::proto::MAX_MESSAGE / 2);
    }

    #[test]
    fn test_replay_time_travels_with_its_flag() {
        let mut history = SlotHistory::new(64);
        let mut replayed = with_block(40, 100, 10);
        replayed.block.as_mut().unwrap().replay_micros = Some(47_200);
        history.record(&replayed);
        history.record(&with_block(41, 100, 10));

        let read = history.get(40).expect("recorded");
        assert_eq!(read.flags & HAS_REPLAY, HAS_REPLAY);
        assert_eq!(read.replay_micros, 47_200);
        let built = history.get(41).expect("recorded");
        assert_eq!(built.flags & HAS_REPLAY, 0);
    }

    #[test]
    fn test_tips_of_nought_are_not_tips_that_were_never_read() {
        // A turn the searchers passed by and one never measured must not draw the same.
        let mut history = SlotHistory::new(64);

        let mut measured = with_block(20, 100, 10);
        measured.block.as_mut().unwrap().tips = Some(0);
        history.record(&measured);
        history.record(&with_block(21, 100, 10));

        let read = history.get(20).expect("recorded");
        assert_eq!(read.flags & HAS_TIPS, HAS_TIPS);
        assert_eq!(read.tips, 0);

        let unread = history.get(21).expect("recorded");
        assert_eq!(unread.flags & HAS_TIPS, 0);
        assert_eq!(unread.tips, 0);
    }

    #[test]
    fn test_the_two_kinds_of_fee_are_kept_apart() {
        let mut history = SlotHistory::new(64);
        let mut block = with_block(30, 100, 10);
        let detail = block.block.as_mut().unwrap();
        detail.total_fees = 104_600_000;
        detail.priority_fees = 99_000_000;
        history.record(&block);

        let read = history.get(30).expect("recorded");
        assert_eq!(read.fees, 104_600_000);
        assert_eq!(read.priority_fees, 99_000_000);
        assert_eq!(read.fees.saturating_sub(read.priority_fees), 5_600_000);
    }

    #[test]
    fn test_empty_block_differs_from_no_block() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, 0, 0));
        history.record(&entry(901));

        assert_eq!(
            history.get(900).expect("recorded").flags & HAS_BLOCK,
            HAS_BLOCK
        );
        assert_eq!(history.get(901).expect("recorded").flags & HAS_BLOCK, 0);
    }

    #[test]
    fn test_a_count_past_the_row_is_clamped_rather_than_wrapped() {
        let mut history = SlotHistory::new(64);
        history.record(&with_block(900, u64::MAX, 0));
        assert_eq!(history.get(900).expect("recorded").votes, u32::MAX);
    }
}
