//! Leader turns: runs of consecutive leader slots, with the TPU path's
//! totals differenced at each turn's end.

use {
    crate::{
        meters::QuicPort,
        metrics_tap::{ExecutedTotals, VerifyTotals},
    },
    serde::Serialize,
    solana_clock::Slot,
    std::collections::VecDeque,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderTurn {
    pub first: Slot,
    pub last: Slot,
    pub produced: u64,
    /// In unix milliseconds.
    pub drained_millis: u64,
    /// `None` for the first turn seen.
    pub since_millis: Option<u64>,
    pub quic: QuicPort,
    pub verify: VerifyTotals,
    pub executed: ExecutedTotals,
}

#[derive(Debug, Default)]
pub struct TurnTracker {
    open: Option<(Slot, Slot)>,
    closed: VecDeque<(Slot, Slot)>,
}

impl TurnTracker {
    /// In slot order.
    pub fn observe(&mut self, slot: Slot, mine: bool) {
        if !mine {
            return;
        }
        match &mut self.open {
            Some((_, last)) if last.saturating_add(1) == slot => *last = slot,
            open => self.closed.extend(open.replace((slot, slot))),
        }
    }

    pub fn ended(&mut self, completed: Slot) -> Vec<(Slot, Slot)> {
        let count = self
            .closed
            .iter()
            .take_while(|(_, last)| *last < completed)
            .count();
        let mut ended: Vec<(Slot, Slot)> = self.closed.drain(..count).collect();
        ended.extend(self.open.take_if(|(_, last)| *last < completed));
        ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn told(tracker: &mut TurnTracker, slots: &[(Slot, bool)]) {
        for &(slot, mine) in slots {
            tracker.observe(slot, mine);
        }
    }

    #[test]
    fn test_consecutive_leader_slots_are_one_turn() {
        let mut tracker = TurnTracker::default();
        told(
            &mut tracker,
            &[
                (7, false),
                (8, true),
                (9, true),
                (10, true),
                (11, true),
                (12, false),
            ],
        );
        assert_eq!(tracker.ended(11), vec![], "the cluster has not passed it");
        assert_eq!(tracker.ended(12), vec![(8, 11)]);
        assert_eq!(tracker.ended(20), vec![], "reported once");
    }

    #[test]
    fn test_two_groups_back_to_back_are_one_turn_of_eight() {
        let mut tracker = TurnTracker::default();
        told(
            &mut tracker,
            &(8..16).map(|slot| (slot, true)).collect::<Vec<_>>(),
        );
        tracker.observe(16, false);
        assert_eq!(tracker.ended(17), vec![(8, 15)]);
    }

    #[test]
    fn test_a_gap_starts_a_new_turn_and_both_end_in_order() {
        let mut tracker = TurnTracker::default();
        told(
            &mut tracker,
            &[(8, true), (9, true), (10, false), (11, true)],
        );
        assert_eq!(tracker.ended(11), vec![(8, 9)]);
        assert_eq!(tracker.ended(12), vec![(11, 11)]);
    }
}
