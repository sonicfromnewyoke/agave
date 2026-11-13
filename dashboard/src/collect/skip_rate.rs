//! This node's skip rate for the epoch: of its leader slots the root has passed, how many have no
//! block in the chain.

use {
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_slot_history::Check,
};

/// One epoch's leader slots for this validator, as far as the root has passed them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LeaderSlotCounts {
    pub epoch: Epoch,
    pub slots: usize,
    /// Slots the root has passed, including any too old for the slot history to answer.
    pub passed: usize,
    pub produced: usize,
    pub skipped: usize,
}

#[derive(Debug, Default)]
pub struct SkipRateWalk {
    /// The epoch and identity the walk is for; a swapped identity starts it again.
    pub epoch: Option<(Epoch, Pubkey)>,
    leader_slots: Vec<Slot>,
    pub next_index: usize,
    produced: usize,
    skipped: usize,
}

impl SkipRateWalk {
    pub fn new(epoch: Epoch, identity: Pubkey, leader_slots: Vec<Slot>) -> Self {
        Self {
            epoch: Some((epoch, identity)),
            leader_slots,
            ..Self::default()
        }
    }

    /// Whether a leader slot the root has passed is still to be counted.
    pub fn due(&self, root: Slot) -> bool {
        self.leader_slots
            .get(self.next_index)
            .is_some_and(|slot| *slot <= root)
    }

    /// Only slots the root has passed are settled. `check` answers from the root bank's slot
    /// history, which a restart does not lose; a slot too old for it is passed but not counted.
    pub fn advance(&mut self, root: Slot, check: impl Fn(Slot) -> Check) {
        while let Some(slot) = self.leader_slots.get(self.next_index).copied() {
            if slot > root {
                break;
            }
            match check(slot) {
                // The history has not reached the root yet; asked again next time.
                Check::Future => break,
                Check::TooOld => {}
                Check::Found => self.produced = self.produced.saturating_add(1),
                Check::NotFound => self.skipped = self.skipped.saturating_add(1),
            }
            self.next_index = self.next_index.saturating_add(1);
        }
    }

    pub fn counts(&self) -> Option<LeaderSlotCounts> {
        let (epoch, _) = self.epoch?;
        Some(LeaderSlotCounts {
            epoch,
            slots: self.leader_slots.len(),
            passed: self.next_index,
            produced: self.produced,
            skipped: self.skipped,
        })
    }

    pub fn rate(&self) -> Option<f64> {
        let elapsed = self.produced.saturating_add(self.skipped);
        (elapsed > 0).then(|| self.skipped as f64 / elapsed as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chain holding every slot from `oldest` to `newest` except those in `missing`.
    fn chain(oldest: Slot, newest: Slot, missing: &'static [Slot]) -> impl Fn(Slot) -> Check {
        move |slot| {
            if slot > newest {
                Check::Future
            } else if slot < oldest {
                Check::TooOld
            } else if missing.contains(&slot) {
                Check::NotFound
            } else {
                Check::Found
            }
        }
    }

    #[test]
    fn test_only_slots_the_root_has_passed_are_counted() {
        let mut walk = SkipRateWalk::new(3, Pubkey::new_unique(), vec![10, 11, 12, 13]);
        walk.advance(11, chain(0, 11, &[11]));
        assert_eq!(walk.rate(), Some(0.5));
        assert!(!walk.due(11));
        assert!(walk.due(12));
        walk.advance(13, chain(0, 13, &[11]));
        assert_eq!(walk.rate(), Some(0.25));
        assert_eq!(
            walk.counts(),
            Some(LeaderSlotCounts {
                epoch: 3,
                slots: 4,
                passed: 4,
                produced: 3,
                skipped: 1,
            })
        );
    }

    #[test]
    fn test_a_slot_older_than_the_history_is_passed_but_not_counted() {
        let mut walk = SkipRateWalk::new(3, Pubkey::new_unique(), vec![10, 11, 20, 21]);
        walk.advance(21, chain(20, 21, &[21]));
        assert_eq!(walk.next_index, 4);
        assert_eq!(walk.rate(), Some(0.5));
        let counts = walk.counts().unwrap();
        assert_eq!(
            (counts.passed, counts.produced, counts.skipped),
            (4, 1, 1),
            "passed, but neither produced nor skipped"
        );
    }

    #[test]
    fn test_a_history_behind_the_root_is_asked_again_later() {
        let mut walk = SkipRateWalk::new(3, Pubkey::new_unique(), vec![10, 11]);
        walk.advance(11, chain(0, 10, &[]));
        assert_eq!(walk.next_index, 1);
        walk.advance(11, chain(0, 11, &[11]));
        assert_eq!(walk.rate(), Some(0.5));
    }

    #[test]
    fn test_no_rate_before_a_leader_slot_has_passed() {
        let mut walk = SkipRateWalk::new(3, Pubkey::new_unique(), vec![10]);
        walk.advance(9, chain(0, 9, &[]));
        assert_eq!(walk.rate(), None);
        assert_eq!(walk.counts().unwrap().passed, 0);
        assert_eq!(SkipRateWalk::default().rate(), None);
        assert_eq!(SkipRateWalk::default().counts(), None);
    }
}
