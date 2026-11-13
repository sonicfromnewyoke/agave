//! Each block's path on this node under alpenglow, from its leader closing it to its finalization
//! here. A block this node built never enters: replay does not run it, so it is never noted.

use {
    super::{HANDOFF_BEHIND_SLOTS, MINUTE_MILLIS, cluster_charts::at_percent},
    crate::slots::ShredArrival,
    serde::Serialize,
    solana_clock::Slot,
    std::collections::BTreeMap,
};

/// An hour of 200 ms slots.
const HELD_BLOCKS: usize = 18_000;

const WINDOW_MILLIS: u64 = 3_600_000;

const WINDOW_MINUTES: u64 = 60;

/// The slowest blocks are picked from these last ten minutes.
const RECENT_MILLIS: u64 = 600_000;

const SLOWEST: usize = 5;

/// A block is listed only where one stage ran this far over its median, past ordinary spread.
const SLOW_BY_MILLIS: u32 = 50;

/// Our first shred arriving this long after the leader's close means its clock is behind ours.
const CLOCK_SLACK_MILLIS: u64 = 100;

/// What is known of one block, filled in as each report arrives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Life {
    /// Unix milliseconds on this node's clock.
    first_shred: u64,
    full: u64,
    replayed: u64,
    /// Unix milliseconds by the leader's clock, from the block's footer.
    closed: Option<u64>,
    /// Microseconds after the block's last shred, on votor's clock.
    vote_after_full_us: Option<u64>,
    finalized_after_full_us: Option<u64>,
    shreds: u64,
    repaired: u64,
    recovered: u64,
}

/// Milliseconds spent in each stage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Stages {
    /// From the first shred to the leader closing the block.
    pub building: Option<u32>,
    /// From the leader closing the block to it being full here.
    pub reaching: Option<u32>,
    pub replay: Option<u32>,
    /// From replay finishing to our notarize vote.
    pub vote: Option<u32>,
    /// From our notarize vote, or from replay where this node did not vote, to the block's
    /// finalization.
    pub finalize: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Reaching,
    Replay,
    Vote,
    Finalize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlowBlock {
    pub slot: Slot,
    pub stages: Stages,
    /// From the leader closing the block to its finalization.
    pub total: Option<u32>,
    /// Milliseconds the leader's clock runs behind ours, ahead where negative, set only where its
    /// close cannot be right. The stages then start from our first shred.
    pub leader_clock_off: Option<i32>,
    /// The stage that ran furthest over its median.
    pub cause: Stage,
    pub shreds: u64,
    pub repaired: u64,
    pub recovered: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockLifeSummary {
    /// Minutes the medians cover, an hour at most.
    pub minutes: u32,
    /// Each stage's own median.
    pub median: Stages,
    /// The median from the leader closing a block to its finalization.
    pub total: Option<u32>,
    pub slowest: Vec<SlowBlock>,
}

struct Timed {
    slot: Slot,
    at: u64,
    stages: Stages,
    total: Option<u32>,
    /// From our first shred to the block being full, on our clock alone.
    receiving: u32,
    /// From the leader's close to the block being full, where that close cannot be right.
    skewed_reach: Option<i64>,
    life: Life,
}

impl Life {
    /// None until the footer is read. Where the leader's close lands well after our first shred, or
    /// after the fill, its clock is off: the stages then start from our first shred.
    fn timed(&self, slot: Slot) -> Option<Timed> {
        let closed = self.closed?;
        let receiving = self.full.saturating_sub(self.first_shred);
        let skewed = closed > self.full
            || self
                .first_shred
                .checked_sub(closed)
                .is_some_and(|after| after > CLOCK_SLACK_MILLIS);
        let reaching = if skewed {
            receiving
        } else {
            self.full.saturating_sub(closed)
        };
        let replay = self.replayed.saturating_sub(self.full);
        let vote_after = self
            .vote_after_full_us
            .and_then(|micros| micros.checked_div(1_000));
        let finalized_after = self
            .finalized_after_full_us
            .and_then(|micros| micros.checked_div(1_000));
        let finalize = finalized_after
            .map(|finalized| finalized.saturating_sub(vote_after.unwrap_or(replay).max(replay)));
        let stages = Stages {
            building: closed
                .checked_sub(self.first_shred)
                .filter(|_| !skewed)
                .map(millis),
            reaching: Some(millis(reaching)),
            replay: Some(millis(replay)),
            vote: vote_after.map(|after| millis(after.saturating_sub(replay))),
            finalize: finalize.map(millis),
        };
        let total = finalized_after
            .filter(|_| !skewed)
            .map(|after| millis(reaching.saturating_add(after)));
        let skewed_reach = skewed
            .then(|| {
                i64::try_from(self.full)
                    .ok()?
                    .checked_sub(i64::try_from(closed).ok()?)
            })
            .flatten();
        Some(Timed {
            slot,
            at: self.full,
            stages,
            total,
            receiving: millis(receiving),
            skewed_reach,
            life: *self,
        })
    }
}

#[derive(Debug, Default)]
pub struct BlockLives {
    blocks: BTreeMap<Slot, Life>,
}

impl BlockLives {
    /// Times on the blockstore's clock: the first shred in unix milliseconds, then milliseconds
    /// after it.
    pub fn note_arrival(
        &mut self,
        slot: Slot,
        first_shred_millis: u64,
        shreds: &ShredArrival,
        replayed_millis: u64,
        cluster_tip: Option<Slot>,
    ) {
        if cluster_tip.is_some_and(|tip| tip.saturating_sub(slot) > HANDOFF_BEHIND_SLOTS) {
            return;
        }
        let life = self.blocks.entry(slot).or_default();
        life.first_shred = first_shred_millis;
        life.full = first_shred_millis.saturating_add(shreds.full_millis);
        life.replayed = first_shred_millis.saturating_add(replayed_millis);
        life.shreds = shreds.count;
        life.repaired = shreds.repaired;
        life.recovered = shreds.recovered;
        crate::metrics_tap::keep_newest(&mut self.blocks, HELD_BLOCKS);
    }

    /// Only for a block already noted, so footers of this node's own blocks are passed by.
    pub fn note_closed(&mut self, slot: Slot, closed_at_nanos: u64) {
        if let Some(life) = self.blocks.get_mut(&slot) {
            life.closed = closed_at_nanos.checked_div(1_000_000);
        }
    }

    pub fn note_votes(
        &mut self,
        slot: Slot,
        vote_after_full_us: Option<u64>,
        finalized_after_full_us: Option<u64>,
    ) {
        if let Some(life) = self.blocks.get_mut(&slot) {
            life.vote_after_full_us = vote_after_full_us.or(life.vote_after_full_us);
            life.finalized_after_full_us = finalized_after_full_us.or(life.finalized_after_full_us);
        }
    }

    pub fn summary(&self, now_millis: u64) -> Option<BlockLifeSummary> {
        let timed: Vec<Timed> = self
            .blocks
            .iter()
            .filter(|(_, life)| now_millis.saturating_sub(life.full) <= WINDOW_MILLIS)
            .filter_map(|(slot, life)| life.timed(*slot))
            .collect();
        let oldest = timed.iter().map(|block| block.at).min()?;
        // The leader's close starts the first two stages, so a block whose close is off is left out
        // of them.
        let trusted = || timed.iter().filter(|block| block.skewed_reach.is_none());
        let median = Stages {
            building: upper_median(trusted().filter_map(|block| block.stages.building)),
            reaching: upper_median(trusted().filter_map(|block| block.stages.reaching)),
            replay: upper_median(timed.iter().filter_map(|block| block.stages.replay)),
            vote: upper_median(timed.iter().filter_map(|block| block.stages.vote)),
            finalize: upper_median(timed.iter().filter_map(|block| block.stages.finalize)),
        };
        let total = upper_median(timed.iter().filter_map(|block| block.total));
        let receiving = upper_median(timed.iter().map(|block| block.receiving));

        let mut slow: Vec<(u32, &Timed, Stage)> = timed
            .iter()
            .filter(|block| now_millis.saturating_sub(block.at) <= RECENT_MILLIS)
            .filter_map(|block| {
                let (over, cause) = furthest_over(block, &median, receiving)?;
                (over >= SLOW_BY_MILLIS).then_some((over, block, cause))
            })
            .collect();
        slow.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.slot.cmp(&a.1.slot)));
        let slowest = slow
            .into_iter()
            .take(SLOWEST)
            .map(|(_, block, cause)| SlowBlock {
                slot: block.slot,
                stages: block.stages,
                total: block.total,
                leader_clock_off: block.skewed_reach.map(|reach| {
                    let typical = median.reaching.map_or(0, i64::from);
                    i32::try_from(reach.saturating_sub(typical)).unwrap_or(i32::MAX)
                }),
                cause,
                shreds: block.life.shreds,
                repaired: block.life.repaired,
                recovered: block.life.recovered,
            })
            .collect();

        let span = now_millis.saturating_sub(oldest);
        let minutes = span.div_ceil(MINUTE_MILLIS).clamp(1, WINDOW_MINUTES);
        Some(BlockLifeSummary {
            minutes: u32::try_from(minutes).unwrap_or(u32::MAX),
            median,
            total,
            slowest,
        })
    }
}

/// The stage furthest over its median, and by how much, on this node's clock alone: reaching us is
/// judged from the first shred, so a leader's clock cannot make its blocks look late.
fn furthest_over(block: &Timed, median: &Stages, receiving: Option<u32>) -> Option<(u32, Stage)> {
    let stages = &block.stages;
    [
        (Some(block.receiving), receiving, Stage::Reaching),
        (stages.replay, median.replay, Stage::Replay),
        (stages.vote, median.vote, Stage::Vote),
        (stages.finalize, median.finalize, Stage::Finalize),
    ]
    .into_iter()
    .filter_map(|(value, median, stage)| Some((value?.checked_sub(median?)?, stage)))
    .max_by_key(|(over, _)| *over)
}

fn upper_median(values: impl Iterator<Item = u32>) -> Option<u32> {
    let mut values: Vec<u32> = values.collect();
    values.sort_unstable();
    at_percent(&values, 50)
}

fn millis(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_760_000_000_000;

    fn arrival(full_millis: u64) -> ShredArrival {
        ShredArrival {
            count: 512,
            repaired: 0,
            recovered: 0,
            full_millis,
        }
    }

    /// A block whose first shred came `ago` milliseconds before now, closed 150 ms after it.
    fn noted(lives: &mut BlockLives, slot: Slot, ago: u64, reaching: u64, replay: u64) {
        let first = NOW.saturating_sub(ago);
        let full_millis = 150_u64.saturating_add(reaching);
        lives.note_arrival(
            slot,
            first,
            &arrival(full_millis),
            full_millis.saturating_add(replay),
            Some(slot),
        );
        lives.note_closed(slot, first.saturating_add(150).saturating_mul(1_000_000));
    }

    #[test]
    fn test_a_block_splits_into_its_stages() {
        let mut lives = BlockLives::default();
        noted(&mut lives, 100, 5_000, 103, 9);
        lives.note_votes(100, Some(12_000), Some(70_000));

        let summary = lives.summary(NOW).expect("one block");
        assert_eq!(
            summary.median,
            Stages {
                building: Some(150),
                reaching: Some(103),
                replay: Some(9),
                vote: Some(3),
                finalize: Some(58),
            }
        );
        assert_eq!(summary.total, Some(173));
        assert_eq!(summary.minutes, 1);
        assert!(summary.slowest.is_empty(), "nothing is over its own median");
    }

    #[test]
    fn test_finality_counts_from_replay_where_we_did_not_vote() {
        let mut lives = BlockLives::default();
        noted(&mut lives, 100, 5_000, 103, 9);
        lives.note_votes(100, None, Some(70_000));

        let summary = lives.summary(NOW).expect("one block");
        assert_eq!(summary.median.vote, None);
        assert_eq!(summary.median.finalize, Some(61));
        assert_eq!(summary.total, Some(173));
    }

    #[test]
    fn test_blocks_caught_up_on_are_left_out() {
        let mut lives = BlockLives::default();
        lives.note_arrival(100, NOW, &arrival(250), 260, Some(133));
        lives.note_arrival(101, NOW, &arrival(250), 260, Some(133));
        assert_eq!(lives.blocks.keys().copied().collect::<Vec<_>>(), vec![101]);
    }

    #[test]
    fn test_nothing_shows_before_the_footer_is_read() {
        let mut lives = BlockLives::default();
        lives.note_arrival(100, NOW.saturating_sub(1_000), &arrival(250), 260, None);
        assert_eq!(lives.summary(NOW), None);
    }

    #[test]
    fn test_a_close_after_the_fill_is_kept_from_the_leader_stages() {
        // The leader's clock running ahead puts its close after the block was full here.
        let mut lives = BlockLives::default();
        lives.note_arrival(100, NOW.saturating_sub(1_000), &arrival(250), 260, None);
        lives.note_closed(100, NOW.saturating_mul(1_000_000));
        let summary = lives.summary(NOW).expect("kept");
        assert_eq!(summary.median.reaching, None);
        assert_eq!(summary.median.replay, Some(10));
    }

    /// A block whose leader stamped its close `behind` milliseconds early, which otherwise arrived
    /// like the rest.
    fn skewed(lives: &mut BlockLives, slot: Slot, ago: u64, behind: u64, replay: u64) {
        noted(lives, slot, ago, 100, replay);
        let first = NOW.saturating_sub(ago);
        lives.note_closed(
            slot,
            first
                .saturating_add(150)
                .saturating_sub(behind)
                .saturating_mul(1_000_000),
        );
    }

    #[test]
    fn test_a_leader_clock_behind_ours_does_not_make_its_block_slow() {
        let mut lives = BlockLives::default();
        for slot in 0..20 {
            noted(&mut lives, slot, 60_000, 100, 9);
        }
        skewed(&mut lives, 30, 30_000, 870, 9);

        let summary = lives.summary(NOW).expect("blocks");
        assert!(summary.slowest.is_empty(), "it arrived like the rest");
        assert_eq!(
            summary.median.reaching,
            Some(100),
            "left out of the leader's stages"
        );
    }

    #[test]
    fn test_a_slow_block_from_a_skewed_leader_says_so() {
        let mut lives = BlockLives::default();
        for slot in 0..20 {
            noted(&mut lives, slot, 60_000, 100, 9);
        }
        skewed(&mut lives, 30, 30_000, 870, 60);

        let summary = lives.summary(NOW).expect("blocks");
        let block = &summary.slowest[0];
        assert_eq!((block.slot, block.cause), (30, Stage::Replay));
        assert_eq!(block.leader_clock_off, Some(870));
        assert_eq!(block.stages.building, None);
        assert_eq!(block.stages.reaching, Some(250), "from our first shred");
        assert_eq!(block.total, None);
    }

    #[test]
    fn test_reports_for_blocks_never_noted_are_passed_by() {
        // Our own blocks: the footer walk and votor both see them, replay never does.
        let mut lives = BlockLives::default();
        lives.note_closed(100, NOW.saturating_mul(1_000_000));
        lives.note_votes(100, Some(1_000), Some(2_000));
        assert!(lives.blocks.is_empty());
    }

    #[test]
    fn test_the_slowest_are_ranked_by_their_worst_stage() {
        let mut lives = BlockLives::default();
        for slot in 0..20 {
            noted(&mut lives, slot, 60_000, 100, 9);
        }
        noted(&mut lives, 30, 30_000, 240, 9);
        noted(&mut lives, 31, 20_000, 100, 70);
        // Under the threshold, so not listed.
        noted(&mut lives, 32, 10_000, 140, 9);

        let summary = lives.summary(NOW).expect("blocks");
        let listed: Vec<(Slot, Stage)> = summary
            .slowest
            .iter()
            .map(|block| (block.slot, block.cause))
            .collect();
        assert_eq!(listed, vec![(30, Stage::Reaching), (31, Stage::Replay)]);
        assert_eq!(summary.slowest[0].stages.reaching, Some(240));
    }

    #[test]
    fn test_only_the_last_ten_minutes_are_listed_but_the_hour_is_counted() {
        let mut lives = BlockLives::default();
        noted(&mut lives, 1, 50_u64.saturating_mul(MINUTE_MILLIS), 300, 9);
        noted(&mut lives, 2, 1_000, 100, 9);
        noted(&mut lives, 3, 2_000, 100, 9);
        // Past the hour: neither counted nor listed.
        noted(&mut lives, 0, 61_u64.saturating_mul(MINUTE_MILLIS), 900, 9);

        let summary = lives.summary(NOW).expect("blocks");
        assert_eq!(summary.minutes, 50);
        assert_eq!(summary.median.reaching, Some(100));
        assert!(summary.slowest.is_empty());
    }
    #[test]
    fn test_the_store_holds_an_hour_of_blocks() {
        let mut lives = BlockLives::default();
        for slot in 0..(HELD_BLOCKS as u64).saturating_add(5) {
            lives.note_arrival(slot, NOW, &arrival(250), 260, None);
        }
        assert_eq!(lives.blocks.len(), HELD_BLOCKS);
        assert_eq!(
            lives.blocks.first_key_value().map(|(slot, _)| *slot),
            Some(5)
        );
    }
}
