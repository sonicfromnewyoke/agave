//! The gap before each leader's turn: from the previous slot's block filling here to the first
//! shred of the next leader's slot arriving here, kept for an epoch of slots.

use {
    super::{MINUTE_MILLIS, cluster_charts::at_percent},
    indexmap::IndexSet,
    serde::Serialize,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::collections::{BTreeMap, HashMap},
};

/// An epoch of slots, rolling, so the figures do not empty at each boundary.
const WINDOW_SLOTS: u64 = 432_000;

/// Percentiles over an epoch of gaps are recomputed no more often than this.
const STATS_EVERY_MILLIS: u64 = 10_000;

/// A leader's own band means something from this many handoffs.
const SETTLED: usize = 10;

/// The trend covers the last hour, a point each 15 seconds, each the minute up to it.
const HOUR_MILLIS: u64 = 3_600_000;
const TREND_STEP_MILLIS: u64 = 15_000;
const TREND_POINTS: u64 = 241;

/// The histogram's bins in milliseconds; the last also holds every longer gap.
const GAP_BIN_MILLIS: i32 = 10;
const GAP_BINS: usize = 60;

/// Bins for the settled leaders' medians; the last also holds every longer median.
const MEDIAN_BIN_MILLIS: i32 = 6;
const MEDIAN_BINS: usize = 100;

/// Enough rows for any real cluster; more would need a virtual list.
const LATE_LISTED: usize = 200;

/// A leader is judged against its own country only where this many settled leaders share it.
const COUNTRY_LEADERS: usize = 3;

/// How far a leader's p25 must sit over its country's p75 to count as late.
const LATE_MARGIN_MILLIS: i32 = 10;

#[derive(Debug, Clone, Copy)]
struct Handoff {
    leader: u16,
    /// Milliseconds; negative where the next leader's first shred arrived before the block filled.
    gap: i32,
    /// When the next leader's first shred arrived, unix milliseconds.
    at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Band {
    pub p25: i32,
    pub p50: i32,
    pub p75: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LateLeader {
    pub leader: String,
    /// From the leader's gossip address.
    pub country: Option<&'static str>,
    pub n: u32,
    pub p50: i32,
    /// Handoffs with a gap above the p75 of its country's leaders.
    pub over: u32,
}

/// One settled leader's figures, for the leaders in a bin of the median strip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaderFigures {
    pub leader: String,
    pub country: Option<&'static str>,
    pub n: u32,
    pub p25: i32,
    pub p50: i32,
    pub p75: i32,
    /// Late for its country; false where its country has too few settled leaders to judge.
    pub late: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OurHandoffs {
    pub country: Option<&'static str>,
    pub n: u32,
    /// Only once settled.
    pub p50: Option<i32>,
    pub over: u32,
    /// Whether our country has enough settled leaders to judge against.
    pub judged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandoffSummary {
    /// When the oldest held handoff arrived, unix milliseconds.
    pub since_millis: u64,
    /// Whether the window has rolled, so it covers a whole epoch of slots.
    pub full: bool,
    pub cluster: Band,
    /// Handoffs per 10 ms of gap from nought; negative gaps count in the first bin.
    pub gaps: Vec<u32>,
    /// Settled leaders that are not late, per 6 ms of their median gap.
    pub medians: Vec<u32>,
    /// Settled leaders that are late, in the same bins, all of them where the list is cut.
    pub late_medians: Vec<u32>,
    pub settled: u32,
    /// Settled leaders whose p25 is 10 ms over the p75 of their country's leaders, latest first.
    pub late: Vec<LateLeader>,
    pub late_count: u32,
    /// Settled leaders whose country is unknown or has too few settled leaders.
    pub unjudged: u32,
    pub ours: OurHandoffs,
    pub trend: Trend,
}

/// The mean gap over each minute of the last hour, a point each 15 seconds, oldest first, with that
/// minute's p25 and p75. A point with no handoffs in its minute is none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Trend {
    pub mean: Vec<Option<i32>>,
    pub p25: Vec<Option<i32>>,
    pub p75: Vec<Option<i32>>,
    /// The median gap over the whole hour.
    pub hour_p50: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandoffNow {
    pub leader: String,
    pub country: Option<&'static str>,
    /// None until the slot before has filled, or where that slot was ours or skipped.
    pub gap: Option<i32>,
    pub n: u32,
    /// Only once settled.
    pub band: Option<Band>,
    pub over: u32,
    pub late: bool,
    /// Whether its country has enough settled leaders to judge against.
    pub judged: bool,
    /// Each handoff, while too few for a band.
    pub gaps: Vec<i32>,
    /// The mean handoff gap over the last minute.
    pub minute_mean: Option<i32>,
}

#[derive(Debug, Default)]
pub struct Handoffs {
    keys: IndexSet<Pubkey>,
    held: BTreeMap<Slot, Handoff>,
    rolled: bool,
    stats: Option<HandoffSummary>,
    stats_at: Option<u64>,
    /// Each judged country's p75, from the last summary.
    country_p75: HashMap<&'static str, i32>,
    /// Every settled leader's figures, from the last summary, until taken.
    leaders: Option<Vec<LeaderFigures>>,
}

impl Handoffs {
    pub fn holds(&self, slot: Slot) -> bool {
        self.held.contains_key(&slot)
    }

    pub fn note(&mut self, slot: Slot, leader: Pubkey, gap: i64, at_millis: u64) {
        let index = self.keys.get_index_of(&leader).unwrap_or(self.keys.len());
        let Ok(index) = u16::try_from(index) else {
            return;
        };
        self.keys.insert(leader);
        let gap = i32::try_from(gap).unwrap_or(if gap < 0 { i32::MIN } else { i32::MAX });
        self.held.insert(
            slot,
            Handoff {
                leader: index,
                gap,
                at: at_millis,
            },
        );
        let floor = self
            .held
            .last_key_value()
            .map_or(0, |(newest, _)| newest.saturating_sub(WINDOW_SLOTS));
        while self
            .held
            .first_key_value()
            .is_some_and(|(oldest, _)| *oldest < floor)
        {
            self.held.pop_first();
            self.rolled = true;
        }
    }

    /// When the summary was last computed, so the leader now can be measured again against it.
    pub fn computed_at(&self) -> Option<u64> {
        self.stats_at
    }

    /// Recomputed only when the last is older than ten seconds.
    pub fn summary(
        &mut self,
        now_millis: u64,
        ours: &Pubkey,
        country: impl Fn(&Pubkey) -> Option<&'static str>,
    ) -> Option<HandoffSummary> {
        let fresh = self
            .stats_at
            .is_some_and(|at| now_millis.saturating_sub(at) < STATS_EVERY_MILLIS);
        if !fresh {
            self.stats = self.compute(now_millis, ours, country);
            self.stats_at = Some(now_millis);
        }
        self.stats.clone()
    }

    /// Every settled leader's figures, once after each summary.
    pub fn take_leaders(&mut self) -> Option<Vec<LeaderFigures>> {
        self.leaders.take()
    }

    fn compute(
        &mut self,
        now_millis: u64,
        ours: &Pubkey,
        country: impl Fn(&Pubkey) -> Option<&'static str>,
    ) -> Option<HandoffSummary> {
        let since_millis = self.held.values().map(|handoff| handoff.at).min()?;
        let mut all: Vec<i32> = self.held.values().map(|handoff| handoff.gap).collect();
        all.sort_unstable();
        let cluster = band(&all)?;

        let mut gaps = vec![0u32; GAP_BINS];
        for gap in &all {
            bump(&mut gaps, bin(*gap, GAP_BIN_MILLIS, GAP_BINS));
        }

        let mut by_leader: HashMap<usize, Vec<i32>> = HashMap::new();
        for handoff in self.held.values() {
            by_leader
                .entry(usize::from(handoff.leader))
                .or_default()
                .push(handoff.gap);
        }
        for own in by_leader.values_mut() {
            own.sort_unstable();
        }
        // Each settled leader with its country, then each country's handoffs pooled.
        let settled_leaders: Vec<(usize, &Vec<i32>, Band, Option<&'static str>)> = by_leader
            .iter()
            .filter(|(_, own)| own.len() >= SETTLED)
            .filter_map(|(leader, own)| {
                let own_band = band(own)?;
                let key = self.keys.get_index(*leader)?;
                Some((*leader, own, own_band, country(key)))
            })
            .collect();
        let mut pooled: HashMap<&'static str, (usize, Vec<i32>)> = HashMap::new();
        for (_, own, _, place) in &settled_leaders {
            if let Some(place) = place {
                let entry = pooled.entry(place).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1.extend(own.iter());
            }
        }
        let country_p75: HashMap<&'static str, i32> = pooled
            .into_iter()
            .filter(|(_, (leaders, _))| *leaders >= COUNTRY_LEADERS)
            .filter_map(|(place, (_, mut gaps))| {
                gaps.sort_unstable();
                Some((place, band(&gaps)?.p75))
            })
            .collect();

        let mut medians = vec![0u32; MEDIAN_BINS];
        let mut late_medians = vec![0u32; MEDIAN_BINS];
        let mut late = Vec::new();
        let mut leaders = Vec::new();
        let mut unjudged = 0u32;
        for (leader, own, own_band, place) in &settled_leaders {
            let local = place.and_then(|place| country_p75.get(place).copied());
            let is_late = late_for(Some(*own_band), local);
            if local.is_none() {
                unjudged = unjudged.saturating_add(1);
            }
            let key = self
                .keys
                .get_index(*leader)
                .map(Pubkey::to_string)
                .unwrap_or_default();
            let median_bin = bin(own_band.p50, MEDIAN_BIN_MILLIS, MEDIAN_BINS);
            if is_late {
                bump(&mut late_medians, median_bin);
                late.push(LateLeader {
                    leader: key.clone(),
                    country: *place,
                    n: count(own.len()),
                    p50: own_band.p50,
                    over: over(own, local),
                });
            } else {
                bump(&mut medians, median_bin);
            }
            leaders.push(LeaderFigures {
                leader: key,
                country: *place,
                n: count(own.len()),
                p25: own_band.p25,
                p50: own_band.p50,
                p75: own_band.p75,
                late: is_late,
            });
        }
        late.sort_by(|a, b| b.p50.cmp(&a.p50).then_with(|| a.leader.cmp(&b.leader)));
        let late_count = count(late.len());
        late.truncate(LATE_LISTED);
        leaders.sort_by(|a, b| a.p50.cmp(&b.p50).then_with(|| b.n.cmp(&a.n)));
        let settled = count(settled_leaders.len());

        let our_country = country(ours);
        let our_local = our_country.and_then(|place| country_p75.get(place).copied());
        let own = self
            .keys
            .get_index_of(ours)
            .and_then(|index| by_leader.get(&index))
            .map_or(&[][..], Vec::as_slice);
        let ours = OurHandoffs {
            country: our_country,
            n: count(own.len()),
            p50: (own.len() >= SETTLED)
                .then(|| band(own).map(|own| own.p50))
                .flatten(),
            over: over(own, our_local),
            judged: our_local.is_some(),
        };

        let summary = HandoffSummary {
            since_millis,
            full: self.rolled,
            cluster,
            gaps,
            medians,
            late_medians,
            settled,
            late,
            late_count,
            unjudged,
            ours,
            trend: self.trend(now_millis),
        };
        self.country_p75 = country_p75;
        self.leaders = Some(leaders);
        Some(summary)
    }

    fn trend(&self, now_millis: u64) -> Trend {
        let first = now_millis.saturating_sub(HOUR_MILLIS);
        let mut recent: Vec<(u64, i32)> = self
            .held
            .values()
            .filter(|handoff| {
                handoff.at > first.saturating_sub(MINUTE_MILLIS) && handoff.at <= now_millis
            })
            .map(|handoff| (handoff.at, handoff.gap))
            .collect();
        recent.sort_unstable_by_key(|(at, _)| *at);

        let mut trend = Trend::default();
        let mut minute = Vec::new();
        for point in 0..TREND_POINTS {
            let end = first.saturating_add(point.saturating_mul(TREND_STEP_MILLIS));
            let from = recent.partition_point(|(at, _)| *at <= end.saturating_sub(MINUTE_MILLIS));
            let to = recent.partition_point(|(at, _)| *at <= end);
            minute.clear();
            minute.extend(
                recent
                    .get(from..to)
                    .unwrap_or_default()
                    .iter()
                    .map(|(_, gap)| *gap),
            );
            minute.sort_unstable();
            let quartiles = band(&minute);
            trend.mean.push(mean(&minute));
            trend.p25.push(quartiles.map(|band| band.p25));
            trend.p75.push(quartiles.map(|band| band.p75));
        }
        let mut hour: Vec<i32> = recent
            .iter()
            .filter(|(at, _)| *at > first)
            .map(|(_, gap)| *gap)
            .collect();
        hour.sort_unstable();
        trend.hour_p50 = band(&hour).map(|band| band.p50);
        trend
    }

    /// The leader of the turn arriving now, judged against its country's p75 from the last summary.
    pub fn now(
        &self,
        leader: &Pubkey,
        country: Option<&'static str>,
        slot: Slot,
        now_millis: u64,
    ) -> HandoffNow {
        let local = country.and_then(|place| self.country_p75.get(place).copied());
        let mut own: Vec<i32> = self
            .keys
            .get_index_of(leader)
            .map(|index| {
                self.held
                    .values()
                    .filter(|handoff| usize::from(handoff.leader) == index)
                    .map(|handoff| handoff.gap)
                    .collect()
            })
            .unwrap_or_default();
        own.sort_unstable();
        let settled = own.len() >= SETTLED;
        let own_band = settled.then(|| band(&own)).flatten();
        let late = late_for(own_band, local);
        let recent: Vec<i32> = self
            .held
            .values()
            .rev()
            .take_while(|handoff| now_millis.saturating_sub(handoff.at) <= MINUTE_MILLIS)
            .map(|handoff| handoff.gap)
            .collect();
        HandoffNow {
            leader: leader.to_string(),
            country,
            gap: self.held.get(&slot).map(|handoff| handoff.gap),
            n: count(own.len()),
            band: own_band,
            over: over(&own, local),
            late,
            judged: local.is_some(),
            gaps: if settled { Vec::new() } else { own },
            minute_mean: mean(&recent),
        }
    }
}

fn mean(gaps: &[i32]) -> Option<i32> {
    let sum: i64 = gaps.iter().map(|gap| i64::from(*gap)).sum();
    i64::try_from(gaps.len())
        .ok()
        .and_then(|len| sum.checked_div(len))
        .and_then(|mean| i32::try_from(mean).ok())
}

/// Quartiles of sorted values, each the value at that share of the count.
fn band(sorted: &[i32]) -> Option<Band> {
    Some(Band {
        p25: at_percent(sorted, 25)?,
        p50: at_percent(sorted, 50)?,
        p75: at_percent(sorted, 75)?,
    })
}

fn bin(millis: i32, width: i32, bins: usize) -> usize {
    let index = usize::try_from(millis.max(0).checked_div(width).unwrap_or(0)).unwrap_or(0);
    index.min(bins.saturating_sub(1))
}

fn bump(bins: &mut [u32], index: usize) {
    if let Some(bin) = bins.get_mut(index) {
        *bin = bin.saturating_add(1);
    }
}

/// Late where its p25 sits over its country's p75 by more than the margin.
fn late_for(own: Option<Band>, local: Option<i32>) -> bool {
    matches!((own, local), (Some(own), Some(local))
        if own.p25 > local.saturating_add(LATE_MARGIN_MILLIS))
}

/// Gaps over its country's p75, none where the country is not judged.
fn over(gaps: &[i32], local: Option<i32>) -> u32 {
    local.map_or(0, |local| {
        count(gaps.iter().filter(|gap| **gap > local).count())
    })
}

fn count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn no_country(_: &Pubkey) -> Option<&'static str> {
        None
    }

    fn germany(_: &Pubkey) -> Option<&'static str> {
        Some("DE")
    }

    /// Ten leaders with ten handoffs each at 60 ms, and one at `slow` ms.
    fn cluster_with(slow: i64) -> Handoffs {
        let mut handoffs = Handoffs::default();
        let mut slot: Slot = 1_000;
        for round in 0..10u64 {
            for leader in (1..=10u8).chain([99]) {
                slot = slot.saturating_add(4);
                let gap = if leader == 99 { slow } else { 60 };
                handoffs.note(slot, key(leader), gap, round.saturating_add(1_000));
            }
        }
        handoffs
    }

    fn cluster() -> Handoffs {
        cluster_with(220)
    }

    #[test]
    fn test_a_leader_late_on_most_turns_is_listed_and_kept_out_of_the_medians() {
        let mut handoffs = cluster();
        let summary = handoffs.summary(0, &key(1), germany).unwrap();
        assert_eq!(summary.settled, 11);
        assert_eq!(summary.late_count, 1);
        assert_eq!(summary.late[0].leader, key(99).to_string());
        assert_eq!(summary.late[0].country, Some("DE"));
        assert_eq!(summary.late[0].p50, 220);
        assert_eq!(summary.late[0].over, 10);
        assert_eq!(summary.medians.iter().sum::<u32>(), 10);
        assert_eq!(summary.late_medians.iter().sum::<u32>(), 1);
        assert_eq!(summary.late_medians[36], 1);
        assert_eq!(summary.gaps.iter().sum::<u32>(), 110);
        assert_eq!(summary.ours.n, 10);
        assert_eq!(summary.ours.p50, Some(60));
        assert_eq!(summary.ours.country, Some("DE"));
        assert!(summary.ours.judged);
        assert_eq!(summary.unjudged, 0);
        assert!(!summary.full);
    }

    #[test]
    fn test_a_leader_alone_in_its_country_is_not_judged() {
        let mut handoffs = cluster();
        let country = |leader: &Pubkey| (leader == &key(99)).then_some("JP").or(Some("DE"));
        let summary = handoffs.summary(0, &key(1), country).unwrap();
        assert_eq!(summary.late_count, 0);
        assert_eq!(summary.unjudged, 1);
        assert_eq!(summary.late_medians.iter().sum::<u32>(), 0);
        let now = handoffs.now(&key(99), Some("JP"), 1_000, 1_009);
        assert!(!now.judged);
        assert!(!now.late);
        assert_eq!(now.over, 0);
    }

    #[test]
    fn test_late_needs_the_p25_ten_ms_over_its_country() {
        // The country's p75 is 60 ms, so 70 is the line.
        let mut near = cluster_with(70);
        assert_eq!(near.summary(0, &key(1), germany).unwrap().late_count, 0);
        let mut over = cluster_with(71);
        assert_eq!(over.summary(0, &key(1), germany).unwrap().late_count, 1);
    }

    #[test]
    fn test_each_settled_leaders_figures_are_taken_once_per_summary() {
        let mut handoffs = cluster();
        handoffs.summary(0, &key(1), germany);
        let leaders = handoffs.take_leaders().unwrap();
        assert_eq!(leaders.len(), 11);
        assert_eq!(
            leaders.last().map(|leader| (leader.p50, leader.late)),
            Some((220, true))
        );
        assert!(
            leaders
                .iter()
                .take(10)
                .all(|leader| leader.p50 == 60 && !leader.late)
        );
        assert_eq!(handoffs.take_leaders(), None);
    }

    #[test]
    fn test_the_trend_means_each_minute_and_leaves_empty_minutes_out() {
        let mut handoffs = Handoffs::default();
        // Two handoffs ten minutes before the end, one a minute before it.
        handoffs.note(4, key(1), 40, 3_000_000);
        handoffs.note(8, key(2), 80, 3_010_000);
        handoffs.note(12, key(3), 200, 3_560_000);
        let trend = handoffs
            .summary(3_600_000, &key(9), no_country)
            .unwrap()
            .trend;
        assert_eq!(trend.mean.len(), 241);
        // The point at 3,015,000 holds the first two; the last holds only the third.
        let at = |millis: u64| usize::try_from(millis.checked_div(15_000).unwrap()).unwrap();
        assert_eq!(trend.mean[at(3_015_000)], Some(60));
        assert_eq!(trend.p25[at(3_015_000)], Some(40));
        assert_eq!(trend.p75[at(3_015_000)], Some(80));
        assert_eq!(trend.mean.last(), Some(&Some(200)));
        assert_eq!(trend.mean[at(3_300_000)], None);
        assert_eq!(trend.hour_p50, Some(80));
    }

    #[test]
    fn test_the_window_rolls_after_an_epoch_of_slots() {
        let mut handoffs = Handoffs::default();
        handoffs.note(10, key(1), 50, 1);
        handoffs.note(WINDOW_SLOTS.saturating_add(10), key(2), 50, 2);
        assert!(handoffs.holds(10));
        handoffs.note(WINDOW_SLOTS.saturating_add(11), key(2), 50, 3);
        assert!(!handoffs.holds(10));
        let summary = handoffs.summary(0, &key(1), no_country).unwrap();
        assert!(summary.full);
        assert_eq!(summary.since_millis, 2);
    }

    #[test]
    fn test_the_summary_is_kept_for_ten_seconds() {
        let mut handoffs = cluster();
        let first = handoffs.summary(0, &key(1), no_country).unwrap();
        handoffs.note(50_000, key(3), 500, 2_000);
        assert_eq!(handoffs.summary(9_999, &key(1), no_country).unwrap(), first);
        assert_ne!(
            handoffs.summary(10_000, &key(1), no_country).unwrap(),
            first
        );
    }

    #[test]
    fn test_a_leader_with_few_handoffs_sends_each_gap() {
        let mut handoffs = cluster();
        handoffs.summary(0, &key(1), no_country);
        handoffs.note(50_000, key(42), 80, 1_010);
        handoffs.note(50_004, key(42), 90, 1_011);
        let now = handoffs.now(&key(42), None, 50_004, 1_011);
        assert_eq!(now.gap, Some(90));
        assert_eq!(now.n, 2);
        assert_eq!(now.band, None);
        assert_eq!(now.gaps, vec![80, 90]);
        assert!(!now.late);
    }

    #[test]
    fn test_a_late_leader_reads_late_now() {
        let mut handoffs = cluster();
        handoffs.summary(0, &key(1), germany);
        let now = handoffs.now(&key(99), Some("DE"), 1_000, 1_009);
        assert!(now.late);
        assert!(now.judged);
        assert_eq!(now.country, Some("DE"));
        assert_eq!(now.over, 10);
        assert!(now.gaps.is_empty());
        assert_eq!(now.band.map(|band| band.p50), Some(220));
    }

    #[test]
    fn test_the_minute_mean_counts_only_the_last_minute() {
        let mut handoffs = Handoffs::default();
        handoffs.note(4, key(1), 1_000, 0);
        handoffs.note(8, key(2), 40, 100_000);
        handoffs.note(12, key(3), 60, 120_000);
        let now = handoffs.now(&key(3), None, 12, 120_000);
        assert_eq!(now.minute_mean, Some(50));
    }

    #[test]
    fn test_negative_and_long_gaps_land_in_the_end_bins() {
        let mut handoffs = Handoffs::default();
        handoffs.note(4, key(1), -40, 1);
        handoffs.note(8, key(1), 5_000, 2);
        let summary = handoffs.summary(0, &key(9), no_country).unwrap();
        assert_eq!(summary.gaps[0], 1);
        assert_eq!(summary.gaps.last(), Some(&1));
        assert_eq!(summary.ours.n, 0);
    }
    #[test]
    fn test_quartiles_take_the_value_at_each_share() {
        assert_eq!(
            band(&[1, 2, 3, 4, 5, 6, 7, 8]),
            Some(Band {
                p25: 3,
                p50: 5,
                p75: 7
            })
        );
        assert_eq!(band(&[]), None);
    }
}
