//! The Cluster page's charts: each figure binned over the held part of the epoch, the epoch's
//! skips from the root bank's slot history, and the cluster's stake by client and by country.

use {
    super::{Encoded, EpochInfo, Replies, by_stake},
    crate::{
        geoip,
        history::{
            FAST_FINAL, HAS_BLOCK, HAS_CLOCK, HAS_FINAL_CERT, HAS_NOTARIZED, HAS_SHREDS,
            SlotHistory,
        },
        search::leader_index,
        validator_info::ValidatorInfoCache,
    },
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_slot_history::Check,
    std::{str::FromStr, sync::Arc},
};

pub const GATHER_EVERY_MILLIS: u64 = 15_000;

/// Named clients and countries shown before the rest are folded.
const CLIENTS_SHOWN: usize = 8;
const COUNTRIES_SHOWN: usize = 10;

pub(super) const BINS: usize = 120;
const HIST_BINS: usize = 30;
const RECENT_MILLIS: u64 = 600_000;

/// Consecutive blocks further apart than this, as after a restart, give no slot time.
const MAX_SLOT_GAP: u64 = 8;
const MAX_INTERVAL_MILLIS: u64 = 10_000;

/// A rate is taken over this many slots.
const RUN_SLOTS: u64 = 20;
const RUN_HELD: u64 = 18;

/// A short window gets fewer bins, each at least this many runs wide, so every bin holds a rate
/// and, at the median slot time, its bands.
const RUNS_PER_BIN: f64 = 3.0;

/// A from-zero axis ends at most this many times the window's 95th.
const OUTLIER_CAP: f64 = 2.0;

/// A bin with fewer readings than this draws its median but no bands.
const MIN_PER_BIN: usize = 3;
const MIN_READINGS: usize = 20;

/// A leader is ranked, and counts towards the spread, from this many settled slots.
const LEADER_SLOTS: u32 = 20;

/// Fewer leaders than these leave the per-leader spread out.
const MIN_LEADERS: usize = 10;
const SKIP_LEADERS: usize = 5;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClusterCharts {
    pub epoch: Epoch,
    /// Unix milliseconds the charts start, after any catching up, and end.
    pub from_millis: u64,
    pub to_millis: u64,
    pub cu: Option<Series>,
    pub tps: Option<Series>,
    pub succeeded: Option<Series>,
    pub failed: Option<Series>,
    pub slot_time: Option<Series>,
    pub notarized: Option<Series>,
    pub fast: Option<Shares>,
    /// Failed transactions' percent of all over the window.
    pub failed_share: Option<f64>,
    pub skips: Option<SkipChart>,
    pub clients: Vec<ClientShare>,
    pub countries: Option<StakeByCountry>,
}

/// One figure per bin, each `None` where the bin holds too few readings.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Series {
    pub p05: Vec<Option<f32>>,
    pub p25: Vec<Option<f32>>,
    pub p50: Vec<Option<f32>>,
    pub p75: Vec<Option<f32>>,
    pub p95: Vec<Option<f32>>,
    pub spread: Spread,
    /// The median over the last ten minutes.
    pub recent: Option<f64>,
    /// The axis, and every reading counted in equal steps up it.
    pub floor: f64,
    pub top: f64,
    pub hist: Vec<u32>,
}

/// A share of blocks per bin, in percent.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Shares {
    pub shares: Vec<Option<f32>>,
    pub count: u64,
    pub total: u64,
    pub share: f64,
    pub recent: Option<f64>,
    pub floor: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Spread {
    pub p05: f64,
    pub p50: f64,
    pub p95: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SkipChart {
    pub to_slot: Slot,
    /// Placed at the median slot time back from now, since the bank keeps no clock per slot.
    pub from_millis: u64,
    pub to_millis: u64,
    /// Percent of each bin's settled slots skipped.
    pub shares: Vec<Option<f32>>,
    pub skipped: u64,
    pub slots: u64,
    pub recent: Option<f64>,
    /// The previous epoch's percent, against which a young epoch is judged.
    pub previous: Option<f64>,
    /// Percent of each leader's settled slots skipped, across the leaders.
    pub per_leader: Option<Spread>,
    pub leaders: Vec<SkipLeader>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkipLeader {
    pub identity: String,
    pub name: Option<String>,
    pub country: Option<&'static str>,
    pub slots: u32,
    pub skipped: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClientShare {
    pub client: String,
    pub validators: usize,
    pub stake: u64,
    /// The client's two releases with the most stake.
    pub releases: Vec<String>,
    /// The folded tail and the clients gossip names by number only.
    pub other: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StakeByCountry {
    pub stake: u64,
    pub countries: usize,
    pub ours: Option<&'static str>,
    pub ours_continent: Option<&'static str>,
    /// The largest first, then the rest folded into one row with no code.
    pub rows: Vec<CountryShare>,
    pub continents: Vec<ContinentShare>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CountryShare {
    pub code: Option<&'static str>,
    pub stake: u64,
    pub validators: usize,
    /// On the folded row, how many countries it holds.
    pub others: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContinentShare {
    /// None for stake whose country is not known.
    pub code: Option<&'static str>,
    pub stake: u64,
}

/// A run of [`RUN_SLOTS`] slots being read; slots are paired with their first shred's time.
struct Run {
    first: (Slot, u64),
    last: (Slot, u64),
    held: u64,
    transactions: u64,
    failed: u64,
}

impl Run {
    /// The seconds [`RUN_SLOTS`] slots take at the pace from the run's first held slot to `end`.
    fn seconds(&self, end: (Slot, u64)) -> Option<f64> {
        let slots = end.0.checked_sub(self.first.0).filter(|slots| *slots > 0)?;
        let millis = end
            .1
            .checked_sub(self.first.1)
            .filter(|millis| *millis > 0)?;
        Some(millis as f64 / slots as f64 * RUN_SLOTS as f64 / 1_000.0)
    }
}

/// Readings taken under the history lock, each with the unix milliseconds it belongs to.
#[derive(Debug, Default)]
pub(super) struct Readings {
    /// The first held row's time at or after the start asked for.
    pub(super) first_at: Option<u64>,
    cu: Vec<(u64, f32)>,
    slot_millis: Vec<(u64, f32)>,
    /// Each run's first held time, the seconds its slots took, its transactions and failed
    /// transactions.
    runs: Vec<(u64, f64, u64, u64)>,
    notarized: Vec<(u64, f32)>,
    /// Each carried finalization certificate's time and whether it was the fast one.
    paths: Vec<(u64, bool)>,
}

/// Walks the held rows from `from_millis` on; the ring holds only the current epoch's tail.
pub(super) fn readings(
    history: &SlotHistory,
    epoch: &EpochInfo,
    newest: Slot,
    from_millis: u64,
) -> Readings {
    let mut readings = Readings::default();
    let last = newest.min(epoch.end_slot);
    let mut previous_full: Option<(Slot, u64)> = None;
    let mut run: Option<Run> = None;
    let mut run_index: Option<u64> = None;
    for slot in epoch.start_slot..=last {
        let Some(row) = history.get(slot) else {
            continue;
        };
        if row.flags & HAS_CLOCK == 0 || row.time_millis < from_millis {
            continue;
        }
        let at = row.time_millis;
        readings.first_at.get_or_insert(at);
        if row.flags & HAS_BLOCK != 0 && epoch.block_cost_limit > 0 {
            readings.cu.push((
                at,
                (f64::from(row.compute) * 100.0 / epoch.block_cost_limit as f64) as f32,
            ));
        }
        if row.flags & HAS_SHREDS != 0 {
            let full = at.saturating_add(u64::from(row.full_millis));
            if let Some((before, before_full)) = previous_full
                && slot.saturating_sub(before) <= MAX_SLOT_GAP
                && let Some(interval) = full.checked_sub(before_full)
                && interval > 0
                && interval < MAX_INTERVAL_MILLIS
            {
                readings.slot_millis.push((full, interval as f32));
            }
            previous_full = Some((slot, full));
        }
        if row.flags & HAS_NOTARIZED != 0 {
            readings
                .notarized
                .push((at, f32::from(row.notarized) / 100.0));
        }
        if row.flags & HAS_FINAL_CERT != 0 {
            readings.paths.push((at, row.flags & FAST_FINAL != 0));
        }
        let index = slot.checked_div(RUN_SLOTS);
        if index != run_index {
            // Timed to the next run's start, which takes in the handoff into this one; a run
            // before a gap is timed to its own last slot.
            let adjacent = run_index.and_then(|held| held.checked_add(1)) == index;
            if let Some(run) = run.take()
                && run.held >= RUN_HELD
                && let Some(seconds) = run.seconds(if adjacent { (slot, at) } else { run.last })
            {
                readings
                    .runs
                    .push((run.first.1, seconds, run.transactions, run.failed));
            }
            run_index = index;
        }
        let run = run.get_or_insert(Run {
            first: (slot, at),
            last: (slot, at),
            held: 0,
            transactions: 0,
            failed: 0,
        });
        run.last = (slot, at);
        run.held = run.held.saturating_add(1);
        if row.flags & HAS_BLOCK != 0 {
            run.transactions = run
                .transactions
                .saturating_add(u64::from(row.votes))
                .saturating_add(u64::from(row.non_votes));
            run.failed = run.failed.saturating_add(u64::from(row.failed));
        }
    }
    readings
}

/// The axis a series is drawn on: fixed where the figure has natural bounds, else rounded up from
/// the busiest bin, but no further than twice the window's 95th, so a few outlying bins do not
/// flatten the rest.
#[derive(Debug, Clone, Copy)]
enum Axis {
    Fixed(f64, f64),
    FromZero,
    /// Up to 100, from a step of ten under the lowest bin's p5 (its median where it has no bands).
    BelowLowest,
}

impl Readings {
    pub(super) fn median_slot_millis(&self) -> Option<f64> {
        let mut millis: Vec<f32> = self.slot_millis.iter().map(|(_, millis)| *millis).collect();
        millis.sort_unstable_by(f32::total_cmp);
        at_percent(&millis, 50).map(f64::from)
    }

    pub(super) fn charts(
        self,
        epoch: Epoch,
        from_millis: u64,
        to_millis: u64,
        slot_millis: Option<f64>,
    ) -> ClusterCharts {
        let bins = Bins::new(from_millis, to_millis, slot_millis);
        let rates = |pick: fn(&(u64, f64, u64, u64)) -> u64| -> Vec<(u64, f32)> {
            self.runs
                .iter()
                .map(|run| (run.0, (pick(run) as f64 / run.1) as f32))
                .collect()
        };
        let tps = rates(|run| run.2);
        let failed = rates(|run| run.3);
        let succeeded = rates(|run| run.2.saturating_sub(run.3));
        let (all, failed_all) = self.runs.iter().fold((0u64, 0u64), |(all, failed), run| {
            (all.saturating_add(run.2), failed.saturating_add(run.3))
        });
        ClusterCharts {
            epoch,
            from_millis,
            to_millis,
            cu: series(&bins, self.cu, Axis::Fixed(0.0, 100.0)),
            tps: series(&bins, tps, Axis::FromZero),
            succeeded: series(&bins, succeeded, Axis::FromZero),
            failed: series(&bins, failed, Axis::FromZero),
            slot_time: series(&bins, self.slot_millis, Axis::FromZero),
            notarized: series(&bins, self.notarized, Axis::BelowLowest),
            fast: shares(&bins, &self.paths),
            failed_share: (all > 0).then(|| failed_all as f64 * 100.0 / all as f64),
            skips: None,
            clients: Vec::new(),
            countries: None,
        }
    }
}

struct Bins {
    from: u64,
    span: u64,
    count: usize,
    recent_from: u64,
}

impl Bins {
    fn new(from: u64, to: u64, slot_millis: Option<f64>) -> Self {
        let span = to.saturating_sub(from).max(1);
        let count = slot_millis
            .filter(|millis| *millis > 0.0)
            .map_or(BINS, |millis| {
                let widest = span as f64 / (millis * RUN_SLOTS as f64 * RUNS_PER_BIN);
                (widest as usize).clamp(1, BINS)
            });
        Self {
            from,
            span,
            count,
            recent_from: to.saturating_sub(RECENT_MILLIS),
        }
    }

    fn of(&self, at: u64) -> usize {
        bin_of(at.saturating_sub(self.from), self.span, self.count)
    }
}

/// The bin of `count` across `span` that `offset` falls in; past the end is the last.
pub(crate) fn bin_of(offset: u64, span: u64, count: usize) -> usize {
    let bin = u128::from(offset)
        .saturating_mul(count as u128)
        .checked_div(u128::from(span))
        .unwrap_or(0);
    usize::try_from(bin)
        .unwrap_or(usize::MAX)
        .min(count.saturating_sub(1))
}

/// Hits against readings: per bin, over all, and over the recent part.
struct Binned {
    bins: Vec<(u64, u64)>,
    all: (u64, u64),
    recent: (u64, u64),
}

impl Binned {
    fn new(count: usize) -> Self {
        Self {
            bins: vec![(0, 0); count],
            all: (0, 0),
            recent: (0, 0),
        }
    }

    fn note(&mut self, bin: usize, recent: bool, hit: bool) {
        let recent = recent.then_some(&mut self.recent);
        for (hits, total) in [self.bins.get_mut(bin), Some(&mut self.all), recent]
            .into_iter()
            .flatten()
        {
            *total = total.saturating_add(1);
            *hits = hits.saturating_add(u64::from(hit));
        }
    }

    /// Each bin's percent, where it holds at least `min` readings.
    fn shares(&self, min: u64) -> Vec<Option<f32>> {
        self.bins
            .iter()
            .map(|(hits, total)| {
                percent(*hits, *total)
                    .filter(|_| *total >= min)
                    .map(|share| share as f32)
            })
            .collect()
    }
}

fn percent(part: u64, whole: u64) -> Option<f64> {
    (whole > 0).then(|| part as f64 * 100.0 / whole as f64)
}

fn series(bins: &Bins, readings: Vec<(u64, f32)>, axis: Axis) -> Option<Series> {
    if readings.len() < MIN_READINGS {
        return None;
    }
    let mut binned: Vec<Vec<f32>> = vec![Vec::new(); bins.count];
    let mut recent = Vec::new();
    let mut all = Vec::with_capacity(readings.len());
    for (at, value) in readings {
        if let Some(bin) = binned.get_mut(bins.of(at)) {
            bin.push(value);
        }
        if at >= bins.recent_from {
            recent.push(value);
        }
        all.push(value);
    }
    let mut columns: [Vec<Option<f32>>; 5] = Default::default();
    for bin in &mut binned {
        bin.sort_unstable_by(f32::total_cmp);
        let enough = bin.len() >= MIN_PER_BIN;
        for (column, percent) in columns.iter_mut().zip([5, 25, 50, 75, 95]) {
            let shown = enough || percent == 50;
            column.push(shown.then(|| at_percent(bin, percent)).flatten());
        }
    }
    all.sort_unstable_by(f32::total_cmp);
    recent.sort_unstable_by(f32::total_cmp);
    let spread = Spread {
        p05: f64::from(at_percent(&all, 5)?),
        p50: f64::from(at_percent(&all, 50)?),
        p95: f64::from(at_percent(&all, 95)?),
    };
    let [p05, p25, p50, p75, p95] = columns;
    let (floor, top) = match axis {
        Axis::Fixed(floor, top) => (floor, top),
        Axis::FromZero => {
            let busiest = p95
                .iter()
                .flatten()
                .copied()
                .fold(spread.p95 as f32, f32::max);
            let capped = f64::from(busiest).min(spread.p95 * OUTLIER_CAP);
            (0.0, nice_top(capped))
        }
        Axis::BelowLowest => {
            let lowest = p05
                .iter()
                .zip(&p50)
                .filter_map(|(low, median)| low.or(*median))
                .fold(100.0f32, f32::min);
            (floor_below(lowest), 100.0)
        }
    };
    Some(Series {
        p05,
        p25,
        p50,
        p75,
        p95,
        spread,
        recent: at_percent(&recent, 50).map(f64::from),
        floor,
        top,
        hist: histogram(&all, floor, top),
    })
}

fn shares(bins: &Bins, paths: &[(u64, bool)]) -> Option<Shares> {
    if paths.len() < MIN_READINGS {
        return None;
    }
    let mut binned = Binned::new(bins.count);
    for (at, fast) in paths {
        binned.note(bins.of(*at), *at >= bins.recent_from, *fast);
    }
    let shares = binned.shares(MIN_PER_BIN as u64);
    let lowest = shares.iter().flatten().copied().fold(100.0f32, f32::min);
    let (count, total) = binned.all;
    Some(Shares {
        shares,
        count,
        total,
        share: percent(count, total).unwrap_or(0.0),
        recent: percent(binned.recent.0, binned.recent.1),
        floor: floor_below(lowest),
    })
}

/// A step of ten under the lowest bin, and never above 90 so a calm hour still has room.
fn floor_below(lowest: f32) -> f64 {
    ((f64::from(lowest) / 10.0).floor() * 10.0).clamp(0.0, 90.0)
}

/// Nearest rank over sorted values.
pub(super) fn at_percent<T: Copy>(sorted: &[T], percent: usize) -> Option<T> {
    let index = sorted
        .len()
        .saturating_mul(percent)
        .checked_div(100)?
        .min(sorted.len().checked_sub(1)?);
    sorted.get(index).copied()
}

/// The first round step at or above `value`, so an axis ends a little over its busiest bin.
fn nice_top(value: f64) -> f64 {
    if !value.is_finite() || value <= 0.0 {
        return 1.0;
    }
    let power = 10f64.powf(value.log10().floor());
    [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
        .into_iter()
        .map(|step| step * power)
        .find(|top| *top >= value)
        .unwrap_or(value)
}

fn histogram(sorted: &[f32], floor: f64, top: f64) -> Vec<u32> {
    let mut hist = vec![0u32; HIST_BINS];
    let range = top - floor;
    if range <= 0.0 {
        return hist;
    }
    for value in sorted {
        let step = ((f64::from(*value) - floor) / range * HIST_BINS as f64).floor();
        let index = (step.max(0.0) as usize).min(HIST_BINS.saturating_sub(1));
        if let Some(count) = hist.get_mut(index) {
            *count = count.saturating_add(1);
        }
    }
    hist
}

/// Percent of the settled slots in `first..=last` skipped.
pub fn skip_share(first: Slot, last: Slot, check: impl Fn(Slot) -> Check) -> Option<f64> {
    let (mut held, mut missed) = (0u64, 0u64);
    for slot in first..=last {
        match check(slot) {
            Check::Found => held = held.saturating_add(1),
            Check::NotFound => {
                held = held.saturating_add(1);
                missed = missed.saturating_add(1);
            }
            Check::TooOld | Check::Future => {}
        }
    }
    (held > 0).then(|| missed as f64 * 100.0 / held as f64)
}

/// One leader's settled slots this epoch, by its index in the epoch's leaders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LeaderSkips {
    pub(super) at: usize,
    pub(super) slots: u32,
    pub(super) skipped: u32,
}

/// The epoch's settled slots in bins, the leaders who skipped most, and the previous epoch's
/// share.
pub(super) fn skip_chart(
    epoch: &EpochInfo,
    previous: Option<f64>,
    root: Slot,
    now_millis: u64,
    slot_millis: f64,
    check: impl Fn(Slot) -> Check,
) -> Option<(SkipChart, Vec<LeaderSkips>)> {
    let to_slot = root.min(epoch.end_slot);
    if to_slot <= epoch.start_slot {
        return None;
    }
    let span = to_slot.saturating_sub(epoch.start_slot).saturating_add(1);
    let recent_slots = (RECENT_MILLIS as f64 / slot_millis.max(1.0)) as u64;
    let recent_from = to_slot.saturating_sub(recent_slots);
    let mut binned = Binned::new(BINS);
    let mut tallies = vec![(0u32, 0u32); epoch.leaders.len()];
    for slot in epoch.start_slot..=to_slot {
        let made = match check(slot) {
            Check::Found => true,
            Check::NotFound => false,
            Check::TooOld | Check::Future => continue,
        };
        let bin = bin_of(slot.saturating_sub(epoch.start_slot), span, BINS);
        binned.note(bin, slot >= recent_from, !made);
        let leader = leader_index(epoch, slot).map(usize::from);
        if let Some((leader_slots, leader_skipped)) = leader.and_then(|at| tallies.get_mut(at)) {
            *leader_slots = leader_slots.saturating_add(1);
            if !made {
                *leader_skipped = leader_skipped.saturating_add(1);
            }
        }
    }
    let mut rates: Vec<u32> = tallies
        .iter()
        .filter(|(slots, _)| *slots >= LEADER_SLOTS)
        .filter_map(|(slots, skipped)| skipped.saturating_mul(10_000).checked_div(*slots))
        .collect();
    rates.sort_unstable();
    let rate = |percent| at_percent(&rates, percent).map(|rate| f64::from(rate) / 100.0);
    let per_leader = (rates.len() >= MIN_LEADERS)
        .then(|| {
            Some(Spread {
                p05: rate(5)?,
                p50: rate(50)?,
                p95: rate(95)?,
            })
        })
        .flatten();
    let mut ranked: Vec<LeaderSkips> = tallies
        .into_iter()
        .enumerate()
        .filter(|(_, (slots, skipped))| *slots >= LEADER_SLOTS && *skipped > 0)
        .map(|(at, (slots, skipped))| LeaderSkips { at, slots, skipped })
        .collect();
    ranked.sort_by(|a, b| {
        let rate = |leader: &LeaderSkips| f64::from(leader.skipped) / f64::from(leader.slots);
        rate(b)
            .total_cmp(&rate(a))
            .then_with(|| b.skipped.cmp(&a.skipped))
    });
    ranked.truncate(SKIP_LEADERS);
    let back = |slots: u64| (slots as f64 * slot_millis) as u64;
    let (skipped, slots) = binned.all;
    Some((
        SkipChart {
            to_slot,
            from_millis: now_millis.saturating_sub(back(span)),
            to_millis: now_millis,
            shares: binned.shares(1),
            skipped,
            slots,
            recent: percent(binned.recent.0, binned.recent.1),
            previous,
            per_leader,
            leaders: Vec::new(),
        },
        ranked,
    ))
}

impl Replies {
    /// Notes the ask and returns the held charts. After a lapse they are stale, so null is held
    /// until the slot feed's next tick gathers them afresh. `None` on a poisoned lock.
    pub fn ask_cluster_charts(&self, now_millis: u64) -> Option<Arc<str>> {
        let lapsed = self.cluster_charts.ask(now_millis);
        let mut held = self.cluster_charts.held.write().ok()?;
        if lapsed {
            *held = Encoded::default();
        }
        Some(held.0.clone())
    }
}

/// The charts, from any source of the epoch, the slot history and gossip: `rooted` checks a slot
/// against the SlotHistory sysvar, `described` is each staked identity's gossip by its stake, and
/// `country_of` a leader's country.
#[allow(clippy::too_many_arguments)]
pub fn cluster_charts(
    info: &EpochInfo,
    history: &SlotHistory,
    newest_arrival: Slot,
    asked_from: u64,
    root: Slot,
    now_millis: u64,
    previous_skips: Option<f64>,
    rooted: Option<&dyn Fn(Slot) -> Check>,
    names: &ValidatorInfoCache,
    country_of: impl Fn(&Pubkey) -> Option<&'static str>,
    described: &[(u64, Option<Described>)],
    ours_country: Option<&'static str>,
) -> ClusterCharts {
    let held = readings(history, info, newest_arrival, asked_from);
    let from_millis = held.first_at.unwrap_or(now_millis).max(asked_from);
    let slot_millis = held.median_slot_millis();
    let mut charts = held.charts(info.epoch, from_millis, now_millis, slot_millis);
    if let Some(rooted) = rooted
        && let Some((mut chart, ranked)) = skip_chart(
            info,
            previous_skips,
            root,
            now_millis,
            slot_millis.unwrap_or(400.0),
            rooted,
        )
    {
        chart.leaders = ranked
            .into_iter()
            .filter_map(|LeaderSkips { at, slots, skipped }| {
                let identity = info.leaders.get(at)?;
                let key = Pubkey::from_str(identity).ok();
                Some(SkipLeader {
                    identity: identity.clone(),
                    name: key.and_then(|key| names.get(&key).and_then(|info| info.name.clone())),
                    country: key.and_then(|key| country_of(&key)),
                    slots,
                    skipped,
                })
            })
            .collect();
        charts.skips = Some(chart);
    }
    charts.clients = client_shares(described);
    charts.countries = country_shares(described, ours_country);
    charts
}

/// What gossip says of one staked identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    pub client: String,
    pub release: String,
    pub country: Option<&'static str>,
}

/// Clients gossip names by number only, and identities gossip does not hold, are folded with the
/// tail.
fn client_shares(described: &[(u64, Option<Described>)]) -> Vec<ClientShare> {
    fn named(described: &Option<Described>) -> Option<&Described> {
        described
            .as_ref()
            .filter(|described| !described.client.starts_with("Unknown"))
    }
    let clients = described
        .iter()
        .map(|(stake, described)| (named(described).map(|named| named.client.as_str()), *stake));
    let (rows, (_, folded, folded_stake)) = by_stake(clients, CLIENTS_SHOWN, |a, b| a.0.cmp(b.0));
    let mut shares: Vec<ClientShare> = rows
        .into_iter()
        .map(|(client, validators, stake)| {
            let releases = described.iter().filter_map(|(stake, described)| {
                let described = named(described).filter(|described| described.client == client)?;
                Some((Some(described.release.as_str()), *stake))
            });
            let (releases, _) = by_stake(releases, 2, |a, b| a.0.cmp(b.0));
            ClientShare {
                client: client.to_string(),
                validators,
                stake,
                releases: releases
                    .into_iter()
                    .map(|(release, ..)| release.to_string())
                    .collect(),
                other: false,
            }
        })
        .collect();
    if folded > 0 {
        shares.push(ClientShare {
            client: "other".to_string(),
            validators: folded,
            stake: folded_stake,
            releases: Vec::new(),
            other: true,
        });
    }
    shares
}

fn country_shares(
    described: &[(u64, Option<Described>)],
    ours: Option<&'static str>,
) -> Option<StakeByCountry> {
    let country = |described: &Option<Described>| described.as_ref()?.country;
    let stake_total = described
        .iter()
        .fold(0u64, |sum, (stake, _)| sum.saturating_add(*stake));
    if stake_total == 0 {
        return None;
    }
    let countries = described
        .iter()
        .map(|(stake, described)| (country(described), *stake));
    let (rows, (tail_countries, folded, folded_stake)) =
        by_stake(countries, COUNTRIES_SHOWN, |a, b| a.0.cmp(b.0));
    let mut rows: Vec<CountryShare> = rows
        .into_iter()
        .map(|(code, validators, stake)| CountryShare {
            code: Some(code),
            stake,
            validators,
            others: None,
        })
        .collect();
    let named = rows.len().saturating_add(tail_countries);
    if folded > 0 {
        rows.push(CountryShare {
            code: None,
            stake: folded_stake,
            validators: folded,
            others: Some(tail_countries),
        });
    }
    let continents = described
        .iter()
        .map(|(stake, described)| (Some(country(described).and_then(geoip::continent)), *stake));
    let (continents, _) = by_stake(continents, usize::MAX, |a, b| a.0.cmp(&b.0));
    Some(StakeByCountry {
        stake: stake_total,
        countries: named,
        ours,
        ours_continent: ours.and_then(geoip::continent),
        rows,
        continents: continents
            .into_iter()
            .map(|(code, _, stake)| ContinentShare { code, stake })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use {super::*, crate::history::PackedSlot};

    fn epoch() -> EpochInfo {
        let slots = 4_000u64;
        EpochInfo {
            epoch: 7,
            start_slot: 1_000,
            end_slot: slots.saturating_add(999),
            slots_in_epoch: slots,
            my_leader_slots: Vec::new(),
            leaders: (0..10).map(|index| format!("leader{index}")).collect(),
            turns: (0..slots / 4)
                .map(|turn| u16::try_from(turn.checked_rem(10).unwrap()).unwrap())
                .collect(),
            block_cost_limit: 60_000_000,
            account_cost_limit: 12_000_000,
        }
    }

    /// A block every slot 400 ms apart, a fifth of its transactions failed, every fourth footer
    /// slow.
    fn filled(epoch: &EpochInfo) -> SlotHistory {
        let mut history = SlotHistory::new(8_192);
        for slot in epoch.start_slot..=epoch.end_slot {
            history.record_test_row(slot, |row: &mut PackedSlot| {
                row.flags = HAS_BLOCK | HAS_CLOCK | HAS_SHREDS | HAS_NOTARIZED | HAS_FINAL_CERT;
                if slot.checked_rem(4) != Some(0) {
                    row.flags |= FAST_FINAL;
                }
                row.non_votes = 100;
                row.failed = 20;
                row.compute = 30_000_000;
                row.notarized = 9_900;
                row.time_millis = slot.saturating_mul(400);
                row.full_millis = 100;
            });
        }
        history
    }

    fn whole_epoch(history: &SlotHistory, epoch: &EpochInfo) -> ClusterCharts {
        let from = epoch.start_slot.saturating_mul(400);
        let to = epoch.end_slot.saturating_mul(400);
        readings(history, epoch, epoch.end_slot, from).charts(7, from, to, Some(400.0))
    }

    #[test]
    fn test_charts_bin_every_reading() {
        let epoch = epoch();
        let charts = whole_epoch(&filled(&epoch), &epoch);
        let cu = charts.cu.unwrap();
        assert_eq!(cu.spread.p50, 50.0);
        assert_eq!((cu.floor, cu.top), (0.0, 100.0));
        assert!(cu.p50.iter().all(|bin| *bin == Some(50.0)));
        assert_eq!(cu.hist.iter().sum::<u32>(), 4_000);
        // A hundred transactions in each 400 ms slot.
        assert_eq!(charts.tps.unwrap().spread.p50, 250.0);
        assert_eq!(charts.failed.unwrap().spread.p50, 50.0);
        assert_eq!(charts.succeeded.unwrap().spread.p50, 200.0);
        assert_eq!(charts.failed_share, Some(20.0));
        assert_eq!(charts.slot_time.unwrap().spread.p50, 400.0);
        let notarized = charts.notarized.unwrap();
        assert_eq!(notarized.spread.p50, 99.0);
        assert!(notarized.p50.iter().all(|bin| *bin == Some(99.0)));
        assert_eq!((notarized.floor, notarized.top), (90.0, 100.0));
        assert_eq!(notarized.hist.iter().sum::<u32>(), 4_000);
        let fast = charts.fast.unwrap();
        assert_eq!((fast.count, fast.total), (3_000, 4_000));
        assert_eq!(fast.share, 75.0);
        assert_eq!(fast.floor, 70.0);
    }

    #[test]
    fn test_notarized_axis_drops_below_the_lowest_bin() {
        let epoch = epoch();
        let mut history = filled(&epoch);
        for slot in 2_000..2_040 {
            history.record_notarized(slot, 4_250);
        }
        let notarized = whole_epoch(&history, &epoch).notarized.unwrap();
        assert_eq!(
            notarized
                .p05
                .iter()
                .flatten()
                .copied()
                .fold(100.0, f32::min),
            42.5
        );
        assert_eq!((notarized.floor, notarized.top), (40.0, 100.0));
    }

    #[test]
    fn test_charts_start_after_catching_up() {
        let epoch = epoch();
        let history = filled(&epoch);
        let from = 3_000u64.saturating_mul(400);
        let to = epoch.end_slot.saturating_mul(400);
        let charts =
            readings(&history, &epoch, epoch.end_slot, from).charts(7, from, to, Some(400.0));
        assert_eq!(charts.cu.unwrap().hist.iter().sum::<u32>(), 2_000);
    }

    #[test]
    fn test_a_run_is_timed_on_its_own_slots_not_the_windows_median() {
        let epoch = epoch();
        let mut history = filled(&epoch);
        // Each turn's first slot takes 700 ms: a run of five turns lasts 9.5 s, the median 400 ms.
        for slot in epoch.start_slot..=epoch.end_slot {
            history.record_test_row(slot, |row: &mut PackedSlot| {
                let since = slot.saturating_sub(epoch.start_slot);
                row.time_millis = since
                    .saturating_mul(400)
                    .saturating_add(since.div_ceil(4).saturating_mul(300));
            });
        }
        let charts = readings(&history, &epoch, epoch.end_slot, 0).charts(7, 0, 2_000_000, None);
        // Two thousand transactions in 9.5 s.
        let tps = charts.tps.unwrap().spread.p50;
        assert!((tps - 2_000.0 / 9.5).abs() < 0.01, "{tps}");
    }

    #[test]
    fn test_a_thin_bin_keeps_its_median_without_bands() {
        let epoch = epoch();
        // The last of 66 bins holds two closed runs, the third still open.
        let tps = whole_epoch(&filled(&epoch), &epoch).tps.unwrap();
        assert_eq!(tps.p50.len(), 66);
        assert!(tps.p50.iter().all(Option::is_some));
        let (last, rest) = tps.p05.split_last().unwrap();
        assert!(rest.iter().all(Option::is_some));
        assert_eq!(*last, None);
    }

    #[test]
    fn test_a_short_window_gets_bins_three_runs_wide() {
        let epoch = epoch();
        let history = filled(&epoch);
        let from = epoch.start_slot.saturating_mul(400);
        // Fifty runs of 8 s over 400 s, so sixteen bins of 25 s.
        let to = from.saturating_add(400_000);
        let newest = epoch.start_slot.saturating_add(999);
        let charts = readings(&history, &epoch, newest, from).charts(7, from, to, Some(400.0));
        let tps = charts.tps.unwrap();
        assert_eq!(tps.p50.len(), 16);
        assert!(tps.p50.iter().all(Option::is_some));
        assert_eq!(charts.cu.unwrap().p50.len(), 16);
    }

    #[test]
    fn test_outlying_bins_do_not_set_the_axis() {
        // Slot times of 200 ms, with one minute in a hundred running at 900.
        let readings: Vec<(u64, f32)> = (0..12_000u64)
            .map(|at| (at.saturating_mul(100), if at < 120 { 900.0 } else { 200.0 }))
            .collect();
        let bins = Bins::new(0, 1_200_000, None);
        let series = series(&bins, readings, Axis::FromZero).unwrap();
        assert_eq!(series.spread.p95, 200.0);
        assert_eq!(series.p95.first().copied().flatten(), Some(900.0));
        assert_eq!(series.top, 400.0);
    }

    #[test]
    fn test_too_few_readings_leave_a_chart_out() {
        let epoch = epoch();
        let history = SlotHistory::new(8_192);
        let charts = readings(&history, &epoch, epoch.end_slot, 0).charts(7, 0, 1_000, Some(400.0));
        assert!(charts.cu.is_none() && charts.tps.is_none() && charts.fast.is_none());
        assert_eq!(charts.failed_share, None);
    }

    #[test]
    fn test_skip_chart_counts_bins_leaders_and_the_previous_epoch() {
        let epoch = epoch();
        // Leader 3 skips every slot of its turns; the previous epoch skipped one in ten.
        let check = |slot: Slot| {
            if slot < epoch.start_slot {
                if slot.checked_rem(10) == Some(0) {
                    Check::NotFound
                } else {
                    Check::Found
                }
            } else if slot > epoch.end_slot {
                Check::Future
            } else if slot
                .saturating_sub(epoch.start_slot)
                .checked_div(4)
                .unwrap()
                .checked_rem(10)
                == Some(3)
            {
                Check::NotFound
            } else {
                Check::Found
            }
        };
        assert_eq!(skip_share(0, 999, check), Some(10.0));
        let (chart, ranked) = skip_chart(
            &epoch,
            skip_share(0, 999, check),
            epoch.end_slot,
            10_000_000,
            400.0,
            check,
        )
        .unwrap();
        assert_eq!((chart.skipped, chart.slots), (400, 4_000));
        assert_eq!(chart.previous, Some(10.0));
        let spread = chart.per_leader.unwrap();
        assert_eq!((spread.p05, spread.p50, spread.p95), (0.0, 0.0, 100.0));
        let ranks: Vec<u32> = (1..=100).collect();
        assert_eq!(
            [5, 50, 95].map(|percent| at_percent(&ranks, percent)),
            [Some(6), Some(51), Some(96)]
        );
        assert!(chart.shares.iter().all(|share| share.is_some()));
        assert_eq!(
            ranked,
            vec![LeaderSkips {
                at: 3,
                slots: 400,
                skipped: 400
            }]
        );
        assert_eq!(chart.to_millis.saturating_sub(chart.from_millis), 1_600_000);
    }

    fn described(client: &str, release: &str, country: Option<&'static str>) -> Option<Described> {
        Some(Described {
            client: client.to_string(),
            release: release.to_string(),
            country,
        })
    }

    #[test]
    fn test_clients_fold_the_tail_numbered_and_unseen() {
        let mut staked: Vec<(u64, Option<Described>)> = (0..10u64)
            .map(|at| {
                (
                    100u64.saturating_sub(at),
                    described(&format!("client{at}"), "4.3.2", None),
                )
            })
            .collect();
        staked.push((50, described("client0", "4.4.0", None)));
        staked.push((7, described("Unknown(42)", "1.0.0", None)));
        staked.push((3, None));
        let shares = client_shares(&staked);
        assert_eq!(shares.len(), CLIENTS_SHOWN.saturating_add(1));
        let first = shares.first().unwrap();
        assert_eq!(
            (first.client.as_str(), first.validators, first.stake),
            ("client0", 2, 150)
        );
        assert_eq!(
            first.releases,
            vec!["4.3.2".to_string(), "4.4.0".to_string()]
        );
        let other = shares.last().unwrap();
        assert!(other.other);
        // client8 (92) and client9 (91), the numbered client and the one gossip does not hold.
        assert_eq!((other.validators, other.stake), (4, 193));
    }

    #[test]
    fn test_countries_rank_fold_and_place_continents() {
        let mut staked: Vec<(u64, Option<Described>)> = Vec::new();
        let codes = [
            "DE", "NL", "US", "GB", "JP", "LT", "SG", "IE", "SE", "FR", "BR", "HK",
        ];
        for (at, code) in codes.iter().enumerate() {
            let stake = 1_000u64.saturating_sub(u64::try_from(at).unwrap().saturating_mul(50));
            staked.push((stake, described("Agave", "4.4.0", Some(code))));
        }
        staked.push((5, None));
        let countries = country_shares(&staked, Some("SG")).unwrap();
        assert_eq!(countries.countries, 12);
        assert_eq!(countries.ours, Some("SG"));
        assert_eq!(countries.ours_continent, Some("AS"));
        assert_eq!(countries.rows.len(), COUNTRIES_SHOWN.saturating_add(1));
        assert_eq!(countries.rows.first().unwrap().code, Some("DE"));
        let folded = countries.rows.last().unwrap();
        // BR, HK and the identity with no country.
        assert_eq!(
            (folded.code, folded.others, folded.validators),
            (None, Some(2), 3)
        );
        assert_eq!(folded.stake, 500 + 450 + 5);
        let europe = countries.continents.first().unwrap();
        assert_eq!(europe.code, Some("EU"));
        assert!(
            countries
                .continents
                .iter()
                .any(|continent| continent.code.is_none())
        );
        let total: u64 = countries
            .continents
            .iter()
            .map(|continent| continent.stake)
            .sum();
        assert_eq!(total, countries.stake);
    }
    #[test]
    fn test_nice_tops() {
        assert_eq!(nice_top(2_991.0), 3_000.0);
        assert_eq!(nice_top(3_200.0), 4_000.0);
        assert_eq!(nice_top(280.0), 300.0);
        assert_eq!(nice_top(201.0), 250.0);
        assert_eq!(nice_top(75.0), 80.0);
        assert_eq!(nice_top(0.0), 1.0);
    }
}
