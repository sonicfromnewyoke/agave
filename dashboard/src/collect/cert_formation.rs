//! How the notarize and finalize certificates typically form, under alpenglow: the stake whose
//! average vote had arrived by each millisecond after a slot began here, over the last epoch votor
//! reported. Weighted by that epoch's rank map, the stake the certificates count against.

use {
    crate::metrics_tap::VoteArrival,
    agave_votor_messages::{
        certificate::CertificateType, consensus_message::Block, fraction::Fraction,
    },
    serde::Serialize,
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    std::{
        collections::{BTreeMap, HashMap},
        num::NonZeroU64,
    },
};

/// The curves run from the slot's start to this many milliseconds.
pub(crate) const CURVE_MILLIS: u64 = 480;

/// Shares are sent in hundredths of a percent.
const SHARE_SCALE: u128 = 10_000;

/// What a published formation was built from: the epoch, its slot time and its path counts.
pub type FormationKey = (Epoch, Option<u64>, Option<(u64, u64)>);

/// The epoch votor reports can trail the current one by two.
const PATH_EPOCHS_HELD: usize = 3;

/// Finalized slots per epoch, by the certificate votor tracked as finalizing them.
#[derive(Debug, Default)]
pub struct FinalizedPaths {
    /// Fast, then second round.
    by_epoch: BTreeMap<Epoch, (u64, u64)>,
}

impl FinalizedPaths {
    pub fn note(&mut self, epoch: Epoch, fast: bool) {
        let (fast_slots, second_slots) = self.by_epoch.entry(epoch).or_default();
        let count = if fast { fast_slots } else { second_slots };
        *count = count.saturating_add(1);
        crate::metrics_tap::keep_newest(&mut self.by_epoch, PATH_EPOCHS_HELD);
    }

    pub fn counts(&self, epoch: Epoch) -> Option<(u64, u64)> {
        self.by_epoch.get(&epoch).copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertFormation {
    pub epoch: Epoch,
    /// This node's mean slot time over the epoch, once caught up.
    pub slot_nanos: Option<u64>,
    /// Index `t` is the share of stake whose average notarize vote arrived before `t` ms.
    pub notarize: Vec<u16>,
    pub finalize: Vec<u16>,
    /// Every measured vote, later ones included.
    pub notarize_total: u16,
    pub finalize_total: u16,
    pub notarized_millis: Option<u64>,
    pub fast_final_millis: Option<u64>,
    pub second_round_millis: Option<u64>,
    /// The most notarize votes any ranked validator's average is over: about the slots this node
    /// timed that epoch, fewer where it started partway through.
    pub slots: Option<u64>,
    /// Slots the fast certificate finalized, and the second round, over the same epoch since this
    /// node started.
    pub fast_slots: Option<u64>,
    pub second_round_slots: Option<u64>,
}

/// Arrivals in microseconds against stake, earliest first. A zero mean is votor's floor, not a
/// measurement.
fn sorted_arrivals(votes: impl Iterator<Item = (u64, Option<u64>)>) -> Vec<(u64, u64)> {
    let mut arrivals: Vec<(u64, u64)> = votes
        .filter_map(|(stake, micros)| micros.filter(|micros| *micros > 0).map(|m| (m, stake)))
        .collect();
    arrivals.sort_unstable();
    arrivals
}

fn share(stake: u128, total: u128) -> u16 {
    stake
        .saturating_mul(SHARE_SCALE)
        .checked_div(total)
        .and_then(|share| u16::try_from(share).ok())
        .unwrap_or(0)
}

/// The share arrived before each millisecond from zero to `CURVE_MILLIS`.
fn curve(arrivals: &[(u64, u64)], total: u128) -> Vec<u16> {
    let mut points =
        Vec::with_capacity(usize::try_from(CURVE_MILLIS).unwrap_or(0).saturating_add(1));
    let mut held: u128 = 0;
    let mut next = arrivals.iter().peekable();
    for millis in 0..=CURVE_MILLIS {
        let before = millis.saturating_mul(1_000);
        while let Some((_, stake)) = next.next_if(|(micros, _)| *micros < before) {
            held = held.saturating_add(u128::from(*stake));
        }
        points.push(share(held, total));
    }
    points
}

/// When the arrived stake first reaches `threshold` of the total, in rounded milliseconds.
fn crossing(arrivals: &[(u64, u64)], total: u64, threshold: Fraction) -> Option<u64> {
    let total = NonZeroU64::new(total)?;
    let mut held = 0u64;
    arrivals.iter().find_map(|(micros, stake)| {
        held = held.saturating_add(*stake);
        (Fraction::new(held, total) >= threshold)
            .then(|| micros.saturating_add(500).checked_div(1_000))
            .flatten()
    })
}

/// `votes` holds each ranked validator's stake with its notarize and finalize means in
/// microseconds. `None` until some notarize vote was timed.
fn formation(
    epoch: Epoch,
    total_stake: u64,
    votes: &[(u64, Option<u64>, Option<u64>)],
    slot_nanos: Option<u64>,
    slots: Option<u64>,
    paths: Option<(u64, u64)>,
) -> Option<CertFormation> {
    let total = u128::from(total_stake);
    let notarize = sorted_arrivals(votes.iter().map(|(stake, n, _)| (*stake, *n)));
    let finalize = sorted_arrivals(votes.iter().map(|(stake, _, f)| (*stake, *f)));
    if notarize.is_empty() || total == 0 {
        return None;
    }
    let measured = |arrivals: &[(u64, u64)]| {
        share(
            arrivals.iter().map(|(_, stake)| u128::from(*stake)).sum(),
            total,
        )
    };
    let block = Block::default();
    let reaches = |arrivals: &[(u64, u64)], certificate: CertificateType| {
        crossing(arrivals, total_stake, certificate.threshold())
    };
    Some(CertFormation {
        epoch,
        slot_nanos,
        notarize: curve(&notarize, total),
        finalize: curve(&finalize, total),
        notarize_total: measured(&notarize),
        finalize_total: measured(&finalize),
        notarized_millis: reaches(&notarize, CertificateType::Notarize(block)),
        fast_final_millis: reaches(&notarize, CertificateType::FinalizeFast(block)),
        second_round_millis: reaches(&finalize, CertificateType::Finalize(block.slot)),
        slots,
        fast_slots: paths.map(|(fast, _)| fast),
        second_round_slots: paths.map(|(_, second)| second),
    })
}

/// From the epoch's rank map, votor's arrivals by vote account, this node's slot time and the
/// epoch's finalized paths, fast then second round.
pub fn cert_formation(
    rank_map: &BLSPubkeyToRankMap,
    epoch: Epoch,
    arrivals: &HashMap<Pubkey, VoteArrival>,
    slot_nanos: Option<u64>,
    paths: Option<(u64, u64)>,
) -> Option<CertFormation> {
    let ranked: Vec<(u64, Option<&VoteArrival>)> = crate::certs::rank_entries(rank_map)
        .map(|entry| (entry.stake.get(), arrivals.get(&entry.vote_account_pubkey)))
        .collect();
    let votes: Vec<(u64, Option<u64>, Option<u64>)> = ranked
        .iter()
        .map(|(stake, arrival)| {
            (
                *stake,
                arrival.map(|arrival| arrival.notarize_mean_micros),
                arrival.and_then(|arrival| arrival.finalize_mean_micros),
            )
        })
        .collect();
    let slots = ranked
        .iter()
        .filter_map(|(_, arrival)| arrival.and_then(|arrival| arrival.notarize_count))
        .max();
    formation(
        epoch,
        rank_map.total_stake().get(),
        &votes,
        slot_nanos,
        slots,
        paths,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_curve_counts_arrivals_before_each_millisecond() {
        let arrivals = sorted_arrivals(
            [
                (30, Some(250_000)),
                (20, Some(250_999)),
                (50, Some(251_000)),
            ]
            .into_iter(),
        );
        let points = curve(&arrivals, 100);
        assert_eq!(points.len(), 481);
        assert_eq!(points[250], 0);
        assert_eq!(points[251], 5_000);
        assert_eq!(points[252], 10_000);
        assert_eq!(points[480], 10_000);
    }

    #[test]
    fn test_crossing_is_the_arrival_that_reaches_the_share() {
        let arrivals = sorted_arrivals(
            [
                (50, Some(280_400)),
                (10, Some(290_600)),
                (40, Some(400_000)),
            ]
            .into_iter(),
        );
        let percent = Fraction::from_percentage;
        assert_eq!(crossing(&arrivals, 100, percent(60)), Some(291));
        assert_eq!(crossing(&arrivals, 100, percent(50)), Some(280));
        assert_eq!(crossing(&arrivals, 200, percent(60)), None);
    }

    #[test]
    fn test_zero_means_and_late_votes() {
        let votes = [
            (40, Some(270_000), Some(0)),
            (30, Some(300_000), Some(350_000)),
            (20, Some(600_000), Some(700_000)),
            (10, None, None),
        ];
        let built = formation(1_063, 100, &votes, Some(222_000_000), Some(12_700), None).unwrap();
        // Late votes count toward the totals but not the curves.
        assert_eq!(built.notarize_total, 9_000);
        assert_eq!(built.notarize[480], 7_000);
        // The finalize mean of zero is no measurement.
        assert_eq!(built.finalize_total, 5_000);
        assert_eq!(built.finalize[1], 0);
        assert_eq!(built.notarized_millis, Some(300));
        assert_eq!(built.fast_final_millis, Some(600));
        assert_eq!(built.second_round_millis, None);
        assert_eq!(built.slot_nanos, Some(222_000_000));
        assert_eq!(built.slots, Some(12_700));
    }

    #[test]
    fn test_paths_are_counted_per_epoch_and_the_oldest_dropped() {
        let mut paths = FinalizedPaths::default();
        for fast in [true, true, false] {
            paths.note(20, fast);
        }
        assert_eq!(paths.counts(20), Some((2, 1)));
        assert_eq!(paths.counts(21), None);
        for epoch in 21..24 {
            paths.note(epoch, false);
        }
        assert_eq!(paths.counts(20), None, "four epochs held is one too many");
        assert_eq!(paths.counts(23), Some((0, 1)));
    }
    #[test]
    fn test_nothing_without_a_notarize_vote() {
        assert_eq!(
            formation(1, 100, &[(100, None, Some(300_000))], None, None, None),
            None
        );
        assert_eq!(
            formation(1, 0, &[(0, Some(300_000), None)], None, None, None),
            None
        );
    }
}
