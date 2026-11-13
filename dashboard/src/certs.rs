//! Whether this node's vote was paid for each slot, and how many slots each
//! validator's was, read from the reward certificates alpenglow leaders write
//! into their block footers.

use {
    crate::collect::cluster_charts::bin_of,
    agave_votor_messages::reward_certificate::NUM_SLOTS_FOR_REWARD,
    bitvec::{order::Lsb0, vec::BitVec},
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_entry::block_component::BlockFooterV1,
    solana_pubkey::Pubkey,
    solana_runtime::epoch_stakes::{BLSPubkeyStakeEntry, BLSPubkeyToRankMap},
    solana_signer_store::{Decoded, decode},
    std::{
        cmp::Reverse,
        collections::{BTreeMap, HashMap},
        sync::Arc,
    },
};

/// Slots from the epoch's first over which the cluster pays out stake
/// rewards, when every validator misses votes.
pub const BOUNDARY_SLOTS: Slot = 1_000;

pub const MISS_BINS: usize = 400;

/// A certificate paying at least this share fewer ranks than the epoch's
/// median certificate is thin, once `THIN_MIN_CERTIFICATES` have been seen.
pub const THIN_SHORTFALL_PERCENT: u64 = 10;
pub const THIN_MIN_CERTIFICATES: u64 = 100;

/// A rank paid in at least this share of the epoch's certificates is a
/// regular, whose absence from one is the writer's doing.
pub const REGULAR_PERCENT: u64 = 90;

const PENDING_VOTES: usize = 4096;

const LOST_LEADERS: usize = 3;

/// The ranks a reward certificate sets, as `solana_signer_store` decodes them.
pub type Ranks = BitVec<u8, Lsb0>;

/// The certificate for slot N can only be written by the leader of slot N+8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reward {
    Paid,
    Unpaid,
    /// No rooted block in slot N+8 carried a certificate, so nobody was paid for N.
    NoCertificate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mark {
    pub slot: Slot,
    pub reward: Reward,
    pub rank: usize,
    /// Empty where there was no certificate.
    pub paid: Ranks,
    pub notar: u32,
    pub skip: u32,
}

impl Mark {
    fn no_certificate(slot: Slot, rank: usize) -> Self {
        Self {
            slot,
            reward: Reward::NoCertificate,
            rank,
            paid: Ranks::new(),
            notar: 0,
            skip: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Participation {
    pub epoch: Epoch,
    pub since_slot: Slot,
    pub paid: u64,
    pub rewarded: u64,
    pub cluster_max: u64,
    /// Over the ranks paid at least once; `None` before any is.
    pub cluster_median: Option<u64>,
    pub misses: Misses,
    pub miss_bins: Vec<u32>,
    pub lost_leaders: Vec<LostLeader>,
    pub ranks: u32,
    /// Absent until `THIN_MIN_CERTIFICATES` are in.
    pub thin_below: Option<u32>,
}

/// A slot in more than one place counts in the first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Misses {
    pub boundary: u64,
    pub leader: u64,
    pub snapshot: u64,
    pub thin: u64,
    /// This node finished replaying the slot after the certificate's writer
    /// had begun its own.
    pub late: u64,
    pub lost: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LostLeader {
    pub identity: String,
    pub name: Option<String>,
    pub count: u64,
}

/// Votor's timeline for a slot, in microseconds from when it began tracking it. The first shred is
/// reported only for a leader window's first slot; the rest anchor on the parent becoming ready.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct VoteSent {
    pub first_shred_us: Option<u64>,
    pub parent_ready_us: Option<u64>,
    pub notarize_us: Option<u64>,
    pub skip_us: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissRecord {
    pub slot: Slot,
    pub place: Place,
    pub paid_ranks: u32,
    pub others: Vec<u32>,
    pub writer: Option<Pubkey>,
    pub vote: Option<VoteSent>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MissDetail {
    pub writer: Option<Pubkey>,
    pub late: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub from: Slot,
    pub to: Option<Slot>,
}

impl Span {
    fn covers(&self, slot: Slot) -> bool {
        slot >= self.from && self.to.is_none_or(|to| slot <= to)
    }
}

pub fn writer_slot(slot: Slot) -> Slot {
    slot.saturating_add(NUM_SLOTS_FOR_REWARD)
}

pub fn rewarded_slot(writer_slot: Slot) -> Option<Slot> {
    writer_slot.checked_sub(NUM_SLOTS_FOR_REWARD)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Place {
    Boundary,
    Leader,
    Snapshot,
    Thin,
    Late,
    Lost,
}

/// A place settled when the slot was seen, else decided against the epoch's certificates when read.
#[derive(Debug, Clone)]
struct Miss {
    slot: Slot,
    fixed: Option<Place>,
    paid_ranks: u32,
    unpaid: Box<[u16]>,
    writer: Option<Pubkey>,
    late: bool,
    vote: Option<VoteSent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub paid: u32,
    pub notar: u32,
    pub skip: u32,
    pub ours_in: bool,
    pub unpaid: Box<[u16]>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WrittenSummary {
    pub certificates: u64,
    pub carried_all: u64,
    pub unpaid_by_rank: Vec<u64>,
    pub unpaid_everywhere: Vec<u64>,
}

#[derive(Debug, Default)]
pub struct Tally {
    pub epoch: Epoch,
    since_slot: Slot,
    epoch_start: Slot,
    slots_in_epoch: u64,
    /// Ascending. Empty where the schedule was not known when the tally began,
    /// when those misses fall under the later places.
    pub leader_slots: Vec<Slot>,
    paid: u64,
    pub rewarded: u64,
    per_rank: Vec<u64>,
    misses: Vec<Miss>,
    by_slot: HashMap<Slot, usize>,
    paid_ranks: Vec<u32>,
    writer_certs: HashMap<Pubkey, u64>,
    pending_votes: BTreeMap<Slot, VoteSent>,
    ours: BTreeMap<Slot, Written>,
}

impl Tally {
    pub fn new(
        epoch: Epoch,
        since_slot: Slot,
        epoch_start: Slot,
        slots_in_epoch: u64,
        leader_slots: Vec<Slot>,
    ) -> Self {
        Self {
            epoch,
            since_slot,
            epoch_start,
            slots_in_epoch,
            leader_slots,
            ..Self::default()
        }
    }

    /// A slot with no certificate counts for nobody.
    pub fn add(&mut self, mark: &Mark, snapshots: &[Span], detail: Option<MissDetail>) {
        let paid_ranks = mark.paid.count_ones();
        match mark.reward {
            Reward::Paid => self.paid = self.paid.saturating_add(1),
            Reward::Unpaid => self.miss(mark, paid_ranks, snapshots, detail),
            Reward::NoCertificate => return,
        }
        self.rewarded = self.rewarded.saturating_add(1);
        if let Some(writer) = detail.and_then(|detail| detail.writer) {
            let count = self.writer_certs.entry(writer).or_default();
            *count = count.saturating_add(1);
        }
        if self
            .leader_slots
            .binary_search(&writer_slot(mark.slot))
            .is_ok()
        {
            self.ours.insert(
                mark.slot,
                Written {
                    paid: u32::try_from(paid_ranks).unwrap_or(u32::MAX),
                    notar: mark.notar,
                    skip: mark.skip,
                    ours_in: matches!(mark.reward, Reward::Paid),
                    unpaid: unpaid(&mark.paid, None),
                },
            );
        }
        if self.paid_ranks.len() <= paid_ranks {
            self.paid_ranks.resize(paid_ranks.saturating_add(1), 0);
        }
        if let Some(count) = self.paid_ranks.get_mut(paid_ranks) {
            *count = count.saturating_add(1);
        }
        if self.per_rank.len() < mark.paid.len() {
            self.per_rank.resize(mark.paid.len(), 0);
        }
        for rank in mark.paid.iter_ones() {
            if let Some(count) = self.per_rank.get_mut(rank) {
                *count = count.saturating_add(1);
            }
        }
    }

    fn miss(
        &mut self,
        mark: &Mark,
        paid_ranks: usize,
        snapshots: &[Span],
        detail: Option<MissDetail>,
    ) {
        let slot = mark.slot;
        let fixed = if slot < self.epoch_start.saturating_add(BOUNDARY_SLOTS) {
            Some(Place::Boundary)
        } else if self.leader_slots.binary_search(&slot).is_ok() {
            Some(Place::Leader)
        } else if snapshots.iter().any(|span| span.covers(slot)) {
            Some(Place::Snapshot)
        } else {
            None
        };
        let detail = detail.unwrap_or_default();
        self.by_slot.insert(slot, self.misses.len());
        self.misses.push(Miss {
            slot,
            fixed,
            paid_ranks: u32::try_from(paid_ranks).unwrap_or(u32::MAX),
            unpaid: unpaid(&mark.paid, Some(mark.rank)),
            writer: detail.writer,
            late: detail.late,
            vote: self.pending_votes.remove(&slot),
        });
    }

    /// Records when this node voted for `slot`, kept until its certificate is read: votor reports
    /// before the walk on some slots and after it on others.
    pub fn note_vote(&mut self, slot: Slot, vote: VoteSent) {
        if let Some(miss) = self
            .by_slot
            .get(&slot)
            .and_then(|at| self.misses.get_mut(*at))
        {
            miss.vote = Some(vote);
            return;
        }
        crate::metrics_tap::insert_capped(&mut self.pending_votes, slot, vote, PENDING_VOTES);
    }

    pub fn writer_certificates(&self, writer: &Pubkey) -> u64 {
        self.writer_certs.get(writer).copied().unwrap_or(0)
    }

    /// `None` where there was no certificate.
    pub fn left_out(&self, mark: &Mark) -> Option<u32> {
        if matches!(mark.reward, Reward::NoCertificate) {
            return None;
        }
        let regulars = self.regulars();
        let count = mark
            .paid
            .iter_zeros()
            .filter(|rank| regulars.get(*rank).is_some_and(|regular| *regular))
            .count();
        Some(u32::try_from(count).unwrap_or(u32::MAX))
    }

    pub fn written_for(&self, slot: Slot) -> Option<&Written> {
        self.ours.get(&slot)
    }

    pub fn written(&self) -> WrittenSummary {
        let regulars = self.regulars();
        let mut unpaid_by_rank = vec![0u64; self.per_rank.len()];
        let mut carried_all = 0u64;
        for written in self.ours.values() {
            let mut left_regular_out = false;
            for rank in &written.unpaid {
                let at = usize::from(*rank);
                if let Some(count) = unpaid_by_rank.get_mut(at) {
                    *count = count.saturating_add(1);
                }
                if regulars.get(at).is_some_and(|regular| *regular) {
                    left_regular_out = true;
                }
            }
            if !left_regular_out {
                carried_all = carried_all.saturating_add(1);
            }
        }
        WrittenSummary {
            certificates: u64::try_from(self.ours.len()).unwrap_or(u64::MAX),
            carried_all,
            unpaid_by_rank,
            unpaid_everywhere: self
                .per_rank
                .iter()
                .map(|paid| self.rewarded.saturating_sub(*paid))
                .collect(),
        }
    }

    pub fn ranks(&self) -> u32 {
        u32::try_from(self.per_rank.len()).unwrap_or(u32::MAX)
    }

    fn place_of(&self, miss: &Miss, thin_below: Option<u32>) -> Place {
        let decided = if thin_below.is_some_and(|below| miss.paid_ranks < below) {
            Place::Thin
        } else if miss.late {
            Place::Late
        } else {
            Place::Lost
        };
        miss.fixed.unwrap_or(decided)
    }

    pub fn regulars(&self) -> Vec<bool> {
        let floor = self.rewarded.saturating_mul(REGULAR_PERCENT);
        self.per_rank
            .iter()
            .map(|paid| self.rewarded > 0 && paid.saturating_mul(100) >= floor)
            .collect()
    }

    pub fn records(&self) -> Vec<MissRecord> {
        let thin_below = self.thin_below();
        let regulars = self.regulars();
        self.misses
            .iter()
            .map(|miss| {
                let others = miss
                    .unpaid
                    .iter()
                    .copied()
                    .filter(|rank| {
                        regulars
                            .get(usize::from(*rank))
                            .is_some_and(|regular| *regular)
                    })
                    .map(u32::from)
                    .collect();
                MissRecord {
                    slot: miss.slot,
                    place: self.place_of(miss, thin_below),
                    paid_ranks: miss.paid_ranks,
                    others,
                    writer: miss.writer,
                    vote: miss.vote,
                }
            })
            .collect()
    }

    /// `None` until `THIN_MIN_CERTIFICATES` are in.
    pub fn usual_paid(&self) -> Option<u32> {
        if self.rewarded < THIN_MIN_CERTIFICATES {
            return None;
        }
        let half = self.rewarded.checked_div(2)?;
        let mut seen = 0u64;
        for (ranks, count) in self.paid_ranks.iter().enumerate() {
            seen = seen.saturating_add(u64::from(*count));
            if seen > half {
                return u32::try_from(ranks).ok();
            }
        }
        None
    }

    fn thin_below(&self) -> Option<u32> {
        let median = u64::from(self.usual_paid()?);
        let shortfall = median
            .saturating_mul(THIN_SHORTFALL_PERCENT)
            .checked_div(100)?;
        u32::try_from(median.saturating_sub(shortfall)).ok()
    }

    /// `name` gives a lost vote's leader a display name.
    pub fn participation(&self, name: impl Fn(&Pubkey) -> Option<String>) -> Participation {
        let thin_below = self.thin_below();
        let mut misses = Misses::default();
        let mut miss_bins = vec![0u32; MISS_BINS];
        let mut by_writer: HashMap<Pubkey, u64> = HashMap::new();
        for miss in &self.misses {
            let place = self.place_of(miss, thin_below);
            let count = match place {
                Place::Boundary => &mut misses.boundary,
                Place::Leader => &mut misses.leader,
                Place::Snapshot => &mut misses.snapshot,
                Place::Thin => &mut misses.thin,
                Place::Late => &mut misses.late,
                Place::Lost => &mut misses.lost,
            };
            *count = count.saturating_add(1);
            if place == Place::Lost
                && let Some(writer) = miss.writer
            {
                let count = by_writer.entry(writer).or_default();
                *count = count.saturating_add(1);
            }
            let offset = miss.slot.saturating_sub(self.epoch_start);
            if let Some(bin) = miss_bins.get_mut(bin_of(offset, self.slots_in_epoch, MISS_BINS)) {
                *bin = bin.saturating_add(1);
            }
        }
        let mut leaders: Vec<(Pubkey, u64)> = by_writer.into_iter().collect();
        leaders.sort_by_key(|(key, count)| (Reverse(*count), *key));
        leaders.truncate(LOST_LEADERS);
        Participation {
            epoch: self.epoch,
            since_slot: self.since_slot,
            paid: self.paid,
            rewarded: self.rewarded,
            cluster_max: self.per_rank.iter().copied().max().unwrap_or(0),
            cluster_median: median_of(self.per_rank.clone()),
            misses,
            miss_bins,
            lost_leaders: leaders
                .into_iter()
                .map(|(identity, count)| LostLeader {
                    name: name(&identity),
                    identity: identity.to_string(),
                    count,
                })
                .collect(),
            ranks: self.ranks(),
            thin_below,
        }
    }
}

/// The ranks `paid` leaves out, but `except`.
fn unpaid(paid: &Ranks, except: Option<usize>) -> Box<[u16]> {
    paid.iter_zeros()
        .filter(|rank| Some(*rank) != except)
        .filter_map(|rank| u16::try_from(rank).ok())
        .collect()
}

pub enum Block {
    Footer(FooterView),
    Missing,
    Pending,
    Opaque,
}

/// What a walk reads of a block's footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterView {
    pub producer_time_nanos: u64,
    pub notar: Option<Vec<u8>>,
    pub skip: Option<Vec<u8>>,
    /// Whether its finalization certificate is the one-round fast one.
    pub fast: Option<bool>,
}

impl From<&BlockFooterV1> for FooterView {
    fn from(footer: &BlockFooterV1) -> Self {
        Self {
            producer_time_nanos: footer.block_producer_time_nanos,
            notar: footer
                .notar_reward_cert
                .as_ref()
                .map(|cert| cert.bitmap().to_vec()),
            skip: footer
                .skip_reward_cert
                .as_ref()
                .map(|cert| cert.to_bitmap().to_vec()),
            fast: carried_fast(footer),
        }
    }
}

/// What a walk read: the last slot, our marks, and when each block's leader closed it.
#[derive(Debug, Default)]
pub struct Walked {
    pub read_to: Option<Slot>,
    pub marks: Vec<Mark>,
    /// The footer's producer clock, unix nanoseconds, for every block read.
    pub closed_at: Vec<(Slot, u64)>,
    pub readings: Vec<FooterReading>,
}

/// What one block's footer says of the cluster rather than of this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterReading {
    pub slot: Slot,
    /// The slot its notarize reward certificate pays, and the stake in it in basis points.
    pub notarized: Option<(Slot, u16)>,
    /// Whether the finalization certificate it carries is the one-round fast certificate.
    pub fast: Option<bool>,
}

/// From any source of blocks and rank maps. Stops at the first slot past the root; footers are
/// read whether or not we hold a rank.
pub fn walk_blocks(
    read: impl Fn(Slot) -> Block,
    rank_map: impl Fn(Slot) -> Option<Arc<BLSPubkeyToRankMap>>,
    vote_account: &Pubkey,
    from: Slot,
    to: Slot,
) -> Walked {
    let mut walked = Walked::default();
    for slot in from..=to {
        let block = read(slot);
        if matches!(block, Block::Pending) {
            break;
        }
        let ranked = rewarded_slot(slot).and_then(|reward_slot| {
            rank_of(&rank_map, vote_account, reward_slot).map(|rank| (reward_slot, rank))
        });
        if let Block::Footer(footer) = &block {
            walked
                .readings
                .push(footer_reading(&rank_map, slot, footer));
        }
        let (closed_at, mark) = read_slot(block, ranked);
        if let Some(nanos) = closed_at {
            walked.closed_at.push((slot, nanos));
        }
        walked.marks.extend(mark);
        walked.read_to = Some(slot);
    }
    walked
}

/// The block's close time, and our mark where we hold a rank for the slot it rewards.
fn read_slot(block: Block, ranked: Option<(Slot, (usize, usize))>) -> (Option<u64>, Option<Mark>) {
    match block {
        Block::Footer(footer) => {
            let mark = ranked.and_then(|(reward_slot, (rank, len))| {
                build_mark(
                    footer.notar.as_deref(),
                    footer.skip.as_deref(),
                    reward_slot,
                    rank,
                    len,
                )
            });
            (Some(footer.producer_time_nanos), mark)
        }
        Block::Missing => (
            None,
            ranked.map(|(reward_slot, (rank, _))| Mark::no_certificate(reward_slot, rank)),
        ),
        Block::Pending | Block::Opaque => (None, None),
    }
}

/// A leader writes the highest finalization certificate it holds and prefers the fast one for
/// the same slot, so fast means 80% of stake notarized in time for it.
fn footer_reading(
    rank_map: &impl Fn(Slot) -> Option<Arc<BLSPubkeyToRankMap>>,
    slot: Slot,
    footer: &FooterView,
) -> FooterReading {
    let notarized = footer.notar.as_ref().and_then(|bitmap| {
        let rewarded = rewarded_slot(slot)?;
        Some((rewarded, notarized_share(rank_map, rewarded, bitmap)?))
    });
    FooterReading {
        slot,
        notarized,
        fast: footer.fast,
    }
}

/// A fast certificate is one notarize aggregate; a slow one adds the notarize aggregate to its
/// finalize votes.
fn carried_fast(footer: &BlockFooterV1) -> Option<bool> {
    footer
        .block_final_cert
        .as_ref()
        .map(|cert| cert.notar_aggregate.is_none())
}

/// The share of the slot's rank map stake set in `bitmap`, in basis points.
fn notarized_share(
    rank_map: &impl Fn(Slot) -> Option<Arc<BLSPubkeyToRankMap>>,
    slot: Slot,
    bitmap: &[u8],
) -> Option<u16> {
    let map = rank_map(slot)?;
    let notarized = decode_paid(bitmap, map.len())?
        .iter_ones()
        .filter_map(|rank| map.get_pubkey_stake_entry(rank))
        .fold(0u64, |sum, entry| sum.saturating_add(entry.stake.get()));
    let share = u128::from(notarized)
        .saturating_mul(10_000)
        .checked_div(u128::from(map.total_stake().get()))?;
    u16::try_from(share).ok()
}

/// The rank map's entries, by rank.
pub fn rank_entries(map: &BLSPubkeyToRankMap) -> impl Iterator<Item = &BLSPubkeyStakeEntry> {
    (0..map.len()).filter_map(|rank| map.get_pubkey_stake_entry(rank))
}

/// `None` where it holds no stake there.
fn rank_of(
    rank_map: &impl Fn(Slot) -> Option<Arc<BLSPubkeyToRankMap>>,
    vote_account: &Pubkey,
    slot: Slot,
) -> Option<(usize, usize)> {
    let map = rank_map(slot)?;
    let rank = map.get_rank_for_vote_pubkey(vote_account)?;
    Some((usize::from(*rank), map.len()))
}

/// Takes each certificate's bitmap. Certificates that set no rank count as none, as the runtime
/// reads them; `None` where one could not be read.
fn build_mark(
    notar: Option<&[u8]>,
    skip: Option<&[u8]>,
    slot: Slot,
    rank: usize,
    len: usize,
) -> Option<Mark> {
    let (notar, skip) = decode_both(notar, skip, len)?;
    let set = |ranks: &Ranks| u32::try_from(ranks.count_ones()).unwrap_or(u32::MAX);
    let (notar_set, skip_set) = (set(&notar), set(&skip));
    let paid = notar | skip;
    if paid.not_any() {
        return Some(Mark::no_certificate(slot, rank));
    }
    let reward = if paid.get(rank).is_some_and(|flag| *flag) {
        Reward::Paid
    } else {
        Reward::Unpaid
    };
    Some(Mark {
        slot,
        reward,
        rank,
        paid,
        notar: notar_set,
        skip: skip_set,
    })
}

/// Each reward certificate's ranks, none set where it is absent; `None` where one does not decode.
pub(crate) fn decode_both(
    notar: Option<&[u8]>,
    skip: Option<&[u8]>,
    len: usize,
) -> Option<(Ranks, Ranks)> {
    let ranks = |bitmap: Option<&[u8]>| match bitmap {
        Some(bitmap) => decode_paid(bitmap, len),
        None => Some(Ranks::repeat(false, len)),
    };
    Some((ranks(notar)?, ranks(skip)?))
}

/// `None` where the bitmap does not decode or uses the two-vector form. Ranks past a short bitmap
/// are unset.
fn decode_paid(bitmap: &[u8], len: usize) -> Option<Ranks> {
    let Ok(Decoded::Base2(mut ranks)) = decode(bitmap, len) else {
        return None;
    };
    ranks.resize(len, false);
    Some(ranks)
}

/// The median of the counts above nought, so validators that earned nothing do not drag it down.
pub fn median_of(mut counts: Vec<u64>) -> Option<u64> {
    counts.retain(|count| *count > 0);
    counts.sort_unstable();
    counts.get(counts.len().checked_sub(1)? / 2).copied()
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_entry::block_component::BlockFinalizationCert,
        solana_ledger::blockstore::make_slot_entries,
    };

    fn bitmap(len: u16, set: &[usize]) -> Vec<u8> {
        let mut bits = vec![0u8; usize::from(len).div_ceil(8)];
        for &rank in set {
            let bit = u32::try_from(rank.checked_rem(8).unwrap()).unwrap();
            bits[rank.checked_div(8).unwrap()] |= 1u8.checked_shl(bit).unwrap();
        }
        let mut bytes = vec![0u8];
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend(bits);
        bytes
    }

    fn flags(len: usize, set: &[usize]) -> Ranks {
        (0..len).map(|rank| set.contains(&rank)).collect()
    }

    fn mark(slot: Slot, reward: Reward, paid: &[usize]) -> Mark {
        Mark {
            slot,
            reward,
            rank: 0,
            paid: flags(4, paid),
            notar: u32::try_from(paid.len()).unwrap(),
            skip: 0,
        }
    }

    #[test]
    fn test_a_rank_in_either_certificate_was_paid() {
        let (notar, skip) = (bitmap(10, &[3, 9]), bitmap(10, &[4]));
        let mark = build_mark(Some(&notar), Some(&skip), 5, 4, 10).unwrap();
        assert_eq!(mark.paid, flags(10, &[3, 4, 9]));
        assert_eq!((mark.notar, mark.skip), (2, 1));
        assert_eq!(mark.reward, Reward::Paid);
    }

    #[test]
    fn test_a_short_bitmap_leaves_the_ranks_past_it_unpaid() {
        let paid = decode_paid(&bitmap(4, &[3]), 10).unwrap();
        assert_eq!(paid, flags(10, &[3]));
    }

    #[test]
    fn test_the_carried_certificate_is_read_as_fast_or_slow() {
        let (_, entries) = make_slot_entries(9, 8, 1);
        let mut footer = BlockFooterV1 {
            bank_hash: entries[0].hash,
            block_producer_time_nanos: 0,
            block_user_agent: Vec::new(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        };
        assert_eq!(carried_fast(&footer), None);
        let mut cert = BlockFinalizationCert::new_for_tests();
        footer.block_final_cert = Some(cert.clone());
        assert_eq!(carried_fast(&footer), Some(true));
        cert.notar_aggregate = Some(cert.final_aggregate.clone());
        footer.block_final_cert = Some(cert);
        assert_eq!(carried_fast(&footer), Some(false));
    }

    #[test]
    fn test_garbage_does_not_decode() {
        assert_eq!(decode_paid(&[0u8, 1], 10), None);
        assert_eq!(decode_paid(&[7u8, 10, 0, 0, 0], 10), None);
    }

    fn footer_closed_at(nanos: u64) -> Block {
        Block::Footer(FooterView {
            producer_time_nanos: nanos,
            notar: None,
            skip: None,
            fast: None,
        })
    }

    #[test]
    fn test_a_footer_is_timed_whether_or_not_we_hold_a_rank() {
        assert_eq!(read_slot(footer_closed_at(42), None), (Some(42), None));
        let (closed_at, mark) = read_slot(footer_closed_at(42), Some((5, (0, 10))));
        assert_eq!(closed_at, Some(42));
        assert_eq!(
            mark.map(|mark| (mark.slot, mark.reward)),
            Some((5, Reward::NoCertificate))
        );
    }

    #[test]
    fn test_a_skipped_slot_marks_only_a_ranked_reward() {
        assert_eq!(read_slot(Block::Missing, None), (None, None));
        let (closed_at, mark) = read_slot(Block::Missing, Some((5, (2, 10))));
        assert_eq!(closed_at, None);
        assert_eq!(
            mark.map(|mark| (mark.rank, mark.reward)),
            Some((2, Reward::NoCertificate))
        );
        assert_eq!(read_slot(Block::Opaque, Some((5, (2, 10)))), (None, None));
    }

    #[test]
    fn test_no_certificate_or_one_that_sets_no_rank_is_no_certificate() {
        let empty = bitmap(10, &[]);
        for (notar, skip) in [
            (None, None),
            (Some(empty.as_slice()), None),
            (None, Some(empty.as_slice())),
            (Some(empty.as_slice()), Some(empty.as_slice())),
        ] {
            let mark = build_mark(notar, skip, 3_000, 2, 10).unwrap();
            assert_eq!(mark.reward, Reward::NoCertificate);
            assert!(mark.paid.is_empty());
            let mut tally = tally();
            tally.add(&mark, &[], None);
            assert_eq!(tally.rewarded, 0, "not counted as rewarded");
            assert_eq!(misses(&tally), Misses::default(), "nor as a miss");
        }
    }

    fn tally() -> Tally {
        Tally::new(7, 100, 0, 4_000, vec![2_000, 2_001, 2_002, 2_003])
    }

    fn read(tally: &Tally) -> Participation {
        tally.participation(|_| None)
    }

    fn misses(tally: &Tally) -> Misses {
        read(tally).misses
    }

    fn detail(writer: Option<Pubkey>, late: bool) -> Option<MissDetail> {
        Some(MissDetail { writer, late })
    }

    /// A mark paying rank 1, its certificate written by `writer`.
    fn by(tally: &mut Tally, slot: Slot, reward: Reward, writer: Pubkey, late: bool) {
        tally.add(&mark(slot, reward, &[1]), &[], detail(Some(writer), late));
    }

    fn wide(slot: Slot, reward: Reward, paid: usize) -> Mark {
        Mark {
            slot,
            reward,
            rank: 0,
            paid: (0..20).map(|rank| rank < paid).collect(),
            notar: u32::try_from(paid).unwrap(),
            skip: 0,
        }
    }

    #[test]
    fn test_our_certificates_are_kept_by_what_they_left_out() {
        // Leader slots 2,000 to 2,003 write the certificates for 1,992 to 1,995.
        let mut tally = tally();
        fill(&mut tally, 100);
        tally.add(&wide(1_992, Reward::Paid, 20), &[], None);
        tally.add(&wide(1_993, Reward::Paid, 17), &[], None);
        tally.add(&wide(1_994, Reward::Paid, 18), &[], None);
        tally.add(&wide(3_000, Reward::Paid, 10), &[], None);
        let written = tally.written();
        assert_eq!(written.certificates, 3);
        // Ranks 18 and 19 are paid in seven of ten, so not regulars.
        assert_eq!(written.carried_all, 2);
        assert_eq!(written.unpaid_by_rank[17], 1);
        assert_eq!(written.unpaid_by_rank[19], 2);
        assert_eq!(written.unpaid_by_rank[0], 0);
        assert_eq!(written.unpaid_everywhere[19], 33);
        assert_eq!(written.unpaid_everywhere[0], 0);
    }

    #[test]
    fn test_a_certificate_we_wrote_is_kept_whole_by_the_slot_it_rewards() {
        assert_eq!(tally().usual_paid(), None);
        let mut tally = tally();
        fill(&mut tally, 100);
        tally.add(&wide(1_993, Reward::Paid, 17), &[], None);
        let written = tally.written_for(1_993).expect("ours");
        assert_eq!(written.paid, 17);
        assert_eq!(written.notar, 17);
        assert_eq!(written.skip, 0);
        assert!(written.ours_in);
        assert_eq!(*written.unpaid, [17, 18, 19]);
        assert!(tally.written_for(500).is_none());
        assert_eq!(tally.usual_paid(), Some(20));
    }

    #[test]
    fn test_the_highest_rank_upstream_allows_is_kept() {
        let last = usize::from(u16::MAX);
        let mut tally = tally();
        let mut mark = wide(1_992, Reward::Paid, 0);
        mark.paid = (0..=last).map(|rank| rank < last).collect();
        tally.add(&mark, &[], None);
        assert_eq!(*tally.written_for(1_992).expect("ours").unpaid, [u16::MAX]);
        mark.slot = 3_000;
        mark.reward = Reward::Unpaid;
        tally.add(&mark, &[], None);
        assert_eq!(*tally.misses[0].unpaid, [u16::MAX]);
    }

    #[test]
    fn test_left_out_counts_only_the_regulars_a_certificate_missed() {
        let mut tally = tally();
        fill(&mut tally, 100);
        assert_eq!(tally.left_out(&wide(500, Reward::Paid, 17)), Some(1));
        assert_eq!(tally.left_out(&wide(501, Reward::Paid, 20)), Some(0));
        assert_eq!(tally.left_out(&wide(502, Reward::NoCertificate, 0)), None);
    }

    /// Seventy paying all twenty ranks and thirty paying eighteen, so the median pays twenty.
    fn fill(tally: &mut Tally, from: Slot) {
        for (index, slot) in (from..from.saturating_add(THIN_MIN_CERTIFICATES)).enumerate() {
            let paid = if index < 70 { 20 } else { 18 };
            tally.add(&wide(slot, Reward::Paid, paid), &[], None);
        }
    }

    #[test]
    fn test_the_tally_counts_paid_slots_for_us_and_the_best_rank() {
        let mut tally = tally();
        tally.add(&mark(100, Reward::Paid, &[0, 1, 2]), &[], None);
        tally.add(&mark(101, Reward::Unpaid, &[1, 2]), &[], None);
        tally.add(&mark(102, Reward::NoCertificate, &[]), &[], None);
        tally.add(&mark(103, Reward::Paid, &[0, 1]), &[], None);
        let participation = read(&tally);
        assert_eq!(
            (
                participation.epoch,
                participation.since_slot,
                participation.paid,
                participation.rewarded,
                participation.cluster_max,
            ),
            (7, 100, 2, 3, 3)
        );
        assert_eq!(participation.misses.boundary, 1);
        assert_eq!(participation.miss_bins.iter().sum::<u32>(), 1);
    }

    #[test]
    fn test_an_empty_tally_has_no_best() {
        let participation = read(&tally());
        assert_eq!(participation.cluster_max, 0);
        assert_eq!(participation.cluster_median, None);
    }

    #[test]
    fn test_the_median_leaves_out_validators_that_earned_nothing() {
        let counts: Vec<u64> = (1..=11).map(|count| count * 10).chain([0, 0, 0]).collect();
        assert_eq!(median_of(counts), Some(60));
        assert_eq!(median_of(vec![7]), Some(7));
        assert_eq!(median_of(vec![0, 0]), None);
        assert_eq!(median_of(Vec::new()), None);
    }

    #[test]
    fn test_participation_reads_the_median_of_the_paid_ranks() {
        let mut tally = tally();
        tally.add(&mark(100, Reward::Paid, &[0, 1, 2]), &[], None);
        tally.add(&mark(101, Reward::Paid, &[0, 1]), &[], None);
        tally.add(&mark(102, Reward::Paid, &[0]), &[], None);
        let participation = read(&tally);
        assert_eq!(participation.cluster_max, 3);
        assert_eq!(participation.cluster_median, Some(2));
    }

    #[test]
    fn test_a_miss_is_placed_by_where_it_fell() {
        let mut tally = tally();
        let written = [Span {
            from: 3_000,
            to: Some(3_010),
        }];
        tally.add(&mark(999, Reward::Unpaid, &[1]), &written, None);
        tally.add(&mark(2_001, Reward::Unpaid, &[1]), &written, None);
        tally.add(&mark(3_005, Reward::Unpaid, &[1]), &written, None);
        tally.add(
            &mark(3_011, Reward::Unpaid, &[1]),
            &written,
            detail(None, true),
        );
        tally.add(&mark(3_012, Reward::Unpaid, &[1]), &written, None);
        assert_eq!(
            misses(&tally),
            Misses {
                boundary: 1,
                leader: 1,
                snapshot: 1,
                thin: 0,
                late: 1,
                lost: 1,
            }
        );
    }

    #[test]
    fn test_a_write_still_going_covers_every_slot_since_it_began() {
        let mut tally = tally();
        let writing = [Span {
            from: 3_000,
            to: None,
        }];
        tally.add(&mark(3_999, Reward::Unpaid, &[1]), &writing, None);
        tally.add(&mark(2_999, Reward::Unpaid, &[1]), &writing, None);
        assert_eq!(misses(&tally).snapshot, 1);
        assert_eq!(misses(&tally).lost, 1);
    }

    #[test]
    fn test_the_boundary_takes_a_leader_slot_within_it() {
        let mut tally = Tally::new(7, 0, 0, 4_000, vec![500]);
        tally.add(&mark(500, Reward::Unpaid, &[1]), &[], None);
        assert_eq!(misses(&tally).boundary, 1);
        assert_eq!(misses(&tally).leader, 0);
    }

    #[test]
    fn test_a_certificate_a_tenth_short_of_the_median_is_thin() {
        let mut tally = tally();
        fill(&mut tally, 1_000);
        // Under eighteen is thin; eighteen and nineteen fall through to late and lost.
        tally.add(&wide(3_000, Reward::Unpaid, 17), &[], None);
        tally.add(&wide(3_001, Reward::Unpaid, 18), &[], detail(None, true));
        tally.add(&wide(3_002, Reward::Unpaid, 19), &[], None);
        let misses = misses(&tally);
        assert_eq!((misses.thin, misses.late, misses.lost), (1, 1, 1));
    }

    #[test]
    fn test_nothing_is_thin_before_enough_certificates() {
        let mut tally = tally();
        tally.add(&mark(3_000, Reward::Unpaid, &[1]), &[], None);
        assert_eq!(misses(&tally).thin, 0);
        assert_eq!(misses(&tally).lost, 1);
        assert_eq!(read(&tally).thin_below, None);
    }

    #[test]
    fn test_the_cutoff_and_the_rank_count_are_published() {
        let mut tally = tally();
        fill(&mut tally, 1_000);
        let participation = read(&tally);
        assert_eq!(
            (participation.ranks, participation.thin_below),
            (20, Some(18))
        );
    }

    #[test]
    fn test_thin_is_read_against_the_certificates_seen_so_far() {
        // Lost when seen, thin once the epoch's certificates show it was.
        let mut tally = tally();
        tally.add(&mark(1_500, Reward::Unpaid, &[1]), &[], None);
        assert_eq!(misses(&tally).lost, 1);
        fill(&mut tally, 2_100);
        assert_eq!(misses(&tally).thin, 1);
    }

    #[test]
    fn test_a_miss_counts_the_regulars_left_out_beside_us() {
        let mut tally = tally();
        fill(&mut tally, 1_000);
        let mut alone = wide(3_000, Reward::Unpaid, 20);
        alone.paid.set(0, false);
        tally.add(&alone, &[], None);
        let mut with_others = wide(3_001, Reward::Unpaid, 20);
        for rank in [0, 3, 4, 19] {
            with_others.paid.set(rank, false);
        }
        tally.add(&with_others, &[], None);
        let records = tally.records();
        assert!(records[0].others.is_empty(), "only this node was left out");
        assert_eq!(
            records[1].others,
            vec![3, 4],
            "two regulars beside it, rank 19 is not one"
        );
        assert_eq!(records[1].paid_ranks, 16);
    }

    #[test]
    fn test_certificates_are_counted_per_writer_paid_or_not() {
        let mut tally = tally();
        let writer = Pubkey::new_unique();
        by(&mut tally, 3_000, Reward::Paid, writer, false);
        by(&mut tally, 3_001, Reward::Unpaid, writer, false);
        by(&mut tally, 3_002, Reward::NoCertificate, writer, false);
        assert_eq!(tally.writer_certificates(&writer), 2);
        assert_eq!(tally.writer_certificates(&Pubkey::new_unique()), 0);
    }

    #[test]
    fn test_a_vote_is_kept_for_a_miss_whichever_arrives_first() {
        let mut tally = tally();
        let vote = VoteSent {
            first_shred_us: Some(1_200),
            parent_ready_us: None,
            notarize_us: Some(413_200),
            skip_us: None,
        };
        tally.note_vote(3_000, vote);
        tally.add(&mark(3_000, Reward::Unpaid, &[1]), &[], None);
        tally.add(&mark(3_001, Reward::Unpaid, &[1]), &[], None);
        tally.note_vote(3_001, vote);
        tally.note_vote(2_999, vote);
        let records = tally.records();
        assert_eq!(records[0].vote, Some(vote));
        assert_eq!(records[1].vote, Some(vote));
        assert_eq!(tally.pending_votes.len(), 1);
    }

    #[test]
    fn test_lost_votes_are_counted_by_the_leader_who_wrote_them_out() {
        let mut tally = tally();
        let (a, b) = (Pubkey::new_unique(), Pubkey::new_unique());
        for slot in [3_000, 3_001, 3_002] {
            by(&mut tally, slot, Reward::Unpaid, a, false);
        }
        by(&mut tally, 3_003, Reward::Unpaid, b, false);
        by(&mut tally, 3_004, Reward::Unpaid, b, true);
        let named = |key: &Pubkey| (*key == a).then(|| "Alpha".to_string());
        let leaders = tally.participation(named).lost_leaders;
        assert_eq!(
            leaders,
            vec![
                LostLeader {
                    identity: a.to_string(),
                    name: Some("Alpha".to_string()),
                    count: 3,
                },
                LostLeader {
                    identity: b.to_string(),
                    name: None,
                    count: 1,
                },
            ]
        );
    }

    #[test]
    fn test_misses_fall_in_the_bin_for_their_place_in_the_epoch() {
        let mut tally = tally();
        // The last is past the epoch, and lands in the last bin.
        for slot in [0, 2_000, 2_001, 3_999, 9_000] {
            tally.add(&mark(slot, Reward::Unpaid, &[1]), &[], None);
        }
        let bins = read(&tally).miss_bins;
        assert_eq!(bins.len(), MISS_BINS);
        assert_eq!((bins[0], bins[200], bins[MISS_BINS - 1]), (1, 2, 2));
    }
}
