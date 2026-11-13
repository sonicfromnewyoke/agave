//! Our vote's reward certificates walked from block footers, the epoch's tally of them, and the
//! miss and written lists read from that tally.

use {
    super::{is_delinquent, json_or_null},
    crate::{
        certs,
        history::{HAS_CLOCK, SlotHistory},
        produced::{BlockCertificate, Release, Validator},
    },
    indexmap::IndexMap,
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    std::{collections::HashMap, net::IpAddr, sync::Arc},
};

/// How often the lists are rebuilt while nobody is asking.
pub const UNASKED_EVERY_MILLIS: u64 = 60_000;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MissList {
    pub epoch: Epoch,
    pub rewarded: u64,
    pub ranks: u32,
    pub writers: Vec<MissWriter>,
    pub validators: Vec<MissValidator>,
    /// Oldest first.
    pub rows: Vec<MissRow>,
}

/// A running validator re-signs its contact every 7.5 seconds.
const NO_GOSSIP_AFTER_MILLIS: u64 = 5 * 60 * 1000;

/// Unheard for five minutes, or absent from the table, as a staked node is two days after it stops.
fn is_quiet(heard_millis: Option<u64>, now_millis: u64) -> bool {
    heard_millis.is_none_or(|at| now_millis.saturating_sub(at) > NO_GOSSIP_AFTER_MILLIS)
}

/// Each staked validator's stalest vote, `None` where one of its accounts never voted.
pub type LastVotes = HashMap<Pubkey, Option<Slot>>;

/// A validator's stalest vote and whether it is delinquent by it. One missing from the vote
/// accounts, as when its stake is gone, is neither.
pub fn standing(key: &Pubkey, votes: &LastVotes, tip: Slot) -> (Option<Slot>, bool) {
    match votes.get(key) {
        Some(last_vote) => (*last_vote, is_delinquent(*last_vote, tip)),
        None => (None, false),
    }
}

/// What the lists show of a validator: its name and its contact, with when it was last heard.
pub struct Described {
    pub name: Option<String>,
    pub client: Option<String>,
    pub version: Option<String>,
    pub ip: Option<IpAddr>,
    pub country: Option<&'static str>,
    pub heard_millis: Option<u64>,
}

impl Described {
    /// The validator as a list shows it, with its client and version where `released`.
    fn validator(self, identity: Pubkey, released: bool) -> Validator {
        Validator {
            identity,
            name: self.name,
            release: released.then_some(Release {
                client: self.client,
                version: self.version,
            }),
            ip: self.ip,
            country: self.country,
        }
    }
}

/// The miss and written lists of the epoch's tally, from any source of the validators' gossip,
/// names and votes.
pub fn miss_lists(
    tally: &certs::Tally,
    rank_map: Option<&BLSPubkeyToRankMap>,
    describe: impl Fn(&Pubkey) -> Described,
    standing: impl Fn(&Pubkey) -> (Option<Slot>, bool),
    time_of: impl Fn(Slot) -> Option<u64>,
    now_millis: u64,
) -> (MissList, WrittenList) {
    let node_of = |rank: usize| {
        rank_map?
            .get_pubkey_stake_entry(rank)
            .map(|entry| entry.node_pubkey)
    };
    let mut writers: IndexMap<Pubkey, MissWriter> = IndexMap::new();
    let mut validators: IndexMap<u32, MissValidator> = IndexMap::new();
    let rows = tally
        .records()
        .iter()
        .map(|record| {
            let others = record
                .others
                .iter()
                .filter_map(|rank| {
                    if let Some(at) = validators.get_index_of(rank) {
                        return u32::try_from(at).ok();
                    }
                    let key = node_of(usize::try_from(*rank).ok()?)?;
                    let described = describe(&key);
                    let (_, delinquent) = standing(&key);
                    let no_gossip = is_quiet(described.heard_millis, now_millis);
                    let validator = MissValidator {
                        validator: described.validator(key, false),
                        delinquent,
                        no_gossip,
                    };
                    u32::try_from(validators.insert_full(*rank, validator).0).ok()
                })
                .collect();
            let writer = record.writer.map(|key| {
                let entry = writers.entry(key);
                let at = u32::try_from(entry.index()).unwrap_or(u32::MAX);
                let writer = entry.or_insert_with(|| MissWriter {
                    validator: describe(&key).validator(key, true),
                    certificates: tally.writer_certificates(&key),
                    misses: 0,
                });
                writer.misses = writer.misses.saturating_add(1);
                at
            });
            MissRow {
                slot: record.slot,
                time_millis: time_of(record.slot),
                place: record.place,
                paid_ranks: record.paid_ranks,
                others,
                writer,
                vote: record.vote,
            }
        })
        .collect();
    let summary = tally.written();
    let written_rows = (0..summary.unpaid_everywhere.len())
        .filter_map(|rank| {
            let key = node_of(rank)?;
            let described = describe(&key);
            let heard_millis = described.heard_millis;
            let (last_vote, delinquent) = standing(&key);
            Some(WrittenRow {
                validator: described.validator(key, true),
                left_out_of_ours: summary.unpaid_by_rank.get(rank).copied().unwrap_or(0),
                left_out_everywhere: summary.unpaid_everywhere.get(rank).copied().unwrap_or(0),
                last_vote,
                delinquent,
                heard_millis,
                no_gossip: is_quiet(heard_millis, now_millis),
            })
        })
        .collect();
    let list = MissList {
        epoch: tally.epoch,
        rewarded: tally.rewarded,
        ranks: tally.ranks(),
        writers: writers.into_values().collect(),
        validators: validators.into_values().collect(),
        rows,
    };
    let written = WrittenList {
        rewarded: tally.rewarded,
        certificates: summary.certificates,
        carried_all: summary.carried_all,
        rows: written_rows,
    };
    (list, written)
}

/// The certificate each of our blocks in `pending` wrote, where the tally holds it.
pub fn block_certificates(
    tally: &certs::Tally,
    pending: Vec<Slot>,
    rank_map: impl Fn(Slot) -> Option<Arc<BLSPubkeyToRankMap>>,
    describe: impl Fn(&Pubkey) -> Described,
    leader: impl Fn(Slot) -> Option<Pubkey>,
) -> Vec<(Slot, BlockCertificate)> {
    let regulars = tally.regulars();
    let mut found = Vec::new();
    for slot in pending {
        let Some(rewards) = certs::rewarded_slot(slot) else {
            continue;
        };
        let Some(written) = tally.written_for(rewards) else {
            continue;
        };
        let map = rank_map(rewards);
        let map = map.as_deref();
        let ranks = map.map_or(0, |map| map.len());
        let stake_of = |rank: usize| {
            map.and_then(|map| map.get_pubkey_stake_entry(rank))
                .map_or(0, |entry| entry.stake.get())
        };
        let total = map.map_or(0, |map| map.total_stake().get());
        let unpaid = written
            .unpaid
            .iter()
            .map(|rank| usize::from(*rank))
            .fold(0u64, |sum, rank| sum.saturating_add(stake_of(rank)));
        let stake_paid = if total > 0 {
            total.saturating_sub(unpaid) as f64 / total as f64
        } else {
            0.0
        };
        let mut left_out: Vec<Validator> = written
            .unpaid
            .iter()
            .filter_map(|rank| {
                let at = usize::from(*rank);
                if !regulars.get(at).copied().unwrap_or(false) {
                    return None;
                }
                let key = map
                    .and_then(|map| map.get_pubkey_stake_entry(at))
                    .map(|entry| entry.node_pubkey)?;
                Some(describe(&key).validator(key, false))
            })
            .collect();
        // Held with the block, so kept without growth slack.
        left_out.shrink_to_fit();
        let leader = leader(rewards);
        found.push((
            slot,
            BlockCertificate {
                rewards,
                leader,
                leader_name: leader.and_then(|key| describe(&key).name),
                // A tie reads as notarized.
                notarized: written.notar >= written.skip,
                paid: written.paid,
                ranks: u32::try_from(ranks).unwrap_or(u32::MAX),
                stake_paid,
                notar: written.notar,
                skip: written.skip,
                ours_in: written.ours_in,
                usual: tally.usual_paid(),
                left_out,
            },
        ));
    }
    found
}

/// A slot's time, where the history holds its clock.
pub fn clock_time(history: &SlotHistory, slot: Slot) -> Option<u64> {
    history
        .get(slot)
        .filter(|row| (row.flags & HAS_CLOCK) != 0)
        .map(|row| row.time_millis)
}

pub struct MissReplies {
    pub misses: Arc<str>,
    pub written: Arc<str>,
}

impl MissReplies {
    pub fn new(misses: &MissList, written: &WrittenList) -> Self {
        Self {
            misses: json_or_null(misses),
            written: json_or_null(written),
        }
    }
}

impl Default for MissReplies {
    fn default() -> Self {
        Self::new(&MissList::default(), &WrittenList::default())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct WrittenList {
    /// Certificates from any writer that paid anybody, which `left_out_everywhere`
    /// is of.
    pub rewarded: u64,
    pub certificates: u64,
    /// Of those, the ones that left no regular out.
    pub carried_all: u64,
    pub rows: Vec<WrittenRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WrittenRow {
    #[serde(flatten)]
    pub validator: Validator,
    pub left_out_of_ours: u64,
    pub left_out_everywhere: u64,
    pub last_vote: Option<Slot>,
    pub delinquent: bool,
    /// When this node last heard it over gossip, in unix milliseconds.
    pub heard_millis: Option<u64>,
    pub no_gossip: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MissValidator {
    #[serde(flatten)]
    pub validator: Validator,
    pub delinquent: bool,
    pub no_gossip: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MissWriter {
    #[serde(flatten)]
    pub validator: Validator,
    pub certificates: u64,
    pub misses: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MissRow {
    pub slot: Slot,
    /// In unix milliseconds.
    pub time_millis: Option<u64>,
    pub place: certs::Place,
    pub paid_ranks: u32,
    /// Indices into `validators`: the regulars left out beside this node.
    pub others: Vec<u32>,
    pub writer: Option<u32>,
    pub vote: Option<certs::VoteSent>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standing_reads_the_stalest_vote_against_the_tip() {
        let [voting, behind, silent, gone] = std::array::from_fn(|_| Pubkey::new_unique());
        let votes = LastVotes::from([(voting, Some(990)), (behind, Some(10)), (silent, None)]);
        assert_eq!(standing(&voting, &votes, 1_000), (Some(990), false));
        assert_eq!(standing(&behind, &votes, 1_000), (Some(10), true));
        assert_eq!(standing(&silent, &votes, 1_000), (None, true));
        assert_eq!(
            standing(&gone, &votes, 1_000),
            (None, false),
            "unknown is not delinquent"
        );
    }

    #[test]
    fn test_a_validator_unheard_for_five_minutes_has_no_gossip() {
        let now = 10 * NO_GOSSIP_AFTER_MILLIS;
        assert!(!is_quiet(Some(now), now));
        assert!(
            !is_quiet(Some(now - NO_GOSSIP_AFTER_MILLIS), now),
            "exactly at the limit is heard"
        );
        assert!(is_quiet(Some(now - NO_GOSSIP_AFTER_MILLIS - 1), now));
        assert!(is_quiet(None, now), "absent from the table");
        assert!(
            !is_quiet(Some(now + 1), now),
            "a timestamp ahead of the clock is heard"
        );
    }
}
