//! Alpenglow's certificates, from the footers the ledger holds and the rank maps read off RPC.

use {
    super::{
        Followed, SlotFeed,
        cluster::{self, leader_slots},
    },
    crate::{
        certs::{self, Block, Participation, Tally},
        collect::{
            cert_formation::{CertFormation, FinalizedPaths, FormationKey, cert_formation},
            certificates::{
                Described, MissReplies, UNASKED_EVERY_MILLIS, block_certificates, clock_time,
                miss_lists, standing,
            },
            earnings::{
                BLOCKS_PER_TICK, EpochEarnings, Inflation, block_bonus, paid_ranks, rank_stakes,
                read_inflation, slot_reward, voter_half,
            },
            finalization::{FinalizationShare, FinalizationTally, Places, VOTE_WINDOW_MILLIS},
        },
        proto::TOPIC_SUMMARY,
        remote::Remote,
        validator_info::ValidatorInfoCache,
    },
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_time_utils::timestamp,
    std::{collections::HashMap, str::FromStr},
};

/// How far one tick's walk reads.
const CERT_SLOTS_PER_TICK: u64 = 64;

debounced_keys! {
    pub(super) struct Certificates {
        cert_formation: Option<CertFormation> = (TOPIC_SUMMARY, "cert_formation"),
        participation: Participation = (TOPIC_SUMMARY, "vote_participation"),
        finalization_share: Option<FinalizationShare> = (TOPIC_SUMMARY, "finalization_share"),
        epoch_earnings: Option<EpochEarnings> = (TOPIC_SUMMARY, "epoch_earnings"),
        ;
        /// The slot the walk started at and the last it read.
        walk: Option<(Slot, Slot)> = None,
        tally: Option<Tally> = None,
        /// The epoch before's, for our blocks captured after the tally moved on.
        previous: Option<Tally> = None,
        /// The epoch and time the miss lists were last made.
        listed: Option<(Epoch, u64)> = None,
        finalized_paths: FinalizedPaths = FinalizedPaths::default(),
        formation_for: Option<FormationKey> = None,
        identity: Option<Pubkey> = None,
        pub(super) finalization: FinalizationTally = FinalizationTally::default(),
        earnings: EarningsWalk = EarningsWalk::default(),
    }
}

/// Our blocks of the epoch the root has passed, read for the bonus their certificates pay.
#[derive(Default)]
struct EarningsWalk {
    key: Option<(Epoch, Pubkey)>,
    /// The inflation, our voter half for a slot and the bonus of a block every rank signed.
    rates: Option<(Inflation, u64, u64)>,
    leader_slots: Vec<Slot>,
    next: usize,
    bonus: u64,
    bonus_lost: u64,
    unread: u32,
}

impl SlotFeed {
    /// Each tick: the footers the root has passed, into the epoch's tally.
    pub(super) fn collect_vote_certs(&mut self) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        if remote.alpenglow != Some(true) || self.followed.completed == 0 {
            return;
        }
        let (Some(vote_account), Some(schedule), Some(me)) = (
            remote.vote.as_ref().and_then(|vote| vote.vote_pubkey),
            remote.epoch_schedule.clone(),
            remote.identity,
        ) else {
            return;
        };
        let root = self.followed.root;
        // From the first slot followed, whose footers are held.
        let (floor, from) = match self.certificates.walk {
            Some((floor, read_to)) => (floor, read_to.saturating_add(1)),
            None => {
                let first = self.followed.first_observed_slot.unwrap_or(root).min(root);
                (first, first)
            }
        };
        let to = root.min(from.saturating_add(CERT_SLOTS_PER_TICK).saturating_sub(1));
        if to < from {
            return;
        }
        let followed = &self.followed;
        // The walk waits for RPC's read of the rank map of the epoch a footer rewards.
        let ranked = |slot| {
            certs::rewarded_slot(slot)
                .is_none_or(|rewarded| remote.rank_maps.contains_key(&schedule.get_epoch(rewarded)))
        };
        let walked = certs::walk_blocks(
            |slot| match ranked(slot) {
                true => block_at(followed, slot, root),
                false => Block::Pending,
            },
            |slot| remote.rank_maps.get(&schedule.get_epoch(slot)).cloned(),
            &vote_account,
            from,
            to,
        );
        for (slot, nanos) in walked.closed_at {
            self.tap.note_block_closed(slot, nanos);
            self.followed.block_lives.note_closed(slot, nanos);
        }
        if !walked.readings.is_empty() {
            let mut history = self.history.write().unwrap();
            for reading in walked.readings {
                if let Some((rewarded, basis_points)) = reading.notarized {
                    history.record_notarized(rewarded, basis_points);
                }
                if let Some(fast) = reading.fast {
                    history.record_final_path(reading.slot, fast);
                }
            }
        }
        for votes in self.tap.take_block_votes() {
            self.followed
                .block_lives
                .note_votes(votes.slot, votes.vote_us, votes.finalized_us);
        }
        self.certificates.walk = Some((floor, walked.read_to.unwrap_or(from.saturating_sub(1))));
        if let Some(tally) = self.certificates.tally.as_mut() {
            for (slot, vote) in self.tap.take_vote_tracks() {
                tally.note_vote(slot, vote);
            }
        }
        // A new identity's leader slots, for the tally under way.
        if self.certificates.identity.replace(me) != Some(me)
            && let Some(tally) = self.certificates.tally.as_mut()
        {
            tally.leader_slots = leader_slots(&remote, tally.epoch, me).unwrap_or_default();
        }
        for mark in walked.marks {
            if mark.slot < floor {
                continue;
            }
            let epoch = schedule.get_epoch(mark.slot);
            if self
                .certificates
                .tally
                .as_ref()
                .is_none_or(|tally| tally.epoch != epoch)
            {
                self.certificates.previous = self.certificates.tally.replace(Tally::new(
                    epoch,
                    mark.slot,
                    schedule.get_first_slot_in_epoch(epoch),
                    schedule.get_slots_in_epoch(epoch),
                    leader_slots(&remote, epoch, me).unwrap_or_default(),
                ));
            }
            let detail = Some(miss_detail(&self.followed, &remote, mark.slot));
            let left_out = self.certificates.tally.as_mut().and_then(|tally| {
                tally.add(&mark, &remote.snapshot_spans, detail);
                tally.left_out(&mark)
            });
            if let Some(entry) = self.followed.slots.update(mark.slot, |entry| {
                entry.reward = Some(mark.reward);
                entry.left_out = left_out.map(|count| u16::try_from(count).unwrap_or(u16::MAX));
            }) {
                self.publish_slot(&entry);
            }
        }
        if let Some(tally) = &self.certificates.tally {
            let names = self.info_cache.read().unwrap();
            let participation =
                tally.participation(|leader| names.get(leader).and_then(|info| info.name.clone()));
            drop(names);
            self.certificates
                .participation
                .publish(&self.publisher, participation);
        }
    }

    /// For each frozen block: the ranked vote accounts' last votes, as RPC last read them.
    pub(super) fn observe_finalization(&mut self, slot: Slot) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let (Some(true), Some(schedule), Some(accounts)) = (
            remote.alpenglow,
            remote.epoch_schedule.as_ref(),
            remote.vote_accounts.as_ref(),
        ) else {
            return;
        };
        let Some(ranks) = remote.rank_maps.get(&schedule.get_epoch(slot)) else {
            return;
        };
        let votes: Vec<(Pubkey, Slot)> = accounts
            .rows
            .iter()
            .filter(|row| row.last_vote > 0)
            .filter_map(|row| Some((Pubkey::from_str(&row.vote_pubkey).ok()?, row.last_vote)))
            .filter(|(vote, _)| ranks.get_rank_for_vote_pubkey(vote).is_some())
            .collect();
        self.certificates.finalization.observe(&votes, timestamp());
    }

    /// With a viewer, every slow tick.
    pub(super) fn collect_finalization_share(&mut self) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let now_millis = timestamp();
        let share = (remote.alpenglow == Some(true))
            .then(|| {
                let ours = remote.vote.as_ref()?.vote_pubkey?;
                let vote_micros = self.tap.vote_lag_micros(now_millis, VOTE_WINDOW_MILLIS);
                let places = places(&remote, ours);
                self.certificates
                    .finalization
                    .summary(&ours, now_millis, vote_micros, &places)
            })
            .flatten();
        self.certificates
            .finalization_share
            .publish(&self.publisher, share);
    }

    /// Each tick, so a page left closed loses no slot.
    pub(super) fn collect_finalized_paths(&mut self) {
        let Some(schedule) = self.remote.read().unwrap().epoch_schedule.clone() else {
            return;
        };
        for (slot, fast) in self.tap.take_finalized_paths() {
            self.certificates
                .finalized_paths
                .note(schedule.get_epoch(slot), fast);
        }
    }

    /// Every slow tick: built again only when votor reports a new epoch, or that epoch's slot time
    /// or path counts move, once RPC has read the epoch's rank map.
    pub(super) fn collect_cert_formation(&mut self) {
        let Some(epoch) = self.tap.vote_arrival_epoch() else {
            return;
        };
        let slot_nanos = self.followed.clock.epoch_mean_nanos(epoch);
        let paths = self.certificates.finalized_paths.counts(epoch);
        if self.certificates.formation_for == Some((epoch, slot_nanos, paths)) {
            return;
        }
        let (held, arrivals) = self.tap.vote_arrivals();
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let Some(rank_map) = remote.rank_maps.get(&epoch).filter(|_| held == Some(epoch)) else {
            return;
        };
        self.certificates.formation_for = Some((epoch, slot_nanos, paths));
        let formation = cert_formation(rank_map, epoch, &arrivals, slot_nanos, paths);
        self.certificates
            .cert_formation
            .publish(&self.publisher, formation);
    }

    /// Every slow tick, the miss and written lists: while asked for, else once a minute.
    pub(super) fn collect_miss_list(&mut self) {
        let Some(tally) = &self.certificates.tally else {
            return;
        };
        let now_millis = timestamp();
        let asked = self.replies.misses.wanted(now_millis);
        let recent = self.certificates.listed.is_some_and(|(epoch, at)| {
            epoch == tally.epoch && now_millis.saturating_sub(at) < UNASKED_EVERY_MILLIS
        });
        let remote = self.remote.read().unwrap();
        let Some(accounts) = remote.vote_accounts.as_ref().filter(|_| asked || !recent) else {
            return;
        };
        let votes = cluster::tally(accounts).stalest_votes;
        let names = self.info_cache.read().unwrap();
        let history = self.history.read().unwrap();
        let (list, written) = miss_lists(
            tally,
            remote.rank_maps.get(&tally.epoch).map(|map| &**map),
            |key| describe(&remote, &names, key),
            |key| standing(key, &votes, accounts.slot),
            |slot| clock_time(&history, slot),
            now_millis,
        );
        drop((history, names, remote));
        *self.replies.misses.held.write().unwrap() = MissReplies::new(&list, &written);
        self.certificates.listed = Some((list.epoch, now_millis));
    }

    /// Every slow tick: the certificate each of our blocks wrote.
    pub(super) fn fill_certificates(&mut self) {
        let pending = self.replies.produced.read().unwrap().without_certificate();
        let remote = self.remote.read().unwrap();
        let Some(schedule) = remote
            .epoch_schedule
            .as_ref()
            .filter(|_| !pending.is_empty())
        else {
            return;
        };
        let names = self.info_cache.read().unwrap();
        let certificates = &self.certificates;
        let found: Vec<_> = [&certificates.previous, &certificates.tally]
            .into_iter()
            .flatten()
            .flat_map(|tally| {
                block_certificates(
                    tally,
                    pending.clone(),
                    |slot| remote.rank_maps.get(&schedule.get_epoch(slot)).cloned(),
                    |key| describe(&remote, &names, key),
                    |slot| remote.slot_leader(slot).copied(),
                )
            })
            .collect();
        drop((names, remote));
        let mut store = self.replies.produced.write().unwrap();
        for (slot, certificate) in found {
            store.set_certificate(slot, certificate);
        }
    }

    /// Each tick: `None` until every block of ours the root has passed is read.
    pub(super) fn collect_earnings(&mut self) {
        let earnings = self.read_earnings();
        self.certificates
            .epoch_earnings
            .publish(&self.publisher, earnings);
    }

    fn read_earnings(&mut self) -> Option<EpochEarnings> {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        if remote.alpenglow != Some(true) {
            return None;
        }
        let schedule = remote.epoch_schedule.as_ref()?;
        let (me, vote) = (remote.identity?, remote.vote.as_ref()?);
        let vote_account = vote.vote_pubkey?;
        // The credits RPC read cover the root as it stood then.
        let root = self.followed.root.min(vote.root);
        let epoch = schedule.get_epoch(root);
        let walk = &mut self.certificates.earnings;
        if walk.key != Some((epoch, me)) {
            // The walk waits for RPC's read of this epoch's inputs.
            *walk = EarningsWalk {
                key: Some((epoch, me)),
                rates: Some(rates(&remote, epoch, &vote_account)?),
                leader_slots: leader_slots(&remote, epoch, me)?,
                ..EarningsWalk::default()
            };
        }
        let (inflation, voter_slot, full_block) = walk.rates?;
        let held_from = self.followed.first_observed_slot.unwrap_or(0);
        let mut read = 0usize;
        while let Some(slot) = walk.leader_slots.get(walk.next).copied() {
            if slot > root || read == BLOCKS_PER_TICK {
                break;
            }
            // Under the first slot followed a block was never seen here.
            let block = if slot < held_from {
                None
            } else {
                match block_at(&self.followed, slot, root) {
                    Block::Pending => break,
                    Block::Opaque => None,
                    Block::Missing => Some(None),
                    Block::Footer(footer) => Some(Some(footer)),
                }
            };
            let counted = block.and_then(|footer| {
                let Some(rewarded) = certs::rewarded_slot(slot) else {
                    return Some((0, 0));
                };
                let map = remote.rank_maps.get(&schedule.get_epoch(rewarded))?;
                let paid = match &footer {
                    Some(footer) => paid_ranks(footer, map.len())?,
                    None => certs::Ranks::repeat(false, map.len()),
                };
                let total = total_stake(&remote);
                Some(block_bonus(rank_stakes(map).zip(paid), &inflation, total))
            });
            match counted {
                Some((got, full)) => {
                    walk.bonus = walk.bonus.saturating_add(got);
                    walk.bonus_lost = walk.bonus_lost.saturating_add(full.saturating_sub(got));
                }
                None => walk.unread = walk.unread.saturating_add(1),
            }
            walk.next = walk.next.saturating_add(1);
            read = read.saturating_add(1);
        }
        if walk
            .leader_slots
            .get(walk.next)
            .is_some_and(|slot| *slot <= root)
        {
            return None;
        }
        let view = vote.account.as_ref()?.vote_state_view();
        let credits = view
            .epoch_credits_iter()
            .last()
            .filter(|item| item.epoch() == epoch)
            .map_or(0, |item| item.credits().saturating_sub(item.prev_credits()));
        let voting = credits.saturating_sub(walk.bonus);
        let passed = root
            .saturating_sub(schedule.get_first_slot_in_epoch(epoch))
            .saturating_add(1);
        let blocks = u64::try_from(walk.leader_slots.len()).unwrap_or(u64::MAX);
        Some(EpochEarnings {
            epoch,
            voting,
            voting_lost: passed.saturating_mul(voter_slot).saturating_sub(voting),
            voting_max: inflation.slots.saturating_mul(voter_slot),
            bonus: walk.bonus,
            bonus_lost: walk.bonus_lost,
            bonus_max: blocks.saturating_mul(full_block),
            commission_bps: view.inflation_rewards_commission(),
            unread: walk.unread,
        })
    }
}

/// One epoch's pay rates, off its rank map.
fn rates(remote: &Remote, epoch: Epoch, vote_account: &Pubkey) -> Option<(Inflation, u64, u64)> {
    let inflation = read_inflation(remote.vote_reward.as_ref()?, epoch)?;
    let map = remote.rank_maps.get(&epoch)?;
    let total = total_stake(remote);
    let rank = *map.get_rank_for_vote_pubkey(vote_account)?;
    let ours = map.get_pubkey_stake_entry(usize::from(rank))?.stake.get();
    let (_, full_block) = block_bonus(
        rank_stakes(map).map(|stake| (stake, true)),
        &inflation,
        total,
    );
    Some((
        inflation,
        voter_half(slot_reward(&inflation, total, ours)),
        full_block,
    ))
}

/// The epoch's stake, here every vote account's as RPC last read it.
fn total_stake(remote: &Remote) -> u64 {
    remote
        .vote_accounts
        .iter()
        .flat_map(|accounts| &accounts.rows)
        .map(|row| row.activated_stake)
        .sum()
}

/// Countries from gossip addresses; empty when our own address has none.
fn places(remote: &Remote, vote_account: Pubkey) -> Places {
    let Some(ours) = remote
        .identity
        .and_then(|identity| remote.country(&identity))
    else {
        return Places::default();
    };
    let mut by_vote: HashMap<Pubkey, &'static str> = remote
        .vote_accounts
        .iter()
        .flat_map(|accounts| &accounts.rows)
        .filter(|row| row.activated_stake > 0)
        .filter_map(|row| {
            let node = Pubkey::from_str(&row.node_pubkey).ok()?;
            Some((
                Pubkey::from_str(&row.vote_pubkey).ok()?,
                remote.country(&node)?,
            ))
        })
        .collect();
    // The gossip table's copy leaves this node out.
    by_vote.insert(vote_account, ours);
    Places {
        ours: Some(ours),
        by_vote,
    }
}

/// A validator as its gossip table entry and its name give it. RPC lists the table's entries but
/// not when each was last heard: one in it reads as heard now.
fn describe(remote: &Remote, names: &ValidatorInfoCache, key: &Pubkey) -> Described {
    let contact = remote.gossip.get(key);
    Described {
        name: names.get(key).and_then(|info| info.name.clone()),
        client: contact.map(|contact| contact.client.clone()),
        version: contact.map(|contact| contact.version.clone()),
        ip: contact.and_then(|contact| Some(contact.addresses.gossip?.ip())),
        country: contact.and_then(|contact| contact.country()),
        heard_millis: contact.map(|_| timestamp()),
    }
}

/// Pending past the root; skipped where the rooted chain passed it by.
fn block_at(followed: &Followed, slot: Slot, root: Slot) -> Block {
    if slot > root {
        return Block::Pending;
    }
    if !followed.rooted.contains(&slot) {
        return Block::Missing;
    }
    followed
        .footer_views
        .get(&slot)
        .cloned()
        .map_or(Block::Opaque, Block::Footer)
}

/// Who wrote the certificate for `slot`, and whether this node finished replaying `slot` only
/// after that leader's own slot had begun arriving.
fn miss_detail(followed: &Followed, remote: &Remote, slot: Slot) -> certs::MissDetail {
    let writer_slot = certs::writer_slot(slot);
    let replayed_at = followed.slots.get(slot).and_then(|voted| {
        let (arrived, replayed) = voted.time_millis.zip(voted.replayed_millis)?;
        Some(arrived.saturating_add(replayed))
    });
    let writer_arrived = followed
        .slots
        .get(writer_slot)
        .and_then(|written| written.time_millis);
    certs::MissDetail {
        writer: remote.slot_leader(writer_slot).copied(),
        late: replayed_at
            .zip(writer_arrived)
            .is_some_and(|(ours, theirs)| ours > theirs),
    }
}
