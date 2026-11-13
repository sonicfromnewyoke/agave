//! Our vote account, the vote accounts and the gossip table, from RPC.

use {
    super::SlotFeed,
    crate::{
        certs::median_of,
        collect::{
            Admission, Consensus, Encoded, EpochInfo, Health, NANOS_PER_DAY, Peer,
            SLOT_OVERVIEW_LEN, SkipRate, StakeSummary, StakeTally, UPCOMING_SLOTS, ValidatorCounts,
            VersionShare, VoteCost, VoteCredits, assess_health,
            cluster_charts::{Described, GATHER_EVERY_MILLIS, cluster_charts, skip_share},
            gossip_peers::{Gossiped, gossip_peers},
            is_delinquent, json_or_null,
            skip_rate::{LeaderSlotCounts, SkipRateWalk},
            strip_prerelease, tally_stake, version_shares,
        },
        meter_source::WorkingReading,
        proto::{TOPIC_EPOCH, TOPIC_PEERS, TOPIC_SUMMARY},
        remote::{Contact, Remote, VoteAccounts},
        search::{LeaderMap, LeaderTraits},
        startup::{GossipStake, KEY_GOSSIP_STAKE, tally_gossip_stake},
    },
    solana_clock::{Epoch, Slot},
    solana_core::validator::ValidatorStartProgress,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_net_utils::SocketAddrSpace,
    solana_pubkey::Pubkey,
    solana_slot_history::Check,
    solana_time_utils::timestamp,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

const LEADER_TRAITS_EVERY: Duration = Duration::from_secs(10);

debounced_keys! {
    pub(super) struct ClusterKeys {
        identity_name: Option<String> = (TOPIC_SUMMARY, "identity_name"),
        identity_icon: Option<String> = (TOPIC_SUMMARY, "identity_icon"),
        identity_balance: u64 = (TOPIC_SUMMARY, "identity_balance"),
        vote_balance: u64 = (TOPIC_SUMMARY, "vote_balance"),
        vote_cost: VoteCost = (TOPIC_SUMMARY, "vote_cost"),
        bls_key: Option<bool> = (TOPIC_SUMMARY, "bls_key"),
        admission: Option<Admission> = (TOPIC_SUMMARY, "admission"),
        vote_credits: Option<VoteCredits> = (TOPIC_SUMMARY, "vote_credits"),
        vote_commission: Option<u8> = (TOPIC_SUMMARY, "vote_commission"),
        stake: StakeSummary = (TOPIC_SUMMARY, "stake"),
        vote_slot: Option<Slot> = (TOPIC_SUMMARY, "vote_slot"),
        consensus: Consensus = (TOPIC_SUMMARY, "consensus"),
        epoch_remaining_nanos: u64 = (TOPIC_SUMMARY, "epoch_remaining_nanos"),
        slot_duration_nanos: u64 = (TOPIC_SUMMARY, "estimated_slot_duration_nanos"),
        health: Health = (TOPIC_SUMMARY, "health"),
        validator_counts: ValidatorCounts = (TOPIC_SUMMARY, "validator_counts"),
        versions: Vec<VersionShare> = (TOPIC_SUMMARY, "versions"),
        client: String = (TOPIC_SUMMARY, "client"),
        skip_rate: SkipRate = (TOPIC_SUMMARY, "skip_rate"),
        leader_slot_counts: Vec<LeaderSlotCounts> = (TOPIC_SUMMARY, "leader_slot_counts"),
        epoch: EpochInfo = (TOPIC_EPOCH, "new"),
        peers: Vec<Peer> = (TOPIC_PEERS, "all"),
        gossip_stake: Option<GossipStake> = (TOPIC_SUMMARY, KEY_GOSSIP_STAKE),
        ;
        charts_made: u64 = 0,
        /// The epoch the previous epoch's skip share was read for, and the share.
        previous_skips: Option<(Epoch, Option<f64>)> = None,
        traits_at: Option<Instant> = None,
        voting: bool = false,
        last_vote_advance: Instant = Instant::now(),
        /// This epoch's walk and the previous one's.
        skip: (SkipRateWalk, SkipRateWalk) = Default::default(),
        epoch_published: Option<(Epoch, Pubkey, bool)> = None,
    }
}

impl SlotFeed {
    /// Each tick: our identity, vote account and epoch.
    pub(super) fn collect_vote(&mut self) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let (keys, publisher) = (&mut self.cluster, &self.publisher);
        if let Some(alpenglow) = remote.alpenglow {
            let consensus = if alpenglow {
                Consensus::Alpenglow
            } else {
                Consensus::Tower
            };
            keys.consensus.publish(publisher, consensus);
        }
        if let Some(params) = remote.slot_params(self.followed.completed) {
            keys.slot_duration_nanos.publish(publisher, params.nanos);
        }
        if let Some(identity) = remote.identity {
            let names = self.info_cache.read().unwrap();
            let info = names.get(&identity);
            keys.identity_name
                .publish(publisher, info.and_then(|info| info.name.clone()));
            keys.identity_icon
                .publish(publisher, info.and_then(|info| info.icon_url.clone()));
        }
        let (Some(vote), Some(accounts), Some(working), Some(identity)) = (
            &remote.vote,
            &remote.vote_accounts,
            remote.working,
            remote.identity,
        ) else {
            return;
        };
        keys.identity_balance
            .publish(publisher, vote.identity_balance);
        keys.vote_balance.publish(publisher, vote.vote_balance);

        let epoch = working.epoch;
        let ours = vote.vote_pubkey.map(|key| key.to_string());
        let row = accounts
            .rows
            .iter()
            .find(|row| Some(&row.vote_pubkey) == ours.as_ref());
        let view = row
            .and(vote.account.as_ref())
            .map(|account| account.vote_state_view());
        let pending = view.map_or(0, |view| view.pending_delegator_rewards());
        if let Some(cost) = vote_cost(&remote, working.slot, pending) {
            keys.vote_cost.publish(publisher, cost);
        }
        // Ours from the same read as everyone's, so the share never compares two moments.
        let credits = row.map_or(0, |row| credits_in(&row.epoch_credits, epoch));
        let staked: Vec<u64> = accounts
            .rows
            .iter()
            .filter(|row| row.activated_stake > 0)
            .map(|row| credits_in(&row.epoch_credits, epoch))
            .collect();
        let cluster_max = staked.iter().copied().max();
        let cluster_median = median_of(staked);
        keys.voting = row.is_some_and(|row| row.node_pubkey == identity.to_string());
        keys.bls_key.publish(
            publisher,
            view.map(|view| view.bls_pubkey_compressed().is_some()),
        );
        keys.vote_credits.publish(
            publisher,
            view.map(|_| VoteCredits {
                epoch,
                credits,
                cluster_max,
                cluster_median,
            }),
        );
        keys.vote_commission
            .publish(publisher, view.map(|view| view.commission()));
        let admission = vote.vote_pubkey.and_then(|vote_pubkey| {
            if remote.alpenglow != Some(true) {
                return None;
            }
            let seated = |epoch: Epoch| {
                remote
                    .rank_maps
                    .get(&epoch)
                    .map(|map| map.get_rank_for_vote_pubkey(&vote_pubkey).is_some())
            };
            // As the bank's VAT health for the next epoch.
            let minimum = remote
                .slot_params(working.slot)
                .zip(remote.vote_state_rent)
                .map(|(params, rent)| rent.saturating_add(params.vat_burn).saturating_add(pending));
            let ticket_short = view
                .filter(|view| view.bls_pubkey_compressed().is_some())
                .and(minimum)
                .filter(|minimum| vote.vote_balance < *minimum)
                .map(|minimum| minimum.saturating_sub(vote.vote_balance));
            Some(Admission {
                seat: seated(epoch)?,
                next_seat: seated(epoch.saturating_add(1)),
                ticket_short,
            })
        });
        keys.admission.publish(publisher, admission);
        let total_stake: u64 = accounts.rows.iter().map(|row| row.activated_stake).sum();
        let activated_stake = row
            .filter(|_| view.is_some())
            .map_or(0, |row| row.activated_stake);
        keys.stake.publish(
            publisher,
            StakeSummary {
                activated_stake,
                total_stake,
                share: if total_stake == 0 {
                    0.0
                } else {
                    activated_stake as f64 / total_stake as f64
                },
            },
        );
        let last_vote = view
            .filter(|_| keys.voting)
            .and_then(|view| view.last_voted_slot());
        if keys.vote_slot.last() != Some(&last_vote) {
            keys.last_vote_advance = Instant::now();
        }
        keys.vote_slot.publish(publisher, last_vote);
        self.collect_epoch_countdown(&remote, working);
        self.collect_epoch(&remote, working, identity);
    }

    fn collect_epoch(&mut self, remote: &Remote, working: WorkingReading, me: Pubkey) {
        let epoch = working.epoch;
        if self.cluster.epoch_published == Some((epoch, me, true)) {
            return;
        }
        let Some(current) = epoch_record(remote, epoch, working.slot) else {
            return;
        };
        let current = EpochInfo {
            my_leader_slots: leader_slots(remote, epoch, me).unwrap_or_default(),
            ..current
        };
        let known = !current.turns.is_empty();
        let previous = epoch
            .checked_sub(1)
            .and_then(|before| epoch_record(remote, before, working.slot))
            .filter(|record| !record.turns.is_empty());
        let mut archive = self.epochs.write().unwrap();
        archive.clear();
        archive.extend(previous);
        archive.push(current.clone());
        drop(archive);
        self.cluster.epoch.publish(&self.publisher, current);
        self.cluster.epoch_published = Some((epoch, me, known));
    }

    /// With a viewer, every slow tick.
    pub(super) fn collect_skip_rate(&mut self) {
        let remote = self.remote.clone();
        let mut remote = remote.write().unwrap();
        let (Some(schedule), Some(me)) = (remote.epoch_schedule.clone(), remote.identity) else {
            return;
        };
        let root = self.followed.root;
        let epoch = schedule.get_epoch(root);
        let previous = epoch.checked_sub(1);
        let (current, before) = &mut self.cluster.skip;
        if previous.is_some_and(|previous| current.epoch == Some((previous, me))) {
            *before = std::mem::take(current);
        }
        if current.epoch != Some((epoch, me)) {
            let Some(slots) = leader_slots(&remote, epoch, me) else {
                return;
            };
            *current = SkipRateWalk::new(epoch, me, slots);
        }
        if let Some(previous) = previous
            && before.epoch != Some((previous, me))
        {
            *before = leader_slots(&remote, previous, me)
                .map(|slots| SkipRateWalk::new(previous, me, slots))
                .unwrap_or_default();
        }
        // As far as the sysvar read last covers; the root moves on between reads.
        if let Some(history) = remote.slot_history.clone() {
            let covered = root.min(history.next_slot.saturating_sub(1));
            current.advance(covered, |slot| history.check(slot));
            before.advance(covered, |slot| history.check(slot));
        }
        if current.due(root) || before.due(root) {
            remote.slot_history_wanted = Some(remote.slot_history_wanted.unwrap_or(0).max(root));
        }
        let (rate, counts) = (
            current.rate(),
            [&*before, &*current]
                .into_iter()
                .filter_map(SkipRateWalk::counts)
                .collect(),
        );
        self.cluster
            .skip_rate
            .publish(&self.publisher, SkipRate { epoch, rate });
        self.cluster
            .leader_slot_counts
            .publish(&self.publisher, counts);
    }

    fn collect_epoch_countdown(&mut self, remote: &Remote, working: WorkingReading) {
        let completed = self.followed.completed.max(working.slot);
        let Some(params) = remote.slot_params(completed) else {
            return;
        };
        let now = SystemTime::now();
        let elapsed = remote
            .epoch_start
            .filter(|(epoch, _)| *epoch == working.epoch)
            .and_then(|(_, start)| {
                let now = i64::try_from(now.duration_since(UNIX_EPOCH).ok()?.as_secs()).ok()?;
                now.checked_sub(start)
            });
        let start_slot = working.epoch_start_slot;
        let end_slot = start_slot
            .saturating_add(working.slots_in_epoch)
            .saturating_sub(1);
        let slot_nanos =
            self.followed
                .clock
                .slot_nanos(elapsed, start_slot, completed, params.nanos);
        let ahead = Duration::from_nanos(
            end_slot
                .saturating_sub(completed)
                .saturating_mul(slot_nanos),
        );
        let Some(estimate) = now.checked_add(ahead) else {
            return;
        };
        let end = self.followed.clock.epoch_end(working.epoch, estimate, now);
        let remaining = end.duration_since(now).unwrap_or_default();
        self.cluster.epoch_remaining_nanos.publish(
            &self.publisher,
            remaining.as_secs().saturating_mul(1_000_000_000),
        );
    }

    /// With a viewer, every slow tick: what the gossip table and the vote accounts say of peers.
    pub(super) fn collect_peers(&mut self) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let (keys, publisher) = (&mut self.cluster, &self.publisher);
        let contacts = &remote.gossip;
        if let Some(contact) = remote.identity.and_then(|identity| contacts.get(&identity)) {
            keys.client.publish(publisher, contact.client.clone());
        }
        let completed = self.followed.completed;
        let vote_slot = keys.vote_slot.last().copied().flatten();
        keys.health.publish(
            publisher,
            assess_health(
                self.followed.completed_at.0.elapsed(),
                completed,
                keys.voting,
                vote_slot,
                vote_slot.map(|vote| completed.saturating_sub(vote)),
                keys.last_vote_advance.elapsed(),
            ),
        );
        let Some(accounts) = &remote.vote_accounts else {
            return;
        };
        let tally = tally(accounts);
        let space = SocketAddrSpace::new(remote.allow_private_addr);
        let rpc_nodes = contacts
            .iter()
            .filter(|(key, contact)| {
                Some(**key) != remote.identity && contact.rpc.is_some_and(|addr| space.check(&addr))
            })
            .count();
        let versions: HashMap<Pubkey, String> = contacts
            .iter()
            .filter(|(key, _)| tally.staked.contains_key(*key))
            .map(|(key, contact)| (*key, contact.version.clone()))
            .collect();
        keys.validator_counts.publish(
            publisher,
            ValidatorCounts {
                total: tally.staked.len(),
                delinquent: tally.delinquent.len(),
                rpc_nodes,
                non_delinquent_stake: tally.non_delinquent_stake,
                delinquent_stake: tally.delinquent_stake,
            },
        );
        keys.versions
            .publish(publisher, version_shares(&tally.staked, &versions));
    }

    /// Each tick, as the boot thread: while the validator waits for a supermajority, the stake it
    /// can see in gossip, a TVU peer in the table, and this node, counting as seen. RPC lists the
    /// table without its freshness, so a peer's contact is not checked for its age.
    pub(super) fn collect_gossip_stake(&mut self) {
        let remote = self.remote.read().unwrap();
        let contacts = &remote.gossip;
        let stake = match (remote.progress, &remote.vote_accounts, remote.identity) {
            (
                Some(ValidatorStartProgress::WaitingForSupermajority { slot, .. }),
                Some(accounts),
                Some(me),
            ) => contacts.get(&me).map(|ours| {
                let space = SocketAddrSpace::new(remote.allow_private_addr);
                let mut seen: HashMap<Pubkey, String> = contacts
                    .iter()
                    .filter(|(key, contact)| {
                        let tvu = contact.addresses.tvu_udp;
                        **key != me && tvu.is_some_and(|addr| space.check(&addr))
                    })
                    .map(|(key, contact)| (*key, contact.version.clone()))
                    .collect();
                seen.insert(me, ours.version.clone());
                tally_gossip_stake(
                    slot,
                    ours.shred_version,
                    tally(accounts).staked,
                    &seen,
                    &self.info_cache.read().unwrap(),
                )
            }),
            _ => None,
        };
        self.cluster.gossip_stake.publish(&self.publisher, stake);
    }

    /// With a viewer: the leaders of the strip's turns and of the next slots, with their stake and
    /// what they gossip.
    pub(super) fn collect_peer_table(&mut self) {
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let Some(accounts) = &remote.vote_accounts else {
            return;
        };
        let ahead = self.followed.highest;
        let mut leaders: HashSet<Pubkey> = (ahead.saturating_add(1)
            ..=ahead.saturating_add(UPCOMING_SLOTS))
            .map_while(|slot| remote.slot_leader(slot).copied())
            .collect();
        let highest = self.followed.completed;
        let first = highest.saturating_sub(SLOT_OVERVIEW_LEN as u64);
        let stride = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
        let mut slot = first.saturating_sub(first.checked_rem(stride).unwrap_or(0));
        while slot <= highest {
            leaders.extend(remote.slot_leader(slot).copied());
            slot = slot.saturating_add(stride);
        }
        let stakes = tally(accounts).staked;
        let contacts = &remote.gossip;
        let names = self.info_cache.read().unwrap();
        let mut peers: Vec<Peer> = leaders
            .into_iter()
            .map(|identity| {
                let contact = contacts.get(&identity);
                let info = names.get(&identity);
                Peer {
                    stake: stakes.get(&identity).copied().unwrap_or(0),
                    version: contact.map(|contact| contact.version.clone()),
                    client: contact.map(|contact| contact.client.clone()),
                    ip: contact.and_then(ip),
                    country: contact.and_then(Contact::country),
                    name: info.and_then(|info| info.name.clone()),
                    icon: info.and_then(|info| info.icon_url.clone()),
                    identity: identity.to_string(),
                }
            })
            .collect();
        drop(names);
        peers.sort_by(|a, b| a.identity.cmp(&b.identity));
        self.cluster.peers.publish(&self.publisher, peers);
    }

    /// While a page asks: at once after a lapse, then every fifteen seconds.
    /// The skip chart checks slots against the SlotHistory sysvar as RPC last read it.
    pub(super) fn collect_cluster_charts(&mut self) {
        let now_millis = timestamp();
        if !self.replies.cluster_charts.wanted(now_millis)
            || now_millis.saturating_sub(self.cluster.charts_made) < GATHER_EVERY_MILLIS
        {
            return;
        }
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let (Some(schedule), Some(accounts)) = (&remote.epoch_schedule, &remote.vote_accounts)
        else {
            return;
        };
        let root = self.followed.root;
        let epoch = schedule.get_epoch(root);
        let epochs = self.epochs.read().unwrap();
        let Some(info) = epochs
            .iter()
            .find(|info| info.epoch == epoch && !info.turns.is_empty())
        else {
            return;
        };
        self.cluster.charts_made = now_millis;
        let history = remote.slot_history.clone();
        let rooted = history.as_ref().map(|history| {
            move |slot: Slot| match slot < history.next_slot {
                true => history.check(slot),
                false => Check::Future,
            }
        });
        let previous = epoch.checked_sub(1).map(|previous| {
            (
                schedule.get_first_slot_in_epoch(previous),
                schedule.get_last_slot_in_epoch(previous),
            )
        });
        if self
            .cluster
            .previous_skips
            .is_none_or(|(held, _)| held != epoch)
            && let (Some(rooted), Some(history)) = (&rooted, &history)
            && previous.is_none_or(|(_, last)| last < history.next_slot)
        {
            let share = previous.and_then(|(first, last)| skip_share(first, last, rooted));
            self.cluster.previous_skips = Some((epoch, share));
        }
        let contacts = &remote.gossip;
        let described: Vec<(u64, Option<Described>)> = tally(accounts)
            .staked
            .iter()
            .map(|(key, stake)| {
                let described = contacts.get(key).map(|contact| Described {
                    client: contact.client.clone(),
                    release: strip_prerelease(&contact.version).to_string(),
                    country: contact.country(),
                });
                (*stake, described)
            })
            .collect();
        let charts = cluster_charts(
            info,
            &self.history.read().unwrap(),
            self.followed.newest_arrival,
            self.followed.caught_up_millis.unwrap_or(0),
            root,
            now_millis,
            self.cluster.previous_skips.and_then(|(_, share)| share),
            rooted
                .as_ref()
                .map(|rooted| rooted as &dyn Fn(Slot) -> Check),
            &self.info_cache.read().unwrap(),
            |key| remote.country(key),
            &described,
            remote.identity.and_then(|me| remote.country(&me)),
        );
        drop((epochs, remote));
        *self.replies.cluster_charts.held.write().unwrap() = Encoded(json_or_null(&charts));
        let mut remote = self.remote.write().unwrap();
        remote.slot_history_wanted = Some(remote.slot_history_wanted.unwrap_or(0).max(root));
    }

    /// With a viewer, every slow tick while asked for: the gossip table.
    pub(super) fn collect_gossip_peers(&mut self) {
        let now_millis = timestamp();
        if !self.replies.gossip_peers.wanted(now_millis) {
            return;
        }
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let Some(accounts) = &remote.vote_accounts else {
            return;
        };
        let node_of: HashMap<Pubkey, Pubkey> = accounts
            .rows
            .iter()
            .filter_map(|row| {
                Some((
                    Pubkey::from_str(&row.vote_pubkey).ok()?,
                    Pubkey::from_str(&row.node_pubkey).ok()?,
                ))
            })
            .collect();
        let by_node = |vote: &Pubkey| node_of.get(vote).copied();
        let finalization: HashMap<Pubkey, f64> = self
            .certificates
            .finalization
            .shares(now_millis)
            .into_iter()
            .filter_map(|(vote, share)| Some((by_node(&vote)?, share)))
            .collect();
        let (arrival_epoch, arrivals) = self.tap.vote_arrivals();
        let arrivals = arrivals
            .into_iter()
            .filter_map(|(vote, arrival)| Some((by_node(&vote)?, arrival)))
            .collect();
        let gossiped = remote
            .gossip
            .iter()
            .map(|(key, contact)| Gossiped {
                identity: *key,
                client: (contact.client.clone(), contact.version.clone()),
                gossip: contact.addresses.gossip.map(|address| address.ip()),
                rpc: contact.rpc.map(|address| address.port()),
            })
            .collect();
        let list = gossip_peers(
            self.followed.root,
            accounts.slot,
            &tally(accounts),
            gossiped,
            &finalization,
            (arrival_epoch, &arrivals),
            &self.info_cache.read().unwrap(),
        );
        *self.replies.gossip_peers.held.write().unwrap() = Encoded(json_or_null(&list));
    }

    /// With a viewer, every ten seconds: what the server answers of every staked node and every
    /// held epoch's leaders.
    pub(super) fn collect_leader_traits(&mut self) {
        if self
            .cluster
            .traits_at
            .is_some_and(|at| at.elapsed() < LEADER_TRAITS_EVERY)
        {
            return;
        }
        let remote = self.remote.clone();
        let remote = remote.read().unwrap();
        let Some(accounts) = &remote.vote_accounts else {
            return;
        };
        self.cluster.traits_at = Some(Instant::now());
        let tally = tally(accounts);
        let mut map: LeaderMap = tally
            .staked
            .iter()
            .map(|(identity, stake)| {
                let last_vote = tally.stalest_votes.get(identity).copied().flatten();
                let traits = LeaderTraits {
                    stake: *stake,
                    last_vote,
                    delinquent: is_delinquent(last_vote, accounts.slot),
                    ..LeaderTraits::default()
                };
                (*identity, traits)
            })
            .collect();
        let scheduled: HashSet<Pubkey> = self
            .epochs
            .read()
            .unwrap()
            .iter()
            .flat_map(|epoch| &epoch.leaders)
            .filter_map(|key| key.parse().ok())
            .collect();
        for (identity, contact) in &remote.gossip {
            if map.contains_key(identity) || scheduled.contains(identity) {
                let traits = map.entry(*identity).or_default();
                traits.client = Some(contact.client.clone());
                traits.version = Some(contact.version.clone());
                traits.ip = ip(contact);
                traits.country = contact.country();
            }
        }
        *self.replies.leader_traits.write().unwrap() = map;
    }
}

/// The staked vote accounts by identity, against the slot they were read at.
pub(super) fn tally(accounts: &VoteAccounts) -> StakeTally {
    tally_stake(
        accounts.rows.iter().filter_map(|row| {
            let identity = Pubkey::from_str(&row.node_pubkey).ok()?;
            let last_vote = (row.last_vote > 0).then_some(row.last_vote);
            Some((identity, row.activated_stake, last_vote))
        }),
        accounts.slot,
    )
}

fn ip(contact: &Contact) -> Option<String> {
    Some(contact.addresses.gossip?.ip().to_string())
}

/// With no leader slots of ours, and no turns where the schedule is not known.
fn epoch_record(remote: &Remote, epoch: Epoch, working: Slot) -> Option<EpochInfo> {
    let schedule = remote.epoch_schedule.as_ref()?;
    let params = remote.slot_params(working)?;
    let start_slot = schedule.get_first_slot_in_epoch(epoch);
    let slots_in_epoch = schedule.get_slots_in_epoch(epoch);
    let (leaders, turns) = remote
        .schedules
        .get(&epoch)
        .filter(|leaders| leaders.by_slot.len() as u64 == slots_in_epoch)
        .map(|leaders| {
            (
                leaders.leaders.iter().map(Pubkey::to_string).collect(),
                leaders
                    .by_slot
                    .iter()
                    .step_by(NUM_CONSECUTIVE_LEADER_SLOTS.get())
                    .copied()
                    .collect(),
            )
        })
        .unwrap_or_default();
    Some(EpochInfo {
        epoch,
        start_slot,
        end_slot: schedule.get_last_slot_in_epoch(epoch),
        slots_in_epoch,
        my_leader_slots: Vec::new(),
        leaders,
        turns,
        block_cost_limit: params.block_limit,
        account_cost_limit: params.account_limit,
    })
}

/// Our slots of `epoch`, where its schedule is known.
pub(super) fn leader_slots(remote: &Remote, epoch: Epoch, me: Pubkey) -> Option<Vec<Slot>> {
    let leaders = remote.schedules.get(&epoch)?;
    Some(
        (leaders.first..)
            .zip(&leaders.by_slot)
            .filter(|(_, index)| leaders.leaders.get(usize::from(**index)) == Some(&me))
            .map(|(slot, _)| slot)
            .collect(),
    )
}

/// The credits of `epoch`, from the newest entry of a vote account's history.
fn credits_in(history: &[(Epoch, u64, u64)], epoch: Epoch) -> u64 {
    history
        .last()
        .filter(|(at, ..)| *at == epoch)
        .map_or(0, |(_, credits, previous)| {
            credits.saturating_sub(*previous)
        })
}

fn vote_cost(remote: &Remote, slot: Slot, pending: u64) -> Option<VoteCost> {
    let params = remote.slot_params(slot)?;
    if remote.alpenglow? {
        return Some(VoteCost::Ticket {
            lamports: params.vat_burn,
            minimum: remote.vote_state_rent?.saturating_add(params.vat_burn),
            pending,
        });
    }
    let slots_per_day = NANOS_PER_DAY.checked_div(u128::from(params.nanos.max(1)))?;
    Some(VoteCost::Fees {
        per_day: u64::try_from(slots_per_day)
            .unwrap_or(u64::MAX)
            .saturating_mul(remote.lamports_per_signature?),
    })
}
