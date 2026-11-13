//! What the dashboard has learned about the validator, shared between the pollers that
//! learn it and the meters and feeds that read it.

use {
    crate::{
        admin::AdminContactInfo,
        certs::Span,
        geoip,
        meter_source::{IngestAddresses, WorkingReading},
    },
    agave_feature_set::raise_block_limits_to_100m,
    indexmap::IndexSet,
    solana_clock::{Epoch, Slot, UnixTimestamp},
    solana_core::validator::ValidatorStartProgress,
    solana_cost_model::block_cost_limits::{MAX_BLOCK_UNITS, MAX_WRITABLE_ACCOUNT_UNITS},
    solana_epoch_schedule::EpochSchedule,
    solana_pubkey::Pubkey,
    solana_rpc_client_api::response::{RpcVoteAccountInfo, SlotTransactionStats},
    solana_runtime::{
        bank::DEFAULT_VAT_TO_BURN_PER_EPOCH, epoch_stakes::BLSPubkeyToRankMap,
        slot_params::slot_time_feature_gates,
    },
    solana_slot_history::SlotHistory as SlotHistorySysvar,
    solana_vote::vote_account::VoteAccount,
    std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
        str::FromStr,
        sync::{Arc, RwLock},
        time::SystemTime,
    },
};

#[derive(Debug, Default)]
pub struct Remote {
    /// The validator's process, from its admin socket; kept after the socket goes quiet, until
    /// another validator answers.
    pub pid: Option<u32>,
    pub started: Option<SystemTime>,
    pub progress: Option<ValidatorStartProgress>,
    /// Answered once startup is complete.
    pub contact: Option<AdminContactInfo>,
    pub rpc_address: Option<SocketAddr>,
    /// The newest frozen block, as the slot feed read it off the ledger.
    pub working: Option<WorkingReading>,
    /// Blocks the validator is behind the cluster's tip, which a snapshot write is marked with.
    pub behind_cluster: Option<u64>,
    /// The ledger's newest root.
    pub finalized: Option<Slot>,
    pub identity: Option<Pubkey>,
    pub epoch_schedule: Option<EpochSchedule>,
    /// Each slot's leader, by epoch, for the epochs whose schedule is known.
    pub schedules: BTreeMap<Epoch, Arc<EpochLeaders>>,
    /// Asked by the slot collector while frames that went missing leave some slots' fate open: the
    /// newest of them, which the SlotHistory sysvar read must cover.
    pub slot_history_wanted: Option<Slot>,
    /// The SlotHistory sysvar as RPC last gave it, from the newest bank.
    pub slot_history: Option<Arc<SlotHistorySysvar>>,
    /// Our accounts as RPC's processed bank holds them.
    pub vote: Option<VoteReading>,
    pub vote_accounts: Option<Arc<VoteAccounts>>,
    pub alpenglow: Option<bool>,
    /// The cluster clock's stamp of an epoch's first slot, read once each epoch.
    pub epoch_start: Option<(Epoch, UnixTimestamp)>,
    pub vote_state_rent: Option<u64>,
    pub lamports_per_signature: Option<u64>,
    /// Slot time from the ledger's genesis.
    pub genesis_nanos: Option<u64>,
    /// Every activated feature's activation slot.
    pub features: HashMap<Pubkey, Slot>,
    /// As given to the validator.
    pub allow_private_addr: bool,
    /// Our own contact's, as RPC lists it.
    pub ingest: Option<IngestAddresses>,
    /// The gossip table, as RPC last listed it, by identity.
    pub gossip: HashMap<Pubkey, Contact>,
    /// Alpenglow's rank maps, by epoch: the vote accounts with stake and a BLS key, as read at
    /// the start of the epoch before (where the bank takes its epoch stakes from), or later.
    pub rank_maps: BTreeMap<Epoch, Arc<BLSPubkeyToRankMap>>,
    /// The snapshot writes the certificate tally lays misses against.
    pub snapshot_spans: Vec<Span>,
    /// Alpenglow's vote reward account, which holds each epoch's inflation.
    pub vote_reward: Option<Vec<u8>>,
    /// Recent frozen banks' own counts, from RPC's slot updates.
    pub frozen: BTreeMap<Slot, SlotTransactionStats>,
    /// Jito's tip accounts, where the dashboard is told the tip payment program.
    pub tip_accounts: Vec<Pubkey>,
    /// Their balances at a confirmed bank, read off RPC once its subscriptions to them run: tips
    /// are counted from there by the changes the subscriptions report.
    pub tip_base: Option<(Slot, Vec<u64>)>,
}

/// A node as RPC lists it from the gossip table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Contact {
    pub shred_version: u16,
    pub version: String,
    pub client: String,
    pub addresses: IngestAddresses,
    pub rpc: Option<SocketAddr>,
}

impl Contact {
    /// The country of its gossip address.
    pub fn country(&self) -> Option<&'static str> {
        geoip::country(self.addresses.gossip?.ip())
    }
}

#[derive(Debug, Clone)]
pub struct VoteReading {
    /// The ledger's root as the vote account was read.
    pub root: Slot,
    pub identity_balance: u64,
    pub vote_balance: u64,
    pub vote_pubkey: Option<Pubkey>,
    pub account: Option<VoteAccount>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotParams {
    pub nanos: u64,
    pub vat_burn: u64,
    pub block_limit: u64,
    pub account_limit: u64,
}

impl Remote {
    /// As the bank derives them (solana_runtime::slot_params): the genesis slot time, then each
    /// slot-time feature from the epoch after its activation, with the cost limits scaled to it.
    pub fn slot_params(&self, slot: Slot) -> Option<SlotParams> {
        let genesis = self.genesis_nanos?;
        let schedule = self.epoch_schedule.as_ref()?;
        let mut earliest = Slot::MAX;
        let mut transitions = Vec::new();
        for (feature, params) in slot_time_feature_gates().into_iter().rev() {
            let nanos = u64::try_from(params.ns_per_slot()).ok()?;
            let Some(activated) = self.features.get(&feature) else {
                continue;
            };
            let effective =
                schedule.get_first_slot_in_epoch(schedule.get_epoch(*activated).saturating_add(1));
            if nanos <= genesis && effective < earliest {
                transitions.push((effective, nanos, params.vat_to_burn_per_epoch()));
                earliest = effective;
            }
        }
        let (nanos, vat_burn, scale) = transitions
            .into_iter()
            .filter(|(effective, ..)| *effective <= slot)
            .max_by_key(|(effective, ..)| *effective)
            .map_or(
                (genesis, DEFAULT_VAT_TO_BURN_PER_EPOCH, LEGACY_NANOS),
                |(_, nanos, burn)| (nanos, burn, nanos),
            );
        let raised = self
            .features
            .get(&raise_block_limits_to_100m::id())
            .is_some_and(|activated| *activated <= slot);
        let limit = |units: u64| {
            let scaled = units.saturating_mul(scale).checked_div(LEGACY_NANOS)?;
            if raised {
                scaled.saturating_mul(100).checked_div(60)
            } else {
                Some(scaled)
            }
        };
        Some(SlotParams {
            nanos,
            vat_burn,
            block_limit: limit(MAX_BLOCK_UNITS)?,
            account_limit: limit(MAX_WRITABLE_ACCOUNT_UNITS)?,
        })
    }

    /// The leader of `slot`, where its epoch's schedule is known.
    pub fn slot_leader(&self, slot: Slot) -> Option<&Pubkey> {
        let epoch = self.epoch_schedule.as_ref()?.get_epoch(slot);
        self.schedules.get(&epoch)?.leader(slot)
    }

    /// Whether `slot` is ours, where its epoch's schedule and our identity are known.
    pub fn is_ours(&self, slot: Slot) -> Option<bool> {
        let identity = self.identity.as_ref()?;
        Some(self.slot_leader(slot)? == identity)
    }

    /// Our own contact's, else the admin socket's, which lacks the QUIC ports but one.
    pub fn ingest_addresses(&self) -> Option<IngestAddresses> {
        self.ingest.or_else(|| {
            self.contact
                .as_ref()
                .map(AdminContactInfo::ingest_addresses)
        })
    }

    /// The country of `key`'s gossip address.
    pub fn country(&self, key: &Pubkey) -> Option<&'static str> {
        self.gossip.get(key)?.country()
    }
}

/// Every vote account, current and delinquent, at `slot`.
#[derive(Debug, Default)]
pub struct VoteAccounts {
    pub slot: Slot,
    pub rows: Vec<RpcVoteAccountInfo>,
}

/// One epoch's leader schedule, as getLeaderSchedule gives it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochLeaders {
    pub first: Slot,
    /// In order of first slot.
    pub leaders: Vec<Pubkey>,
    /// Each slot's leader from `first`, as an index into `leaders`.
    pub by_slot: Vec<u16>,
}

impl EpochLeaders {
    /// From each leader's slot indices in the epoch; `None` unless every slot of the epoch has
    /// exactly one leader.
    pub fn from_indices(
        first: Slot,
        slots_in_epoch: u64,
        schedule: &HashMap<String, Vec<usize>>,
    ) -> Option<Self> {
        let length = usize::try_from(slots_in_epoch).ok()?;
        let mut owner: Vec<Option<Pubkey>> = vec![None; length];
        for (key, indices) in schedule {
            let key = Pubkey::from_str(key).ok()?;
            for index in indices {
                let slot = owner.get_mut(*index)?;
                if slot.replace(key).is_some() {
                    return None;
                }
            }
        }
        let mut leaders = IndexSet::new();
        let by_slot = owner
            .into_iter()
            .map(|key| u16::try_from(leaders.insert_full(key?).0).ok())
            .collect::<Option<_>>()?;
        Some(Self {
            first,
            leaders: leaders.into_iter().collect(),
            by_slot,
        })
    }

    pub fn leader(&self, slot: Slot) -> Option<&Pubkey> {
        let offset = usize::try_from(slot.checked_sub(self.first)?).ok()?;
        let index = *self.by_slot.get(offset)?;
        self.leaders.get(usize::from(index))
    }

    /// An epoch of `slots` slots from slot 0, each led by `leader(slot)`.
    #[cfg(test)]
    pub fn for_tests(slots: u64, leader: impl Fn(Slot) -> Pubkey) -> Self {
        let mut indices: HashMap<String, Vec<usize>> = HashMap::new();
        for slot in 0..slots {
            indices
                .entry(leader(slot).to_string())
                .or_default()
                .push(usize::try_from(slot).unwrap());
        }
        Self::from_indices(0, slots, &indices).unwrap()
    }
}

#[cfg(test)]
pub const US: Pubkey = Pubkey::new_from_array([1; 32]);
#[cfg(test)]
pub const THEM: Pubkey = Pubkey::new_from_array([2; 32]);

/// The slot time the legacy slot params are set for.
const LEGACY_NANOS: u64 = 400_000_000;

pub type Shared = Arc<RwLock<Remote>>;

#[cfg(test)]
mod tests {
    use {super::*, agave_feature_set::reduce_slot_time_to_200ms};

    #[test]
    fn test_slot_params_follow_the_slot_time_features_from_the_next_epoch() {
        let schedule = EpochSchedule::without_warmup();
        let next_epoch = schedule.get_first_slot_in_epoch(1);
        let mut remote = Remote {
            genesis_nanos: Some(400_000_000),
            epoch_schedule: Some(schedule),
            features: HashMap::from([(reduce_slot_time_to_200ms::id(), 5)]),
            ..Remote::default()
        };
        let legacy = remote.slot_params(next_epoch - 1).unwrap();
        assert_eq!(
            (
                legacy.nanos,
                legacy.vat_burn,
                legacy.block_limit,
                legacy.account_limit
            ),
            (400_000_000, 1_600_000_000, 60_000_000, 24_000_000)
        );
        let reduced = remote.slot_params(next_epoch).unwrap();
        assert_eq!(
            (
                reduced.nanos,
                reduced.vat_burn,
                reduced.block_limit,
                reduced.account_limit
            ),
            (200_000_000, 800_000_000, 30_000_000, 12_000_000)
        );
        remote
            .features
            .insert(raise_block_limits_to_100m::id(), next_epoch);
        let raised = remote.slot_params(next_epoch).unwrap();
        assert_eq!(
            (raised.block_limit, raised.account_limit),
            (50_000_000, 20_000_000)
        );
    }
}
