//! Every node in the gossip table with what it advertises about itself, gathered only while a
//! client is asking for it.

use {
    super::{StakeTally, is_delinquent},
    crate::{geoip, metrics_tap::VoteArrival, validator_info::ValidatorInfoCache},
    indexmap::IndexSet,
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    std::{collections::HashMap, net::IpAddr},
};

/// Notarize mean, its spread and its worst, then the finalize mean, in microseconds; then the
/// notarize votes the mean is over.
pub type VoteArrivalCell = (u64, Option<u64>, Option<u64>, Option<u64>, Option<u64>);

/// One array per column, so several thousand rows stay compact.
#[derive(Debug, Default, Serialize)]
pub struct GossipPeers {
    /// Our root, which the snapshot and lowest slots are read against.
    pub root: Slot,
    /// Active stake in lamports, of which `stake` is each node's share.
    pub total_stake: u64,
    /// `(client, version)` pairs, indexed by `client`.
    pub clients: Vec<(String, String)>,
    pub identity: Vec<String>,
    pub name: Vec<Option<String>>,
    /// In lamports.
    pub stake: Vec<u64>,
    pub client: Vec<usize>,
    pub ip: Vec<Option<String>>,
    /// Two-letter country code of `ip`.
    pub country: Vec<Option<&'static str>>,
    pub rpc: Vec<Option<u16>>,
    /// Under alpenglow, the share of recent blocks whose finalization certificate carried its vote.
    pub finalization: Vec<Option<f32>>,
    /// The epoch `vote_arrival` describes; none until votor has reported one.
    pub vote_arrival_epoch: Option<Epoch>,
    /// Under alpenglow, from a slot starting here to the peer's votes arriving.
    pub vote_arrival: Vec<Option<VoteArrivalCell>>,
    /// Staked and not voting: never, or too far behind the tip.
    pub delinquent: Vec<bool>,
    /// The oldest last vote among a staked node's vote accounts.
    pub last_vote: Vec<Option<Slot>>,
}

/// What a peer gossips about itself.
pub struct Gossiped {
    pub identity: Pubkey,
    /// Its client and version.
    pub client: (String, String),
    pub gossip: Option<IpAddr>,
    pub rpc: Option<u16>,
}

/// The table, from any source of the gossip table, the stakes and the votes; `finalization` and
/// `arrivals` by node, `tip` the slot the votes are judged against.
pub fn gossip_peers(
    root: Slot,
    tip: Slot,
    tally: &StakeTally,
    mut peers: Vec<Gossiped>,
    finalization: &HashMap<Pubkey, f64>,
    (arrival_epoch, arrivals): (Option<Epoch>, &HashMap<Pubkey, VoteArrival>),
    info: &ValidatorInfoCache,
) -> GossipPeers {
    let stake_of = |key: &Pubkey| tally.staked.get(key).copied().unwrap_or(0);
    peers.sort_by(|a, b| {
        stake_of(&b.identity)
            .cmp(&stake_of(&a.identity))
            .then_with(|| a.identity.cmp(&b.identity))
    });
    let mut list = GossipPeers {
        root,
        total_stake: tally
            .delinquent_stake
            .saturating_add(tally.non_delinquent_stake),
        vote_arrival_epoch: arrival_epoch,
        ..GossipPeers::default()
    };
    let mut clients = IndexSet::new();
    for peer in peers {
        let key = peer.identity;
        let (index, _) = clients.insert_full(peer.client);
        list.identity.push(key.to_string());
        list.name
            .push(info.get(&key).and_then(|info| info.name.clone()));
        list.stake.push(stake_of(&key));
        list.client.push(index);
        list.ip.push(peer.gossip.map(|ip| ip.to_string()));
        list.country.push(peer.gossip.and_then(geoip::country));
        list.rpc.push(peer.rpc);
        list.finalization
            .push(finalization.get(&key).map(|share| *share as f32));
        list.vote_arrival.push(arrivals.get(&key).map(|arrival| {
            (
                arrival.notarize_mean_micros,
                arrival.notarize_spread_micros,
                arrival.notarize_worst_micros,
                arrival.finalize_mean_micros,
                arrival.notarize_count,
            )
        }));
        let last_vote = tally.stalest_votes.get(&key).copied();
        list.delinquent
            .push(last_vote.is_some_and(|vote| is_delinquent(vote, tip)));
        list.last_vote.push(last_vote.flatten());
    }
    list.clients = clients.into_iter().collect();
    list
}
