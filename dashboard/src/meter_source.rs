//! What the meters read from the validator: the newest frozen block's counters, where it keeps its
//! data and the addresses it receives on.

use {
    agave_snapshots::snapshot_config::SnapshotConfig,
    solana_clock::{Epoch, Slot},
    std::{net::SocketAddr, path::PathBuf},
};

/// The newest frozen block's place in the epoch and the transactions along its chain, as the
/// ledger holds them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkingReading {
    pub slot: Slot,
    /// The transactions of its block and of those before it on its chain, from the first block
    /// read.
    pub transaction_count: u64,
    /// The non-vote ones: all under alpenglow, where votes are no transactions; under tower none
    /// are counted.
    pub non_vote_transaction_count: u64,
    pub epoch: Epoch,
    pub epoch_start_slot: Slot,
    pub slots_in_epoch: u64,
    pub alpenglow: bool,
}

/// Where the validator keeps its ledger, accounts and snapshot archives.
#[derive(Clone, Debug)]
pub struct HostPaths {
    pub ledger: PathBuf,
    pub accounts: Vec<PathBuf>,
    pub snapshot_config: SnapshotConfig,
}

/// The addresses the validator receives on, as its contact info advertises them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestAddresses {
    pub tvu_udp: Option<SocketAddr>,
    pub tpu_quic: Option<SocketAddr>,
    pub tpu_forwards_quic: Option<SocketAddr>,
    pub tpu_vote_udp: Option<SocketAddr>,
    pub gossip: Option<SocketAddr>,
    pub serve_repair_udp: Option<SocketAddr>,
}
