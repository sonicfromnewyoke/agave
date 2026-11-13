//! What the slot feed is told of the validator, in the order it happened: each slot's first shred,
//! bank, block, footer, confirmation and root, read off the ledger, and the tip accounts' balances,
//! read off RPC.

use {
    crate::certs::FooterView,
    crossbeam_channel::Sender,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Stamped by the ledger on the validator host's clock, in unix milliseconds.
    FirstShredReceived {
        at_millis: u64,
    },
    CreatedBank,
    Confirmed,
    Rooted,
}

/// A frozen bank's block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenBlock {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub blockhash: String,
    /// Every transaction the block holds, failed ones included.
    pub executed_transactions: u64,
    /// The block's entries that hold transactions.
    pub entries: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    /// The validator whose ledger is followed. Another one starts the slot strip again.
    Validator {
        pid: u32,
    },
    Slot {
        slot: Slot,
        parent: Option<Slot>,
        status: Status,
    },
    Block(FrozenBlock),
    Footer {
        slot: Slot,
        footer: FooterView,
    },
    /// A tip account's balance as of a bank.
    Account {
        slot: Slot,
        pubkey: Pubkey,
        lamports: u64,
    },
}

/// Where the frames are sent from.
#[derive(Clone)]
pub struct Frames {
    pub sender: Sender<Frame>,
    /// Counted for the slot feed, which logs what went missing and checks the slots it may have
    /// carried.
    pub dropped: Arc<AtomicU64>,
}

impl Frames {
    pub fn send(&self, frame: Frame) {
        if self.sender.try_send(frame).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
