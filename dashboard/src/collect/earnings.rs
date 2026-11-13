//! This epoch's inflation reward to our vote account, split into voting and the producer bonus our
//! blocks earn for the votes their reward certificates carry.

use {
    crate::certs,
    serde::{Deserialize, Serialize},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
};

/// Where the runtime keeps each epoch's inflation: `vote_reward_account` under the alpenglow
/// feature id.
pub const VOTE_REWARD_ACCOUNT: Pubkey =
    Pubkey::from_str_const("3vJzniWALu2qGFJ1ZY6JD3Y2ZmA5zWqmVdDX7yBXHgu7");

pub const BLOCKS_PER_TICK: usize = 64;

/// Lamports before commission. The maxima are for the whole epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EpochEarnings {
    pub epoch: Epoch,
    pub voting: u64,
    pub voting_lost: u64,
    pub voting_max: u64,
    pub bonus: u64,
    pub bonus_lost: u64,
    pub bonus_max: u64,
    pub commission_bps: u16,
    /// Our blocks the ledger no longer holds; their bonus counts as voting.
    pub unread: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct Inflation {
    pub reward: u64,
    pub slots: u64,
    pub epoch: Epoch,
}

/// The account holds the current epoch's three words, then an optional previous epoch's.
pub fn read_inflation(mut data: &[u8], epoch: Epoch) -> Option<Inflation> {
    let current: Inflation = bincode::deserialize_from(&mut data).ok()?;
    if current.epoch == epoch {
        return Some(current);
    }
    let previous: Option<Inflation> = bincode::deserialize_from(data).ok()?;
    previous.filter(|previous| previous.epoch == epoch)
}

/// The runtime's reward for one slot's vote by `stake`, before the voter and leader halves.
pub fn slot_reward(inflation: &Inflation, total: u64, stake: u64) -> u64 {
    let numerator = u128::from(inflation.reward).saturating_mul(u128::from(stake));
    let denominator = u128::from(inflation.slots).saturating_mul(u128::from(total));
    numerator
        .checked_div(denominator)
        .and_then(|reward| u64::try_from(reward).ok())
        .unwrap_or(0)
}

pub fn voter_half(reward: u64) -> u64 {
    reward / 2
}

fn leader_half(reward: u64) -> u64 {
    reward.saturating_sub(voter_half(reward))
}

/// What a certificate paid its leader, and what one carrying every rank would have.
pub fn block_bonus(
    stakes: impl Iterator<Item = (u64, bool)>,
    inflation: &Inflation,
    total: u64,
) -> (u64, u64) {
    stakes.fold((0u64, 0u64), |(got, full), (stake, paid)| {
        let half = leader_half(slot_reward(inflation, total, stake));
        let got = if paid { got.saturating_add(half) } else { got };
        (got, full.saturating_add(half))
    })
}

pub fn rank_stakes(map: &BLSPubkeyToRankMap) -> impl Iterator<Item = u64> + '_ {
    certs::rank_entries(map).map(|entry| entry.stake.get())
}

/// Ranks in either reward certificate; `None` where one does not decode.
pub fn paid_ranks(footer: &certs::FooterView, len: usize) -> Option<certs::Ranks> {
    let (notar, skip) = certs::decode_both(footer.notar.as_deref(), footer.skip.as_deref(), len)?;
    Some(notar | skip)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The account as mainnet held it in epoch 1079.
    const MAINNET: &str = "a7c2e02f9c3800008097060000000000370400000000000001fbf0833fa138000080970600000000003604000000000000";

    #[test]
    fn test_the_address_is_the_runtimes() {
        let (address, _) = Pubkey::find_program_address(
            &[b"vote_reward_account"],
            &agave_feature_set::alpenglow::id(),
        );
        assert_eq!(address, VOTE_REWARD_ACCOUNT);
    }

    #[test]
    fn test_reads_either_epoch_and_no_other() {
        let data = hex::decode(MAINNET).unwrap();
        let current = read_inflation(&data, 1079).unwrap();
        assert_eq!(current.reward, 62_243_469_312_679);
        assert_eq!(current.slots, 432_000);
        let previous = read_inflation(&data, 1078).unwrap();
        assert_eq!(previous.reward, 62_265_206_501_627);
        assert_eq!(read_inflation(&data, 1077), None);
        assert_eq!(read_inflation(&data[..25], 1078), None);
        assert_eq!(read_inflation(&data[..20], 1079), None);
    }

    #[test]
    fn test_the_leader_takes_the_odd_lamport() {
        assert_eq!(voter_half(7), 3);
        assert_eq!(leader_half(7), 4);
        assert_eq!(leader_half(0), 0);
    }

    #[test]
    fn test_slot_reward_matches_the_runtime_formula() {
        let inflation = Inflation {
            reward: 62_243_469_312_679,
            slots: 432_000,
            epoch: 1079,
        };
        let total = 434_917_723_208_692_500;
        // 280,042 SOL of the 434.9M staked, at epoch 1079's inflation.
        assert_eq!(slot_reward(&inflation, total, 280_041_995_582_784), 92_773);
        assert_eq!(slot_reward(&inflation, 0, 1), 0);
    }
    #[test]
    fn test_block_bonus_counts_only_paid_ranks() {
        let inflation = Inflation {
            reward: 1_000,
            slots: 10,
            epoch: 0,
        };
        // Each rank's slot reward is its stake share of 100: 50, 30 and 20, leader halves 25, 15, 10.
        let stakes = [(50, true), (30, false), (20, true)];
        assert_eq!(block_bonus(stakes.into_iter(), &inflation, 100), (35, 50));
        assert_eq!(block_bonus(std::iter::empty(), &inflation, 100), (0, 0));
    }
}
