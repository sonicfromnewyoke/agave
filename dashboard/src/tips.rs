//! Jito tips, read as the movement of the eight tip payment accounts between
//! a frozen bank and its parent. Only the measured figure is kept; shares are
//! derived where drawn.

use {serde::Serialize, solana_pubkey::Pubkey};

pub const TIP_ACCOUNTS: usize = 8;

/// In basis points; one approximation applied to every leader.
pub const JITO_CUT_BPS: u16 = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TipRates {
    pub jito_cut_bps: u16,
    pub commission_bps: Option<u16>,
}

pub struct TipMeter {
    /// Derived from the program id, which differs between clusters.
    accounts: [Pubkey; TIP_ACCOUNTS],
    /// Starts high and converges at the first crank, so the first turn after a restart reads low.
    floor: u64,
}

impl TipMeter {
    pub fn new(program_id: &Pubkey) -> Self {
        Self {
            accounts: std::array::from_fn(|at| {
                let seed = format!("TIP_ACCOUNT_{at}");
                Pubkey::find_program_address(&[seed.as_bytes()], program_id).0
            }),
            floor: u64::MAX,
        }
    }

    pub fn accounts(&self) -> &[Pubkey] {
        &self.accounts
    }

    /// A bank's tips from its tip accounts' total and its parent's.
    pub fn measure_total(&mut self, now: u64, before: u64) -> u64 {
        self.floor = self.floor.min(now);
        // A fall means the receiver was cranked in this slot, so only what stands above the floor
        // arrived after it.
        now.saturating_sub(if now >= before { before } else { self.floor })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_the_eight_accounts_are_derived_and_distinct() {
        let meter = TipMeter::new(&Pubkey::new_unique());
        let accounts = meter.accounts();
        assert_eq!(accounts.len(), TIP_ACCOUNTS);
        for (index, account) in accounts.iter().enumerate() {
            assert!(
                !accounts[..index].contains(account),
                "account {index} repeats an earlier one"
            );
        }
    }

    #[test]
    fn test_a_different_cluster_derives_different_accounts() {
        let one = TipMeter::new(&Pubkey::new_unique());
        let other = TipMeter::new(&Pubkey::new_unique());
        assert_ne!(one.accounts(), other.accounts());
    }
}
