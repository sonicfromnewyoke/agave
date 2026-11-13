//! Fee payers whose transactions fail in our blocks, against the same payers in other leaders'
//! blocks, for this epoch and the last.

use {
    crate::{
        produced::{BlockFees, TopFeePayer, TopFeeTransaction},
        versions::is_simple_vote,
    },
    agave_feature_set::FeatureSet,
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_compute_budget_instruction::instructions_processor::process_compute_budget_instructions,
    solana_entry::entry::Entry,
    solana_fee::{SignatureCounts, calculate_signature_fee},
    solana_message::VersionedMessage,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::signature_details::get_precompile_signature_details,
    solana_svm_transaction::instruction::SVMInstruction,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, HashMap},
    },
};

/// Flagged from this many transactions in our blocks in an epoch.
const MIN_TRANSACTIONS: u64 = 100;
const MIN_FAILED_PERCENT: u64 = 10;
/// 0.05 SOL of priority fees on failed transactions.
const MIN_PRIORITY_FAILED: u64 = 50_000_000;
/// Escalated above this many times its rate in other leaders' blocks.
const ESCALATE_RATIO: u128 = 3;
/// The rate elsewhere counts from this many transactions there.
const MIN_AWAY_TRANSACTIONS: u64 = 100;

/// Payers followed per epoch; past this, new payers count only in the epoch's totals.
const MAX_PAYERS: usize = 100_000;

const TOP_PAYERS: usize = 10;
const MAX_ALERTS: usize = 20;
/// Payers and transactions listed for each of our blocks.
const TOP_IN_BLOCK: usize = 5;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Tally {
    transactions: u64,
    failed: u64,
    /// Lamports.
    priority_failed: u64,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.failed = self.failed.saturating_add(other.failed);
        self.priority_failed = self.priority_failed.saturating_add(other.priority_failed);
    }

    fn note(&mut self, failed: bool, priority_fee: u64) {
        self.transactions = self.transactions.saturating_add(1);
        if failed {
            self.failed = self.failed.saturating_add(1);
            self.priority_failed = self.priority_failed.saturating_add(priority_fee);
        }
    }
}

/// One rooted block's reading.
#[derive(Debug, Default)]
pub struct BlockTally {
    epoch: Epoch,
    ours: bool,
    /// Every non-vote transaction of ours whose result was found.
    total: Tally,
    payers: HashMap<Pubkey, Tally>,
}

#[derive(Debug, Default)]
struct Payer {
    ours: Tally,
    away: Tally,
    flagged_slot: Option<Slot>,
}

#[derive(Debug, Default)]
struct EpochPayers {
    blocks: u64,
    ours: Tally,
    /// The followed payers' transactions in other leaders' blocks.
    away: Tally,
    payers: HashMap<Pubkey, Payer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Alert {
    Flagged,
    Escalated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EpochFigures {
    pub epoch: Epoch,
    pub blocks: u64,
    pub transactions: u64,
    pub failed: u64,
    pub priority_failed: u64,
    pub away_transactions: u64,
    pub away_failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayerRow {
    pub epoch: Epoch,
    pub payer: String,
    pub transactions: u64,
    pub failed: u64,
    pub priority_failed: u64,
    pub away_transactions: u64,
    pub away_failed: u64,
    pub alert: Option<Alert>,
    /// Our slot in which it first met the flag.
    pub flagged_slot: Option<Slot>,
}

/// Newest epoch first; the payers are the newest epoch's, by priority fees on failed transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeePayerSummary {
    pub epochs: Vec<EpochFigures>,
    pub payers: Vec<PayerRow>,
    pub alerts: Vec<PayerRow>,
}

#[derive(Debug, Default)]
pub struct FeePayers {
    epochs: BTreeMap<Epoch, EpochPayers>,
}

impl FeePayers {
    /// Whether `payer` is followed in other leaders' blocks of `epoch`.
    pub fn followed(&self, epoch: Epoch, payer: &Pubkey) -> bool {
        self.epochs
            .get(&epoch)
            .is_some_and(|held| held.payers.contains_key(payer))
    }

    /// Counts a rooted block, read in slot order, and keeps its epoch and the one before.
    pub fn add(&mut self, slot: Slot, tally: BlockTally) {
        let kept = tally.epoch.saturating_sub(1);
        self.epochs.retain(|held, _| *held >= kept);
        let epoch = self.epochs.entry(tally.epoch).or_default();
        if tally.ours {
            epoch.blocks = epoch.blocks.saturating_add(1);
            epoch.ours.add(&tally.total);
            for (key, block) in &tally.payers {
                if epoch.payers.len() >= MAX_PAYERS && !epoch.payers.contains_key(key) {
                    continue;
                }
                let payer = epoch.payers.entry(*key).or_default();
                payer.ours.add(block);
                if payer.flagged_slot.is_none() && is_flagged(&payer.ours) {
                    payer.flagged_slot = Some(slot);
                }
            }
        } else {
            for (key, block) in &tally.payers {
                if let Some(payer) = epoch.payers.get_mut(key) {
                    payer.away.add(block);
                    epoch.away.add(block);
                }
            }
        }
    }

    pub fn summary(&self) -> Option<FeePayerSummary> {
        let (newest, _) = self.epochs.last_key_value()?;
        let epochs = self
            .epochs
            .iter()
            .rev()
            .map(|(epoch, held)| EpochFigures {
                epoch: *epoch,
                blocks: held.blocks,
                transactions: held.ours.transactions,
                failed: held.ours.failed,
                priority_failed: held.ours.priority_failed,
                away_transactions: held.away.transactions,
                away_failed: held.away.failed,
            })
            .collect();

        // Ranked before any row is built, since an epoch can hold many thousands of payers.
        let mut ranked: Vec<(&Pubkey, &Payer)> = self
            .epochs
            .get(newest)
            .into_iter()
            .flat_map(|held| held.payers.iter())
            .filter(|(_, payer)| payer.ours.failed > 0)
            .collect();
        ranked.sort_unstable_by_key(|(_, payer)| {
            Reverse((payer.ours.priority_failed, payer.ours.failed))
        });
        let payers = ranked
            .into_iter()
            .take(TOP_PAYERS)
            .map(|(key, payer)| row(*newest, key, payer))
            .collect();

        let mut alerts: Vec<PayerRow> = self
            .epochs
            .iter()
            .flat_map(|(epoch, held)| {
                held.payers
                    .iter()
                    .filter(|(_, payer)| is_flagged(&payer.ours))
                    .map(|(key, payer)| row(*epoch, key, payer))
            })
            .collect();
        alerts.sort_by_key(|row| {
            Reverse((
                row.epoch,
                row.alert == Some(Alert::Escalated),
                row.priority_failed,
            ))
        });
        alerts.truncate(MAX_ALERTS);

        Some(FeePayerSummary {
            epochs,
            payers,
            alerts,
        })
    }
}

fn row(epoch: Epoch, key: &Pubkey, payer: &Payer) -> PayerRow {
    PayerRow {
        epoch,
        payer: key.to_string(),
        transactions: payer.ours.transactions,
        failed: payer.ours.failed,
        priority_failed: payer.ours.priority_failed,
        away_transactions: payer.away.transactions,
        away_failed: payer.away.failed,
        alert: alert(&payer.ours, &payer.away),
        flagged_slot: payer.flagged_slot,
    }
}

fn is_flagged(ours: &Tally) -> bool {
    ours.transactions >= MIN_TRANSACTIONS
        && ours.failed.saturating_mul(100) >= ours.transactions.saturating_mul(MIN_FAILED_PERCENT)
        && ours.priority_failed >= MIN_PRIORITY_FAILED
}

fn alert(ours: &Tally, away: &Tally) -> Option<Alert> {
    if !is_flagged(ours) {
        return None;
    }
    // Our rate over three times theirs, cross multiplied.
    let here = u128::from(ours.failed).saturating_mul(u128::from(away.transactions));
    let there = u128::from(away.failed)
        .saturating_mul(u128::from(ours.transactions))
        .saturating_mul(ESCALATE_RATIO);
    if away.transactions >= MIN_AWAY_TRANSACTIONS && here > there {
        Some(Alert::Escalated)
    } else {
        Some(Alert::Flagged)
    }
}

#[derive(Debug)]
struct PaidTransaction<'a> {
    transaction: &'a VersionedTransaction,
    payer: Pubkey,
    failed: Option<bool>,
    priority: u64,
    paid: u64,
}

/// What one of our blocks' non-vote transactions paid, as they are read.
#[derive(Debug, Default)]
struct BlockPaid<'a> {
    payers: HashMap<Pubkey, TopFeePayer>,
    transactions: Vec<PaidTransaction<'a>>,
}

impl<'a> BlockPaid<'a> {
    fn note(
        &mut self,
        transaction: &'a VersionedTransaction,
        payer: Pubkey,
        failed: Option<bool>,
        priority: u64,
        base: u64,
    ) {
        let paid = base.saturating_add(priority);
        let fees = self.payers.entry(payer).or_insert(TopFeePayer {
            payer,
            transactions: 0,
            failed: 0,
            priority: 0,
            paid: 0,
        });
        fees.transactions = fees.transactions.saturating_add(1);
        fees.failed = fees
            .failed
            .saturating_add(u64::from(failed.unwrap_or(false)));
        fees.priority = fees.priority.saturating_add(priority);
        fees.paid = fees.paid.saturating_add(paid);
        self.transactions.push(PaidTransaction {
            transaction,
            payer,
            failed,
            priority,
            paid,
        });
    }

    fn top(mut self, slot: Slot) -> BlockFees {
        let payer_count = u64::try_from(self.payers.len()).unwrap_or(u64::MAX);
        let mut payers: Vec<TopFeePayer> = self.payers.into_values().collect();
        payers.sort_unstable_by_key(|fees| Reverse(fees.paid));
        payers.truncate(TOP_IN_BLOCK);
        self.transactions
            .sort_unstable_by_key(|row| Reverse(row.paid));
        self.transactions.truncate(TOP_IN_BLOCK);
        BlockFees {
            slot,
            payer_count,
            payers,
            transactions: self
                .transactions
                .into_iter()
                .map(|row| TopFeeTransaction {
                    signature: row
                        .transaction
                        .signatures
                        .first()
                        .map_or_else(String::new, ToString::to_string),
                    payer: row.payer,
                    failed: row.failed,
                    priority: row.priority,
                    paid: row.paid,
                })
                .collect(),
        }
    }
}

/// What a block's tally needs of its bank.
pub struct BlockInputs<'a> {
    pub slot: Slot,
    pub epoch: Epoch,
    pub ours: bool,
    pub lamports_per_signature: u64,
    pub feature_set: &'a FeatureSet,
}

/// The transactions whose results the tally reads: every non-vote one of our block, or only the
/// followed payers' in another's.
pub fn tallied(
    entries: &[Entry],
    ours: bool,
    followed: impl Fn(&Pubkey) -> bool,
) -> impl Iterator<Item = (&VersionedTransaction, &Pubkey)> {
    entries
        .iter()
        .flat_map(|entry| &entry.transactions)
        .filter(|transaction| !is_simple_vote(transaction))
        .filter_map(|transaction| {
            Some((
                transaction,
                transaction.message.static_account_keys().first()?,
            ))
        })
        .filter(move |(_, payer)| ours || followed(payer))
}

/// The block's [`tallied`] transactions with their payers, and any source of their results. A
/// result not found leaves the transaction out of the tally, though not out of what the block paid.
pub fn tally_block<'a>(
    block: &BlockInputs,
    tallied: impl IntoIterator<Item = (&'a VersionedTransaction, &'a Pubkey)>,
    failed: impl Fn(&VersionedTransaction) -> Option<bool>,
) -> (BlockTally, Option<BlockFees>) {
    let ours = block.ours;
    let mut tally = BlockTally {
        epoch: block.epoch,
        ours,
        ..BlockTally::default()
    };
    let mut paid = BlockPaid::default();
    for (transaction, payer) in tallied {
        let result = failed(transaction);
        let priority = if ours {
            let priority = priority_fee(transaction, block.feature_set);
            let base = base_fee(transaction, block.lamports_per_signature);
            paid.note(transaction, *payer, result, priority, base);
            priority
        } else {
            0
        };
        let Some(failed) = result else {
            continue;
        };
        tally
            .payers
            .entry(*payer)
            .or_default()
            .note(failed, priority);
        if ours {
            tally.total.note(failed, priority);
        }
    }
    (tally, ours.then(|| paid.top(block.slot)))
}

/// Charged for each of the transaction's signatures and each a precompile verifies.
fn base_fee(transaction: &VersionedTransaction, lamports_per_signature: u64) -> u64 {
    let precompiles = get_precompile_signature_details(instructions(&transaction.message));
    let counts = SignatureCounts {
        num_transaction_signatures: u64::try_from(transaction.signatures.len()).unwrap_or(u64::MAX),
        num_ed25519_signatures: precompiles.num_ed25519_instruction_signatures,
        num_secp256k1_signatures: precompiles.num_secp256k1_instruction_signatures,
        num_secp256r1_signatures: precompiles.num_secp256r1_instruction_signatures,
    };
    calculate_signature_fee(counts, lamports_per_signature)
}

/// What the fee charged beyond the signatures: a v1 message states it, earlier ones set a price
/// per compute unit.
fn priority_fee(transaction: &VersionedTransaction, feature_set: &FeatureSet) -> u64 {
    if let VersionedMessage::V1(v1) = &transaction.message {
        return v1.config.priority_fee.unwrap_or(0);
    }
    process_compute_budget_instructions(instructions(&transaction.message), feature_set)
        .map_or(0, |limits| limits.get_prioritization_fee())
}

/// Each instruction with its program, as the runtime's readers take them.
fn instructions(
    message: &VersionedMessage,
) -> impl Iterator<Item = (&Pubkey, SVMInstruction<'_>)> + Clone {
    let keys = message.static_account_keys();
    message.instructions().iter().filter_map(|instruction| {
        let program = keys.get(usize::from(instruction.program_id_index))?;
        Some((program, SVMInstruction::from(instruction)))
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{compiled_instruction::CompiledInstruction, legacy},
        solana_sdk_ids::{ed25519_program, secp256k1_program, secp256r1_program},
        solana_signature::Signature,
        solana_signer::Signer,
        solana_system_interface::instruction::transfer,
        solana_transaction::Transaction,
    };

    fn tally(transactions: u64, failed: u64, priority_failed: u64) -> Tally {
        Tally {
            transactions,
            failed,
            priority_failed,
        }
    }

    fn block(epoch: Epoch, ours: bool, payers: &[(Pubkey, Tally)]) -> BlockTally {
        let mut total = Tally::default();
        for (_, payer) in payers {
            total.add(payer);
        }
        BlockTally {
            epoch,
            ours,
            total,
            payers: payers.iter().copied().collect(),
        }
    }

    #[test]
    fn flags_at_each_threshold() {
        assert!(is_flagged(&tally(100, 10, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(99, 99, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(100, 9, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(
            100,
            100,
            MIN_PRIORITY_FAILED.saturating_sub(1)
        )));
    }

    #[test]
    fn escalates_over_three_times_the_rate_elsewhere() {
        let ours = tally(100, 40, MIN_PRIORITY_FAILED);
        // 40% against 13% is over three times, against 14% not.
        assert_eq!(alert(&ours, &tally(100, 13, 0)), Some(Alert::Escalated));
        assert_eq!(alert(&ours, &tally(100, 14, 0)), Some(Alert::Flagged));
        assert_eq!(alert(&ours, &tally(100, 0, 0)), Some(Alert::Escalated));
        // Too few elsewhere to judge.
        assert_eq!(alert(&ours, &tally(99, 0, 0)), Some(Alert::Flagged));
        assert_eq!(alert(&tally(100, 5, 0), &tally(100, 0, 0)), None);
    }

    #[test]
    fn counts_rooted_blocks_and_follows_our_payers_elsewhere() {
        let (spammer, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let mut payers = FeePayers::default();
        payers.add(
            10,
            block(5, true, &[(spammer, tally(120, 60, MIN_PRIORITY_FAILED))]),
        );
        // Another leader's block.
        payers.add(
            11,
            block(
                5,
                false,
                &[(spammer, tally(200, 10, 0)), (other, tally(50, 50, 0))],
            ),
        );
        let summary = payers.summary().unwrap();
        assert_eq!(
            summary.epochs,
            [EpochFigures {
                epoch: 5,
                blocks: 1,
                transactions: 120,
                failed: 60,
                priority_failed: MIN_PRIORITY_FAILED,
                away_transactions: 200,
                away_failed: 10,
            }]
        );
        assert_eq!(summary.payers.len(), 1);
        let row = &summary.alerts[0];
        assert_eq!(row.payer, spammer.to_string());
        assert_eq!(row.alert, Some(Alert::Escalated));
        assert_eq!(row.flagged_slot, Some(10));
    }

    #[test]
    fn keeps_this_epoch_and_the_last() {
        let payer = Pubkey::new_unique();
        let mut payers = FeePayers::default();
        for (slot, epoch) in [(1, 3), (2, 4), (3, 5)] {
            payers.add(slot, block(epoch, true, &[(payer, tally(1, 1, 1))]));
        }
        let summary = payers.summary().unwrap();
        let epochs: Vec<Epoch> = summary.epochs.iter().map(|figures| figures.epoch).collect();
        assert_eq!(epochs, [5, 4]);
        assert!(summary.payers.iter().all(|row| row.epoch == 5));
    }

    #[test]
    fn orders_alerts_newest_epoch_and_escalated_first() {
        let [a, b, c] = std::array::from_fn(|_| Pubkey::new_unique());
        let flagged = tally(100, 100, MIN_PRIORITY_FAILED);
        let mut payers = FeePayers::default();
        payers.add(1, block(4, true, &[(a, flagged)]));
        payers.add(2, block(5, true, &[(b, flagged), (c, flagged)]));
        payers.add(3, block(5, false, &[(c, tally(100, 1, 0))]));
        let alerts: Vec<(String, Option<Alert>)> = payers
            .summary()
            .unwrap()
            .alerts
            .into_iter()
            .map(|row| (row.payer, row.alert))
            .collect();
        assert_eq!(
            alerts,
            [
                (c.to_string(), Some(Alert::Escalated)),
                (b.to_string(), Some(Alert::Flagged)),
                (a.to_string(), Some(Alert::Flagged)),
            ]
        );
    }

    #[test]
    fn reads_results_and_priority_fees_as_given() {
        let payer = Keypair::new();
        let to = Pubkey::new_unique();
        let paid = |lamports: u64| {
            Transaction::new_signed_with_payer(
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                    ComputeBudgetInstruction::set_compute_unit_price(2_000_000),
                    transfer(&payer.pubkey(), &to, lamports),
                ],
                Some(&payer.pubkey()),
                &[&payer],
                Hash::default(),
            )
        };
        let (landed, failed) = (paid(1_000), paid(10_000_000_000));
        let unprocessed =
            solana_system_transaction::transfer(&Keypair::new(), &to, 1, Hash::default());
        let results = HashMap::from([(landed.signatures[0], false), (failed.signatures[0], true)]);
        let entries = [Entry {
            num_hashes: 1,
            hash: Hash::default(),
            transactions: [landed, failed, unprocessed]
                .into_iter()
                .map(VersionedTransaction::from)
                .collect(),
        }];
        let feature_set = FeatureSet::all_enabled();
        let block = |ours| BlockInputs {
            slot: 7,
            epoch: 0,
            ours,
            lamports_per_signature: 5_000,
            feature_set: &feature_set,
        };
        let result =
            |transaction: &VersionedTransaction| results.get(&transaction.signatures[0]).copied();

        let (ours, fees) = tally_block(&block(true), tallied(&entries, true, |_| false), result);
        // 2,000,000 micro-lamports per unit over a 10,000 unit limit.
        assert_eq!(ours.total, tally(2, 1, 20_000));
        assert_eq!(ours.payers[&payer.pubkey()], tally(2, 1, 20_000));
        let fees = fees.expect("our block's fees");
        assert_eq!(fees.slot, 7);
        // The unprocessed transaction has no result, yet is counted as paid.
        assert_eq!(fees.transactions.len(), 3);
        assert_eq!(fees.payer_count, 2);
        let top = &fees.payers[0];
        assert_eq!(top.payer, payer.pubkey());
        assert_eq!((top.transactions, top.failed), (2, 1));
        assert_eq!(top.priority, 40_000);
        assert_eq!(top.paid, 10_000u64.saturating_add(40_000));
        let failures: Vec<Option<bool>> = fees.transactions.iter().map(|row| row.failed).collect();
        assert!(failures.contains(&Some(false)) && failures.contains(&Some(true)));
        assert_eq!(failures[2], None);
        assert_eq!(fees.transactions[0].priority, 20_000);

        let away_payer = |key: &Pubkey| *key == payer.pubkey();
        let (away, no_fees) =
            tally_block(&block(false), tallied(&entries, false, away_payer), result);
        assert_eq!(away.total, Tally::default());
        assert_eq!(away.payers[&payer.pubkey()], tally(2, 1, 0));
        assert!(no_fees.is_none());
        assert!(
            tally_block(&block(false), tallied(&entries, false, |_| false), result)
                .0
                .payers
                .is_empty()
        );
    }

    #[test]
    fn lists_a_blocks_five_highest_payers_and_transactions() {
        let transaction = VersionedTransaction::default();
        let mut paid = BlockPaid::default();
        let payers: Vec<Pubkey> = (0..7).map(|_| Pubkey::new_unique()).collect();
        for (fee, payer) in (1u64..).zip(&payers) {
            paid.note(&transaction, *payer, Some(false), fee, 0);
        }
        // Many small payments outrank the larger single ones, by payer but not by transaction.
        for _ in 0..30 {
            paid.note(&transaction, payers[0], Some(true), 1, 0);
        }
        let fees = paid.top(9);
        assert_eq!(fees.slot, 9);
        assert_eq!(fees.payer_count, 7);
        assert_eq!(fees.payers.len(), TOP_IN_BLOCK);
        assert_eq!(fees.payers[0].payer, payers[0]);
        assert_eq!(fees.payers[0].paid, 31);
        assert_eq!(fees.payers[0].failed, 30);
        assert_eq!(fees.transactions.len(), TOP_IN_BLOCK);
        let highest: Vec<u64> = fees.transactions.iter().map(|row| row.paid).collect();
        assert_eq!(highest, [7, 6, 5, 4, 3]);
        assert_eq!(fees.transactions[0].payer, payers[6]);
    }
    #[test]
    fn charges_each_signature_a_precompile_verifies() {
        let with_program = |program: Pubkey, data: Vec<u8>| VersionedTransaction {
            signatures: vec![Signature::default(); 2],
            message: VersionedMessage::Legacy(legacy::Message {
                account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique(), program],
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![],
                    data,
                }],
                ..legacy::Message::default()
            }),
        };
        let fee = |program, data| base_fee(&with_program(program, data), 5_000);
        assert_eq!(fee(ed25519_program::id(), vec![3, 9]), 25_000);
        assert_eq!(fee(secp256k1_program::id(), vec![1]), 15_000);
        assert_eq!(fee(secp256r1_program::id(), vec![2]), 20_000);
        assert_eq!(fee(ed25519_program::id(), vec![]), 10_000);
        // The first byte of any other program's data counts for nothing.
        assert_eq!(fee(Pubkey::new_unique(), vec![200]), 10_000);
    }
}
