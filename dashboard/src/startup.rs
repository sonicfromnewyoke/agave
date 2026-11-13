//! Publishing the validator's boot phase. Shared by the boot thread and the
//! collector, so the handover between them is invisible to the client.

use {
    crate::{
        metrics_tap::StakeInGossip,
        proto::{Publisher, TOPIC_SUMMARY},
        validator_info::ValidatorInfoCache,
    },
    serde::Serialize,
    solana_clock::Slot,
    solana_core::validator::ValidatorStartProgress,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
};

pub const KEY_STARTUP_PROGRESS: &str = "startup_progress";
pub const KEY_GOSSIP_STAKE: &str = "gossip_stake";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipStake {
    pub slot: Slot,
    pub shred_version: u16,
    pub total: u64,
    pub seen: u64,
    pub validators: Vec<GossipValidator>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GossipValidator {
    pub identity: String,
    pub name: Option<String>,
    pub version: Option<String>,
    pub stake: u64,
    pub seen: bool,
}

/// `seen` is each node seen in gossip, by its version.
pub fn tally_gossip_stake(
    slot: Slot,
    shred_version: u16,
    staked: HashMap<Pubkey, u64>,
    seen: &HashMap<Pubkey, String>,
    names: &ValidatorInfoCache,
) -> GossipStake {
    let mut validators: Vec<GossipValidator> = staked
        .into_iter()
        .map(|(identity, stake)| {
            let info = names.get(&identity);
            let version = seen.get(&identity).cloned();
            GossipValidator {
                identity: identity.to_string(),
                name: info.and_then(|info| info.name.clone()),
                seen: version.is_some(),
                version,
                stake,
            }
        })
        .collect();
    validators.sort_by(|a, b| (b.stake, &a.identity).cmp(&(a.stake, &b.identity)));
    let stake = |seen_only: bool| {
        validators
            .iter()
            .filter(|v| v.seen || !seen_only)
            .fold(0, |sum, v| u64::saturating_add(sum, v.stake))
    };
    GossipStake {
        slot,
        shred_version,
        total: stake(false),
        seen: stake(true),
        validators,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StartupProgress {
    pub phase: String,
    pub detail: Option<String>,
    pub running: bool,
    /// From 0 to 1, measured from where replay began.
    pub fraction: Option<f64>,
    /// From 0 to 1, a whole percent truncated by the validator.
    pub stake_percent: Option<f64>,
    pub stake_in_gossip: Option<StakeInGossip>,
    /// Since most phases cannot say how far along they are.
    pub phase_elapsed_nanos: u64,
    pub phases_taken: Vec<PhaseTiming>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PhaseTiming {
    pub phase: String,
    pub elapsed_nanos: u64,
}

debounced_keys! {
    pub struct StartupPublisher {
        debounce: StartupProgress = (TOPIC_SUMMARY, KEY_STARTUP_PROGRESS),
        ;
        /// Replay starts from a snapshot, so `slot / max_slot` would sit near 100% throughout.
        replay_origin: Option<Slot> = None,
        /// The validator reports its phase but not when it got there.
        current: Option<(String, Instant)> = None,
        /// Accumulated if a phase comes round again, as `loading_ledger` does.
        taken: Vec<PhaseTiming> = Vec::new(),
    }
}

impl StartupPublisher {
    pub fn publish(
        &mut self,
        publisher: &Publisher,
        progress: ValidatorStartProgress,
        stake_in_gossip: Option<StakeInGossip>,
    ) {
        let phase = describe(progress);
        let waiting = matches!(
            progress,
            ValidatorStartProgress::WaitingForSupermajority { .. }
        );
        let progress = StartupProgress {
            phase: phase.name.to_string(),
            detail: phase.detail,
            running: matches!(progress, ValidatorStartProgress::Running),
            fraction: self.fraction(phase.replay_slots),
            stake_percent: phase.stake_percent,
            stake_in_gossip: stake_in_gossip.filter(|_| waiting),
            phase_elapsed_nanos: self.elapsed(phase.name, Instant::now()),
            phases_taken: self.taken.clone(),
        };
        self.debounce.publish(publisher, progress);
    }

    /// Whole seconds, so the debounce does not send four messages a second.
    fn elapsed(&mut self, phase: &str, now: Instant) -> u64 {
        if let Some((current, since)) = &self.current
            && current == phase
        {
            return whole_seconds(now, *since);
        }
        if let Some((finished, since)) = self.current.replace((phase.to_string(), now)) {
            let elapsed_nanos = now.duration_since(since).as_nanos() as u64;
            match self.taken.iter_mut().find(|taken| taken.phase == finished) {
                Some(taken) => {
                    taken.elapsed_nanos = taken.elapsed_nanos.saturating_add(elapsed_nanos)
                }
                None => self.taken.push(PhaseTiming {
                    phase: finished,
                    elapsed_nanos,
                }),
            }
        }
        0
    }

    fn fraction(&mut self, slots: Option<(Slot, Slot)>) -> Option<f64> {
        let (current, target) = slots?;
        let origin = *self.replay_origin.get_or_insert(current);
        // Replay can be handed a target it has already passed, and the origin is
        // only an estimate; neither may run the meter backwards or past the end.
        let span = target.checked_sub(origin)?;
        if span == 0 {
            return Some(1.0);
        }
        let done = current.saturating_sub(origin).min(span);
        Some(done as f64 / span as f64)
    }
}

struct Phase {
    name: &'static str,
    detail: Option<String>,
    replay_slots: Option<(Slot, Slot)>,
    stake_percent: Option<f64>,
}

fn describe(progress: ValidatorStartProgress) -> Phase {
    let named = |name| Phase {
        name,
        detail: None,
        replay_slots: None,
        stake_percent: None,
    };
    match progress {
        ValidatorStartProgress::Initializing => named("initializing"),
        ValidatorStartProgress::SearchingForRpcService => named("searching_for_rpc_service"),
        ValidatorStartProgress::DownloadingSnapshot { slot, rpc_addr } => Phase {
            detail: Some(format!("slot {slot} from {rpc_addr}")),
            ..named("downloading_snapshot")
        },
        ValidatorStartProgress::CleaningBlockStore => named("cleaning_blockstore"),
        ValidatorStartProgress::CleaningAccounts => named("cleaning_accounts"),
        ValidatorStartProgress::LoadingLedger => named("loading_ledger"),
        ValidatorStartProgress::ProcessingLedger { slot, max_slot } => Phase {
            detail: Some(format!("slot {slot} of {max_slot}")),
            replay_slots: Some((slot, max_slot)),
            ..named("processing_ledger")
        },
        ValidatorStartProgress::StartingServices => named("starting_services"),
        ValidatorStartProgress::Halted => named("halted"),
        ValidatorStartProgress::WaitingForSupermajority {
            slot,
            gossip_stake_percent,
        } => Phase {
            detail: Some(format!("slot {slot}")),
            stake_percent: Some(gossip_stake_percent as f64 / 100.0),
            ..named("waiting_for_supermajority")
        },
        ValidatorStartProgress::Running => named("running"),
    }
}

fn whole_seconds(now: Instant, since: Instant) -> u64 {
    Duration::from_secs(now.duration_since(since).as_secs()).as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replaying(slot: Slot, max_slot: Slot) -> ValidatorStartProgress {
        ValidatorStartProgress::ProcessingLedger { slot, max_slot }
    }

    /// What one publish of `phase` sends.
    fn sent(phase: ValidatorStartProgress, stake: Option<StakeInGossip>) -> String {
        let publisher = Publisher::new();
        StartupPublisher::default().publish(&publisher, phase, stake);
        publisher.snapshot().pop().unwrap().to_string()
    }

    #[test]
    fn test_fraction_is_measured_from_the_first_slot_seen_and_kept_in_bounds() {
        // Each case on a fresh publisher.
        let read = |slots: &[(Slot, Slot)]| {
            let mut publisher = StartupPublisher::default();
            slots
                .iter()
                .map(|&pair| publisher.fraction(Some(pair)))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            read(&[(1000, 2000), (1500, 2000), (2000, 2000)]),
            [Some(0.0), Some(0.5), Some(1.0)]
        );
        assert_eq!(StartupPublisher::default().fraction(None), None);
        // Never past one nor backwards.
        assert_eq!(
            read(&[(1000, 1100), (1200, 1100), (900, 1100)]),
            [Some(0.0), Some(1.0), Some(0.0)]
        );
        // A target already reached is complete, and one behind the origin reports nothing.
        assert_eq!(read(&[(1000, 1000)]), [Some(1.0)]);
        assert_eq!(read(&[(1000, 900)]), [None]);
    }

    #[test]
    fn test_a_phase_is_timed_from_when_it_was_first_seen() {
        let mut publisher = StartupPublisher::default();
        let base = Instant::now();
        assert_eq!(publisher.elapsed("loading_ledger", base), 0);
        assert_eq!(
            publisher.elapsed("loading_ledger", base + Duration::from_millis(4_500)),
            Duration::from_secs(4).as_nanos() as u64,
            "rounded down to whole seconds, or the debounce sends four a second"
        );
    }

    #[test]
    fn test_a_finished_phase_keeps_what_it_took_and_adds_to_it_when_entered_again() {
        // `loading_ledger` is entered once for the snapshot and again if the
        // blockstore has slots to process.
        let mut publisher = StartupPublisher::default();
        let base = Instant::now();
        publisher.elapsed("loading_ledger", base);
        publisher.elapsed("processing_ledger", base + Duration::from_secs(10));
        publisher.elapsed("loading_ledger", base + Duration::from_secs(15));
        publisher.elapsed("processing_ledger", base + Duration::from_secs(21));

        let took = |phase: &str, secs| PhaseTiming {
            phase: phase.to_string(),
            elapsed_nanos: Duration::from_secs(secs).as_nanos() as u64,
        };
        assert_eq!(
            publisher.taken,
            [took("loading_ledger", 16), took("processing_ledger", 5)],
            "one row per phase however often it comes round: ten seconds and six for loading"
        );
    }

    #[test]
    fn test_publishing_fills_in_the_fraction() {
        let publisher = Publisher::new();
        let mut startup = StartupPublisher::default();
        startup.publish(&publisher, replaying(100, 200), None);
        startup.publish(&publisher, replaying(150, 200), None);

        let snapshot = publisher.snapshot();
        assert_eq!(
            snapshot.len(),
            1,
            "startup progress is a single retained key"
        );
        assert!(
            snapshot[0].contains(r#""fraction":0.5"#),
            "expected a half-complete fraction, got {}",
            snapshot[0]
        );
    }

    #[test]
    fn test_the_stake_count_rides_only_on_the_wait() {
        let seen = Some(StakeInGossip {
            online: 3,
            total: 10,
        });
        let waiting = ValidatorStartProgress::WaitingForSupermajority {
            slot: 5,
            gossip_stake_percent: 30,
        };
        for (phase, carried) in [
            (waiting, true),
            (ValidatorStartProgress::Running, false),
            (ValidatorStartProgress::StartingServices, false),
        ] {
            let sent = sent(phase, seen);
            assert_eq!(
                sent.contains(r#""stake_in_gossip":{"online":3,"total":10}"#),
                carried,
                "{sent}"
            );
        }
    }

    #[test]
    fn test_every_phase_is_named_and_only_running_runs() {
        let phases = [
            ValidatorStartProgress::Initializing,
            ValidatorStartProgress::SearchingForRpcService,
            ValidatorStartProgress::CleaningBlockStore,
            ValidatorStartProgress::CleaningAccounts,
            ValidatorStartProgress::LoadingLedger,
            replaying(1, 2),
            ValidatorStartProgress::StartingServices,
            ValidatorStartProgress::Halted,
            ValidatorStartProgress::WaitingForSupermajority {
                slot: 5,
                gossip_stake_percent: 50,
            },
            ValidatorStartProgress::Running,
        ];
        for phase in phases {
            let sent = sent(phase, None);
            assert!(!sent.contains(r#""phase":"""#), "{sent}");
            assert_eq!(
                sent.contains(r#""running":true"#),
                phase == ValidatorStartProgress::Running,
                "{sent}"
            );
        }
    }
}
