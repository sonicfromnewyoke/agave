//! The agave-validator web dashboard, served by a process of its own beside the validator, read from
//! the validator's ledger, its admin socket, its JSON-RPC, its metrics export and `/proc`. Nothing
//! of it runs inside the validator. See dashboard/README.md.

/// A struct of [`proto::Debounced`] keys, each with its topic and key, then after a `;` any other
/// fields with their defaults, and its `Default`.
macro_rules! debounced_keys {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($key:ident: $type:ty = ($topic:expr, $wire:expr),)*
            $(; $($(#[$doc:meta])* $field_vis:vis $field:ident: $field_type:ty = $default:expr,)*)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $($key: $crate::proto::Debounced<$type>,)*
            $($($(#[$doc])* $field_vis $field: $field_type,)*)?
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $($key: $crate::proto::Debounced::new($topic, $wire),)*
                    $($($field: $default,)*)?
                }
            }
        }
    };
}

mod admin;
mod certs;
mod collect;
mod frames;
mod geoip;
mod history;
mod host_stats;
mod ledger;
mod ledger_feed;
mod meter_source;
mod meters;
mod metrics_listener;
mod metrics_tap;
mod net_ifaces;
mod net_stats;
mod poll;
mod produced;
mod proto;
mod remote;
mod search;
mod server;
mod slot_feed;
mod slots;
mod snapshot;
mod startup;
mod thread_stats;
mod tips;
mod turns;
mod udp_drops;
mod validator_info;
mod versions;

use {
    crate::{
        admin::AdminClient,
        collect::{FAILED_UNREAD, Replies},
        frames::Frames,
        history::{PACKED_SLOTS, SlotHistory},
        meter_source::HostPaths,
        meters::{METER_INTERVAL, Meters},
        metrics_listener::Forward,
        metrics_tap::MetricsTap,
        proto::Publisher,
        remote::{Remote, Shared},
        tips::TipMeter,
        validator_info::ValidatorInfoCache,
    },
    agave_snapshots::{
        SnapshotInterval,
        snapshot_config::{
            DEFAULT_FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            DEFAULT_INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS, SnapshotConfig, SnapshotUsage,
        },
    },
    clap::{App, Arg, ArgMatches, value_t, values_t},
    solana_clap_utils::input_validators::{is_parsable, is_pubkey},
    solana_pubkey::Pubkey,
    std::{
        error::Error,
        net::SocketAddr,
        num::NonZeroU64,
        path::PathBuf,
        str::FromStr,
        sync::{Arc, RwLock, atomic::AtomicU64},
        thread,
        time::{Duration, Instant},
    },
    tokio::net::TcpListener,
};

/// The server and the pollers; the meters have a thread of their own.
const RUNTIME_THREADS: usize = 2;
const PID_WAIT: Duration = Duration::from_millis(250);
const DEFAULT_LISTEN: &str = "127.0.0.1:11000";
/// Frames held between their source and the slot feed's tick: a few seconds of a busy feed.
const FRAMES: usize = 65_536;

struct Config {
    listen: SocketAddr,
    allowed_hosts: Vec<String>,
    rpc_url: Option<String>,
    host: HostPaths,
    statics: poll::Static,
    metrics_listen: Option<SocketAddr>,
    metrics_forward: Option<Forward>,
    allow_private_addr: bool,
}

impl Config {
    fn from_matches(matches: &ArgMatches) -> Self {
        let ledger = PathBuf::from(matches.value_of("ledger").expect("required"));
        let snapshots = matches
            .value_of("snapshots")
            .map_or_else(|| ledger.clone(), PathBuf::from);
        let incremental_snapshots = matches
            .value_of("incremental_snapshots")
            .map_or_else(|| snapshots.clone(), PathBuf::from);
        let (full_interval, incremental_interval) = snapshot_intervals(matches);
        Self {
            listen: value_t!(matches, "listen", SocketAddr).unwrap_or_else(|err| err.exit()),
            allowed_hosts: values_t!(matches, "allowed_host", String).unwrap_or_default(),
            rpc_url: matches.value_of("rpc_url").map(str::to_string),
            host: HostPaths {
                // agave-validator's own default: one accounts directory inside the ledger.
                accounts: values_t!(matches, "accounts", PathBuf)
                    .unwrap_or_else(|_| vec![ledger.join("accounts")]),
                // The archive directories and intervals as the validator was given them, since
                // nothing outside the validator reports its own.
                snapshot_config: SnapshotConfig {
                    usage: if full_interval == SnapshotInterval::Disabled {
                        SnapshotUsage::LoadOnly
                    } else {
                        SnapshotUsage::LoadAndGenerate
                    },
                    full_snapshot_archive_interval: full_interval,
                    incremental_snapshot_archive_interval: incremental_interval,
                    full_snapshot_archives_dir: snapshots,
                    incremental_snapshot_archives_dir: incremental_snapshots,
                    bank_snapshots_dir: ledger.join("snapshots"),
                    ..SnapshotConfig::default()
                },
                ledger,
            },
            statics: poll::Static {
                vote_account: matches.value_of("vote_account").map(str::to_string),
                tip_payment_program: matches.value_of("tip_payment_program").map(str::to_string),
                commission_bps: value_t!(matches, "commission_bps", u16).ok(),
            },
            metrics_listen: matches.value_of("metrics_listen").map(|_| {
                value_t!(matches, "metrics_listen", SocketAddr).unwrap_or_else(|err| err.exit())
            }),
            metrics_forward: matches.value_of("metrics_forward").map(|config| {
                Forward::parse(config).unwrap_or_else(|err| {
                    eprintln!("agave-dashboard: --metrics-forward: {err}");
                    std::process::exit(1)
                })
            }),
            allow_private_addr: matches.is_present("allow_private_addr"),
        }
    }
}

/// The snapshot intervals from the flags agave-validator takes, read as it reads them: without
/// incremental snapshots, `--snapshot-interval-slots` is the full snapshots' interval.
fn snapshot_intervals(matches: &ArgMatches) -> (SnapshotInterval, SnapshotInterval) {
    if matches.is_present("no_snapshots") {
        return (SnapshotInterval::Disabled, SnapshotInterval::Disabled);
    }
    let slots = |name: &str, default: NonZeroU64| {
        value_t!(matches, name, NonZeroU64)
            .map_or(SnapshotInterval::Slots(default), SnapshotInterval::Slots)
    };
    let snapshot_interval = slots(
        "snapshot_interval_slots",
        DEFAULT_INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
    );
    if matches.is_present("no_incremental_snapshots") {
        (snapshot_interval, SnapshotInterval::Disabled)
    } else {
        (
            slots(
                "full_snapshot_interval_slots",
                DEFAULT_FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            ),
            snapshot_interval,
        )
    }
}

fn cli() -> App<'static, 'static> {
    App::new("agave-dashboard")
        .about(
            "The agave-validator dashboard, served beside the validator. Reads the validator's \
             ledger and the admin socket in it, its JSON-RPC and /proc, so it must run on the \
             validator's host as the validator's user",
        )
        .arg(
            Arg::with_name("ledger")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .help(
                    "The validator's ledger directory, followed as it is written, which holds its \
                     admin socket",
                ),
        )
        .arg(
            Arg::with_name("listen")
                .long("listen")
                .value_name("HOST:PORT")
                .takes_value(true)
                .default_value(DEFAULT_LISTEN)
                .help(
                    "Where to serve the dashboard. It is unauthenticated: keep it on loopback or \
                     behind a proxy",
                ),
        )
        .arg(
            Arg::with_name("allowed_host")
                .long("allowed-host")
                .value_name("HOST")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help(
                    "Another host name the dashboard answers to. Repeat the flag for more than \
                     one. Required behind a reverse proxy, which forwards the name the visitor typed. \
                     IP addresses and 'localhost' are always accepted",
                ),
        )
        .arg(
            Arg::with_name("rpc_url")
                .long("rpc-url")
                .value_name("URL")
                .takes_value(true)
                .help(
                    "The RPC node the chain's state is read from: the validator's own, or \
                     another node of the cluster to spare it [default: the address its admin \
                     socket gives]",
                ),
        )
        .arg(
            Arg::with_name("accounts")
                .long("accounts")
                .value_name("DIR")
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help("An accounts directory of the validator's [default: <ledger>/accounts]"),
        )
        .arg(
            Arg::with_name("snapshots")
                .long("snapshots")
                .value_name("DIR")
                .takes_value(true)
                .help("The validator's full snapshot archive directory [default: the ledger]"),
        )
        .arg(
            Arg::with_name("incremental_snapshots")
                .long("incremental-snapshots")
                .value_name("DIR")
                .takes_value(true)
                .help("Its incremental snapshot archive directory [default: --snapshots]"),
        )
        .arg(
            Arg::with_name("no_snapshots")
                .long("no-snapshots")
                .help("As given to the validator: it writes no snapshots"),
        )
        .arg(
            Arg::with_name("no_incremental_snapshots")
                .long("no-incremental-snapshots")
                .help("As given to the validator: it writes no incremental snapshots"),
        )
        .arg(
            Arg::with_name("snapshot_interval_slots")
                .long("snapshot-interval-slots")
                .value_name("SLOTS")
                .takes_value(true)
                .validator(is_parsable::<NonZeroU64>)
                .help(
                    "As given to the validator: the incremental snapshots' interval, or the \
                     full snapshots' one where incremental snapshots are off",
                ),
        )
        .arg(
            Arg::with_name("full_snapshot_interval_slots")
                .long("full-snapshot-interval-slots")
                .value_name("SLOTS")
                .takes_value(true)
                .validator(is_parsable::<NonZeroU64>)
                .help("As given to the validator: the full snapshots' interval"),
        )
        .arg(
            Arg::with_name("allow_private_addr")
                .long("allow-private-addr")
                .help("As given to the validator: private addresses count as reachable"),
        )
        .arg(
            Arg::with_name("vote_account")
                .long("vote-account")
                .value_name("PUBKEY")
                .takes_value(true)
                .validator(is_pubkey)
                .help("The validator's vote account"),
        )
        .arg(
            Arg::with_name("tip_payment_program")
                .long("tip-payment-program")
                .value_name("PUBKEY")
                .takes_value(true)
                .help("Jito's tip payment program, where the validator runs jito's client"),
        )
        .arg(
            Arg::with_name("commission_bps")
                .long("commission-bps")
                .value_name("BPS")
                .takes_value(true)
                .validator(is_parsable::<u16>)
                .help("The validator's tip commission, in basis points"),
        )
        .arg(
            Arg::with_name("metrics_listen")
                .long("metrics-listen")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help(
                    "Receive the validator's metrics here: start the validator with \
                     SOLANA_METRICS_CONFIG=host=http://HOST:PORT,db=dashboard,u=dashboard,\
                     p=dashboard. Without it the metrics-fed cards stay empty",
                ),
        )
        .arg(
            Arg::with_name("metrics_forward")
                .long("metrics-forward")
                .value_name("CONFIG")
                .takes_value(true)
                .requires("metrics_listen")
                .help(
                    "Pass every metrics batch on to an InfluxDB, given as the \
                     SOLANA_METRICS_CONFIG the validator had before: host=..,db=..,u=..,p=..",
                ),
        )
}

fn main() {
    agave_logger::setup_with_default("info");
    let config = Config::from_matches(&cli().get_matches());
    if let Err(err) = run(config) {
        eprintln!("agave-dashboard: {err}");
        std::process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<dyn Error>> {
    // Following the ledger keeps every file of it open.
    if let Err(err) = solana_core::resource_limits::adjust_nofile_limit(false) {
        log::warn!("dashboard: the open file limit stays as it is: {err}");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_THREADS)
        .thread_name("solDashSaRt")
        .enable_all()
        .build()?;
    let publisher = Arc::new(Publisher::new());
    // Fed from the validator's metrics export, never from this process's own points.
    let metrics_tap = Arc::new(MetricsTap::default());
    let info_cache = Arc::new(RwLock::new(ValidatorInfoCache::default()));
    let handles = server::Handles {
        publisher: publisher.clone(),
        history: Arc::new(RwLock::new(SlotHistory::new(PACKED_SLOTS))),
        info_cache: info_cache.clone(),
        epochs: Arc::new(RwLock::new(Vec::new())),
        replies: Arc::new(Replies {
            tap: metrics_tap.clone(),
            ..Replies::default()
        }),
    };
    let history = handles.history.clone();
    let epochs = handles.epochs.clone();
    let replies = handles.replies.clone();
    let tip_program = config
        .statics
        .tip_payment_program
        .as_deref()
        .map(Pubkey::from_str)
        .transpose()
        .map_err(|err| format!("--tip-payment-program: {err}"))?;
    let remote: Shared = Arc::new(RwLock::new(Remote {
        tip_accounts: tip_program
            .map(|program| TipMeter::new(&program).accounts().to_vec())
            .unwrap_or_default(),
        ..Remote::default()
    }));

    // The names stop DNS rebinding; address literals are always accepted.
    let allowed_hosts: Vec<String> = std::iter::once("localhost".to_string())
        .chain(config.allowed_hosts.iter().cloned())
        .collect();
    let listener = runtime.block_on(TcpListener::bind(config.listen))?;
    let listening = listener.local_addr()?;
    log::info!(
        "dashboard: listening on http://{listening} (websocket at /websocket), reading the \
         validator whose ledger is {}",
        config.host.ledger.display()
    );
    runtime.spawn(server::serve(listener, handles, allowed_hosts.into()));

    if let Some(address) = config.metrics_listen {
        let metrics = runtime.block_on(TcpListener::bind(address))?;
        log::info!(
            "dashboard: receiving the validator's metrics on http://{}",
            metrics.local_addr()?
        );
        runtime.spawn(metrics_listener::serve(
            metrics,
            metrics_tap.clone(),
            config.metrics_forward.clone(),
        ));
    }

    let (sender, received) = crossbeam_channel::bounded(FRAMES);
    let frames_dropped = Arc::new(AtomicU64::new(0));
    let frames = Frames {
        sender,
        dropped: frames_dropped.clone(),
    };
    ledger::spawn(
        config.host.ledger.clone(),
        remote.clone(),
        replies.clone(),
        publisher.clone(),
        metrics_tap.clone(),
        frames.clone(),
    )?;
    let mut feed = slot_feed::SlotFeed::new(
        publisher.clone(),
        history,
        metrics_tap.clone(),
        remote.clone(),
        replies,
        received,
        frames_dropped,
        epochs,
        info_cache.clone(),
        tip_program.map(|program| TipMeter::new(&program)),
    );
    thread::Builder::new()
        .name("solDashSaSlots".to_string())
        .spawn(move || {
            loop {
                let started = Instant::now();
                feed.tick();
                thread::sleep(slot_feed::TICK.saturating_sub(started.elapsed()));
            }
        })?;

    poll::publish_static(&publisher, &config.statics);
    runtime.spawn(poll::admin(
        Arc::new(AdminClient::new(config.host.ledger.join("admin.rpc"))),
        remote.clone(),
        publisher.clone(),
        metrics_tap.clone(),
    ));
    remote.write().unwrap().allow_private_addr = config.allow_private_addr;
    runtime.spawn(poll::rpc(
        config.rpc_url.clone(),
        remote.clone(),
        publisher.clone(),
        config
            .statics
            .vote_account
            .as_deref()
            .and_then(|key| key.parse().ok()),
        config.host.ledger.clone(),
    ));
    runtime.spawn(poll::tip_writes(
        config.rpc_url.clone(),
        remote.clone(),
        frames,
    ));
    runtime.spawn(poll::snapshots(
        config.host.snapshot_config.clone(),
        remote.clone(),
        publisher.clone(),
    ));

    let failed_total = Arc::new(AtomicU64::new(FAILED_UNREAD));
    runtime.spawn(poll::slot_updates(
        config.rpc_url.clone(),
        remote.clone(),
        failed_total.clone(),
    ));
    runtime.spawn(poll::validator_names(remote.clone(), info_cache));
    thread::Builder::new()
        .name("solDashSaMeter".to_string())
        .spawn(move || meters(remote, config.host, publisher, metrics_tap, failed_total))?;

    runtime.block_on(async {
        let _ = tokio::signal::ctrl_c().await;
    });
    log::info!("dashboard: stopping");
    Ok(())
}

/// The meters for the validator process the admin socket names, built again when another one
/// answers there.
fn meters(
    remote: Shared,
    host_paths: HostPaths,
    publisher: Arc<Publisher>,
    metrics_tap: Arc<MetricsTap>,
    failed_total: Arc<AtomicU64>,
) {
    loop {
        let Some(pid) = remote.read().unwrap().pid else {
            thread::sleep(PID_WAIT);
            continue;
        };
        let mut meters = Meters::new(
            remote.clone(),
            host_paths.clone(),
            pid,
            publisher.clone(),
            metrics_tap.clone(),
            failed_total.clone(),
        );
        while remote.read().unwrap().pid == Some(pid) {
            let started = Instant::now();
            meters.tick();
            thread::sleep(METER_INTERVAL.saturating_sub(started.elapsed()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intervals(args: &[&str]) -> (SnapshotInterval, SnapshotInterval) {
        let mut argv = vec!["agave-dashboard", "--ledger", "/ledger"];
        argv.extend_from_slice(args);
        snapshot_intervals(&cli().get_matches_from(argv))
    }

    fn slots(slots: u64) -> SnapshotInterval {
        SnapshotInterval::Slots(NonZeroU64::new(slots).unwrap())
    }

    #[test]
    fn test_snapshot_intervals_read_as_the_validator_reads_them() {
        assert_eq!(
            intervals(&[]),
            (
                SnapshotInterval::Slots(DEFAULT_FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS),
                SnapshotInterval::Slots(DEFAULT_INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS)
            )
        );
        assert_eq!(
            intervals(&[
                "--full-snapshot-interval-slots",
                "5000",
                "--snapshot-interval-slots",
                "300"
            ]),
            (slots(5000), slots(300))
        );
        assert_eq!(
            intervals(&[
                "--snapshot-interval-slots",
                "200",
                "--no-incremental-snapshots"
            ]),
            (slots(200), SnapshotInterval::Disabled)
        );
        assert_eq!(
            intervals(&["--no-snapshots"]),
            (SnapshotInterval::Disabled, SnapshotInterval::Disabled)
        );
    }

    #[test]
    fn test_a_value_that_does_not_parse_is_refused() {
        for (flag, value) in [
            ("--snapshot-interval-slots", "0"),
            ("--full-snapshot-interval-slots", "many"),
            ("--vote-account", "not-a-key"),
            ("--commission-bps", "70000"),
        ] {
            let argv = ["agave-dashboard", "--ledger", "/ledger", flag, value];
            let refused = cli().get_matches_from_safe(argv).unwrap_err();
            assert_eq!(refused.kind, clap::ErrorKind::ValueValidation, "{flag}");
        }
    }
}
