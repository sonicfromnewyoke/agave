//! The pollers that fill the dashboard's feed: the validator's admin socket, its JSON-RPC and its
//! snapshot archive directories.

use {
    crate::{
        admin::{AdminClient, AdminContactInfo, AdminError},
        collect::{GossipAddress, cluster_name, earnings::VOTE_REWARD_ACCOUNT, system_time_nanos},
        frames::{Frame, Frames},
        geoip,
        meter_source::IngestAddresses,
        metrics_tap::{MetricsTap, insert_capped},
        proto::{Debounced, Publisher, TOPIC_SUMMARY},
        remote::{Contact, EpochLeaders, Shared, VoteAccounts, VoteReading},
        snapshot::{self, SnapshotTracker, Snapshots},
        startup::StartupPublisher,
        tips::{JITO_CUT_BPS, TipRates},
        validator_info::{self, ValidatorInfoCache},
    },
    agave_feature_set::FEATURE_NAMES,
    agave_snapshots::snapshot_config::SnapshotConfig,
    futures_util::{StreamExt, stream::select_all},
    solana_account::AccountSharedData,
    solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig},
    solana_clock::{Clock, Epoch, Slot},
    solana_cluster_type::ClusterType,
    solana_commitment_config::CommitmentConfig,
    solana_core::validator::ValidatorStartProgress,
    solana_feature_gate_interface::Feature,
    solana_genesis_config::GenesisConfig,
    solana_message::Message,
    solana_pubkey::Pubkey,
    solana_pubsub_client::nonblocking::pubsub_client::PubsubClient,
    solana_rpc_client::{
        api::{
            client_error::Result as ClientResult,
            config::{RpcAccountInfoConfig, RpcGetVoteAccountsConfig, RpcLeaderScheduleConfig},
            request::{MAX_MULTIPLE_ACCOUNTS, RpcRequest},
        },
        nonblocking::rpc_client::RpcClient,
    },
    solana_rpc_client_api::response::{
        RpcContactInfo, RpcKeyedAccount, RpcVoteAccountInfo, SlotUpdate,
    },
    solana_runtime::epoch_stakes::BLSPubkeyToRankMap,
    solana_sdk_ids::{config, sysvar},
    solana_slot_history::SlotHistory as SlotHistorySysvar,
    solana_time_utils::timestamp,
    solana_vote::vote_account::{VoteAccount, VoteAccountsHashMap},
    solana_vote_interface::state::VoteStateV4,
    std::{
        collections::{BTreeMap, HashMap},
        path::{Path, PathBuf},
        str::FromStr,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{Duration, Instant, SystemTime},
    },
    tokio::time::sleep,
};

const STARTUP_POLL: Duration = Duration::from_millis(250);
/// The admin socket's other answers change only on a restart or an identity change.
const ADMIN_SLOW: Duration = Duration::from_secs(2);
const RPC_POLL: Duration = Duration::from_secs(1);
/// Every vote account, the leader schedules and rank maps still missing, and the epoch's own reads
/// once a new epoch begins.
const RPC_SLOW: Duration = Duration::from_secs(5);
/// The gossip table is read whole, this often.
const CONTACTS_READ: Duration = Duration::from_secs(10);
/// After subscribing to the tip accounts, for the confirmed bank to pass the subscriptions' start:
/// the balances read at it then miss no write the subscriptions did not carry.
const TIP_BASE_WAIT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// Slots of fork history the failed totals keep.
const FORK_SLOTS: usize = 1024;
const SNAPSHOT_READ: Duration = Duration::from_secs(1);
/// Validator info is read whole, this often.
const NAMES_READ: Duration = Duration::from_secs(60);

/// What the dashboard is told on its command line, published once.
pub struct Static {
    pub vote_account: Option<String>,
    /// Tip rates are published where a tip payment program is named.
    pub tip_payment_program: Option<String>,
    pub commission_bps: Option<u16>,
}

pub fn publish_static(publisher: &Publisher, config: &Static) {
    if let Some(vote_account) = &config.vote_account {
        publisher.publish(TOPIC_SUMMARY, "vote_key", vote_account);
    }
    if config.tip_payment_program.is_some() {
        publisher.publish(
            TOPIC_SUMMARY,
            "tip_rates",
            &TipRates {
                jito_cut_bps: JITO_CUT_BPS,
                commission_bps: config.commission_bps,
            },
        );
    }
}

debounced_keys! {
    struct AdminKeys {
        startup_time_nanos: u64 = (TOPIC_SUMMARY, "startup_time_nanos"),
        shred_version: u16 = (TOPIC_SUMMARY, "shred_version"),
        gossip_address: Option<GossipAddress> = (TOPIC_SUMMARY, "gossip_address"),
    }
}

/// Startup progress every [`STARTUP_POLL`], and the socket's other answers every [`ADMIN_SLOW`].
pub async fn admin(
    client: Arc<AdminClient>,
    remote: Shared,
    publisher: Arc<Publisher>,
    tap: Arc<MetricsTap>,
) {
    let mut startup = StartupPublisher::default();
    let mut keys = AdminKeys::default();
    let mut slow_at: Option<Instant> = None;
    let mut reachable: Option<bool> = None;
    loop {
        match client.call::<ValidatorStartProgress>("startProgress").await {
            Ok(progress) => {
                if reachable != Some(true) {
                    log::info!(
                        "dashboard: admin socket {} answers",
                        client.path().display()
                    );
                    reachable = Some(true);
                }
                remote.write().unwrap().progress = Some(progress);
                startup.publish(&publisher, progress, tap.stake_in_gossip());
                if slow_at.is_none_or(|at| at.elapsed() >= ADMIN_SLOW) {
                    slow_at = Some(Instant::now());
                    if let Err(err) = poll_admin(&client, &remote, &publisher, &mut keys).await {
                        log::debug!("dashboard: admin socket: {err}");
                    }
                }
            }
            Err(err) => {
                if reachable != Some(false) {
                    log::warn!(
                        "dashboard: admin socket {} does not answer: {err}",
                        client.path().display()
                    );
                    reachable = Some(false);
                }
                remote.write().unwrap().progress = None;
            }
        }
        sleep(STARTUP_POLL).await;
    }
}

async fn poll_admin(
    client: &AdminClient,
    remote: &Shared,
    publisher: &Publisher,
    keys: &mut AdminKeys,
) -> Result<(), AdminError> {
    let pid: u32 = client.call("pid").await?;
    let started: SystemTime = client.call("startTime").await?;
    let rpc_address: Option<std::net::SocketAddr> = client.call("rpcAddress").await?;
    // Refused until startup is complete.
    let contact = client.call::<AdminContactInfo>("contactInfo").await.ok();
    {
        let mut remote = remote.write().unwrap();
        if remote.pid != Some(pid) {
            log::info!("dashboard: the validator runs as process {pid}");
        }
        remote.pid = Some(pid);
        remote.started = Some(started);
        remote.rpc_address = rpc_address;
        if contact.is_some() {
            remote.contact = contact.clone();
        }
    }
    keys.startup_time_nanos
        .publish(publisher, system_time_nanos(started));
    if let Some(contact) = &contact {
        keys.shred_version.publish(publisher, contact.shred_version);
        let address = contact.ingest_addresses().gossip.map(|addr| GossipAddress {
            ip: addr.ip().to_string(),
            country: geoip::country(addr.ip()),
        });
        keys.gossip_address.publish(publisher, address);
    }
    Ok(())
}

debounced_keys! {
    struct RpcKeys {
        version: String = (TOPIC_SUMMARY, "version"),
        cluster: &'static str = (TOPIC_SUMMARY, "cluster"),
        identity_key: String = (TOPIC_SUMMARY, "identity_key"),
    }
}

/// When the slower reads last ran, and what the epoch's reads were of.
#[derive(Default)]
struct Reads {
    slow_at: Option<Instant>,
    contacts_at: Option<Instant>,
    /// The epoch, and whether under alpenglow.
    epoch: Option<(Epoch, bool)>,
}

/// The newest frozen block the ledger gave, else its root, and the slot's epoch.
fn frozen_now(remote: &Shared) -> Option<(Slot, Epoch)> {
    let held = remote.read().unwrap();
    let slot = held
        .working
        .map(|working| working.slot)
        .or(held.finalized)?;
    Some((slot, held.epoch_schedule.as_ref()?.get_epoch(slot)))
}

/// The RPC node named on the command line, else the validator's own, at the address its admin
/// socket gives once it has.
fn rpc_url(named: &Option<String>, remote: &Shared) -> Option<String> {
    named.clone().or_else(|| {
        let address = remote.read().unwrap().rpc_address?;
        Some(format!("http://{address}"))
    })
}

/// An RPC node's pubsub: by agave's convention at the port after RPC's, where the address names
/// one.
fn pubsub_url(rpc_url: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(rpc_url).ok()?;
    let scheme = match url.scheme() {
        "https" => "wss",
        _ => "ws",
    };
    if let Some(port) = url.port() {
        url.set_port(port.checked_add(1)).ok()?;
    }
    url.set_scheme(scheme).ok()?;
    Some(url.to_string())
}

/// The chain's state and the gossip table, from RPC.
pub async fn rpc(
    url: Option<String>,
    remote: Shared,
    publisher: Arc<Publisher>,
    vote_account: Option<Pubkey>,
    ledger: PathBuf,
) {
    let mut client: Option<(String, RpcClient)> = None;
    let mut keys = RpcKeys::default();
    let mut reads = Reads::default();
    let mut reachable: Option<bool> = None;
    loop {
        if let Some(wanted) = rpc_url(&url, &remote)
            && client.as_ref().is_none_or(|(held, _)| *held != wanted)
        {
            log::info!("dashboard: reading the validator's RPC at {wanted}");
            client = Some((
                wanted.clone(),
                RpcClient::new_with_timeout(wanted, RPC_TIMEOUT),
            ));
            reads.slow_at = None;
        }
        if let Some((held, rpc)) = &client {
            match poll_rpc(
                rpc,
                &remote,
                &publisher,
                &mut keys,
                &mut reads,
                vote_account,
                &ledger,
            )
            .await
            {
                Ok(()) if reachable != Some(true) => {
                    log::info!("dashboard: RPC at {held} answers");
                    reachable = Some(true);
                }
                Err(err) if reachable != Some(false) => {
                    log::warn!("dashboard: RPC at {held} does not answer: {err}");
                    reachable = Some(false);
                }
                _ => {}
            }
        }
        sleep(RPC_POLL).await;
    }
}

async fn poll_rpc(
    rpc: &RpcClient,
    remote: &Shared,
    publisher: &Publisher,
    keys: &mut RpcKeys,
    reads: &mut Reads,
    vote_account: Option<Pubkey>,
    ledger: &Path,
) -> ClientResult<()> {
    let slow = reads.slow_at.is_none_or(|at| at.elapsed() >= RPC_SLOW);
    // The ledger's genesis names the cluster, the slot time and the epochs.
    if slow
        && remote.read().unwrap().genesis_nanos.is_none()
        && let Ok(genesis) = GenesisConfig::load(ledger)
    {
        keys.cluster
            .publish(publisher, cluster_of(&genesis.hash().to_string()));
        let mut held = remote.write().unwrap();
        held.genesis_nanos = u64::try_from(genesis.ns_per_slot()).ok();
        held.epoch_schedule = Some(genesis.epoch_schedule.clone());
    }
    read_slot_history(rpc, remote).await?;
    read_vote(rpc, remote, vote_account).await?;
    if slow {
        reads.slow_at = Some(Instant::now());
        // The admin socket names the node once startup is complete; RPC answers before.
        let named = remote
            .read()
            .unwrap()
            .contact
            .as_ref()
            .map(|contact| contact.id.parse());
        let identity = match named {
            Some(Ok(identity)) => identity,
            _ => rpc.get_identity().await?,
        };
        keys.identity_key.publish(publisher, identity.to_string());
        remote.write().unwrap().identity = Some(identity);
        poll_leader_slots(rpc, remote).await?;
        read_cluster(rpc, remote).await?;
        read_epoch(rpc, remote, &mut reads.epoch).await?;
    }
    if reads
        .contacts_at
        .is_none_or(|at| at.elapsed() >= CONTACTS_READ)
    {
        reads.contacts_at = Some(Instant::now());
        let table = gossip_table(rpc.get_cluster_nodes().await?);
        let mut held = remote.write().unwrap();
        // Ours as the table names it: the RPC node may be another.
        if let Some(ours) = held.identity.and_then(|identity| table.get(&identity)) {
            keys.version.publish(publisher, ours.version.clone());
            held.ingest = Some(ours.addresses);
        }
        held.gossip = table;
    }
    Ok(())
}

fn gossip_table(nodes: Vec<RpcContactInfo>) -> HashMap<Pubkey, Contact> {
    let contact = |node: RpcContactInfo| {
        let contact = Contact {
            shred_version: node.shred_version.unwrap_or(0),
            version: node.version.unwrap_or_default(),
            client: node.client_id.unwrap_or_default(),
            addresses: IngestAddresses {
                tvu_udp: node.tvu,
                tpu_quic: node.tpu_quic,
                tpu_forwards_quic: node.tpu_forwards_quic,
                tpu_vote_udp: node.tpu_vote,
                gossip: node.gossip,
                serve_repair_udp: node.serve_repair,
            },
            rpc: node.rpc,
        };
        Some((Pubkey::from_str(&node.pubkey).ok()?, contact))
    };
    nodes.into_iter().filter_map(contact).collect()
}

/// Our identity and vote account in one read of RPC's processed bank, with the ledger's root as it
/// stood before the read.
async fn read_vote(
    rpc: &RpcClient,
    remote: &Shared,
    vote_account: Option<Pubkey>,
) -> ClientResult<()> {
    let (identity, root) = {
        let held = remote.read().unwrap();
        (held.identity, held.finalized)
    };
    let (Some(identity), Some(root)) = (identity, root) else {
        remote.write().unwrap().vote = None;
        return Ok(());
    };
    let keys: Vec<Pubkey> = std::iter::once(identity).chain(vote_account).collect();
    let mut read = rpc
        .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::processed())
        .await?
        .value
        .into_iter();
    let identity_balance = read.next().flatten().map_or(0, |account| account.lamports);
    let account = read.next().flatten();
    remote.write().unwrap().vote = Some(VoteReading {
        root,
        identity_balance,
        vote_balance: account.as_ref().map_or(0, |account| account.lamports),
        vote_pubkey: vote_account,
        account: account
            .and_then(|account| VoteAccount::try_from(AccountSharedData::from(account)).ok()),
    });
    Ok(())
}

/// As the bank builds one from its epoch stakes.
async fn read_rank_map(
    rpc: &RpcClient,
    rows: &[RpcVoteAccountInfo],
) -> ClientResult<Arc<BLSPubkeyToRankMap>> {
    let staked: Vec<(Pubkey, u64)> = rows
        .iter()
        .filter(|row| row.activated_stake > 0)
        .filter_map(|row| Some((row.vote_pubkey.parse().ok()?, row.activated_stake)))
        .collect();
    let mut accounts = VoteAccountsHashMap::default();
    for chunk in staked.chunks(MAX_MULTIPLE_ACCOUNTS) {
        let keys: Vec<Pubkey> = chunk.iter().map(|(key, _)| *key).collect();
        let read = rpc
            .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::processed())
            .await?
            .value;
        for ((key, stake), account) in chunk.iter().zip(read) {
            if let Some(account) = account
                .and_then(|account| VoteAccount::try_from(AccountSharedData::from(account)).ok())
            {
                accounts.insert(*key, (*stake, account));
            }
        }
    }
    Ok(Arc::new(BLSPubkeyToRankMap::new(&accounts)))
}

/// Every vote account, judged at the newest frozen block as the read begins, and whether the
/// cluster runs alpenglow, which it keeps once it does.
async fn read_cluster(rpc: &RpcClient, remote: &Shared) -> ClientResult<()> {
    let Some((slot, epoch)) = frozen_now(remote) else {
        return Ok(());
    };
    let accounts = rpc
        .get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
            commitment: Some(CommitmentConfig::processed()),
            ..RpcGetVoteAccountsConfig::default()
        })
        .await?;
    let known = remote.read().unwrap().alpenglow == Some(true);
    let alpenglow = known || rpc.get_ag_genesis_cert().await?.is_some();
    let rows: Vec<_> = accounts
        .current
        .into_iter()
        .chain(accounts.delinquent)
        .collect();
    // The bank takes an epoch's stakes at the start of the epoch before: the next epoch's map is
    // exact once read early in this one, and this one's where the stakes have not moved since.
    // Votor reports an epoch's vote arrivals up to two epochs on.
    if alpenglow {
        for wanted in [epoch, epoch.saturating_add(1)] {
            if !remote.read().unwrap().rank_maps.contains_key(&wanted) {
                let map = read_rank_map(rpc, &rows).await?;
                let mut held = remote.write().unwrap();
                held.rank_maps.insert(wanted, map);
                held.rank_maps = held.rank_maps.split_off(&epoch.saturating_sub(2));
            }
        }
    }
    let mut held = remote.write().unwrap();
    held.vote_accounts = Some(Arc::new(VoteAccounts { slot, rows }));
    held.alpenglow = Some(alpenglow);
    Ok(())
}

/// What changes only as an epoch begins, read once the newest frozen block is in it: feature
/// activations, rent and the signature fee, the epoch's inflation under alpenglow, and the cluster
/// clock's stamp of the epoch's start.
async fn read_epoch(
    rpc: &RpcClient,
    remote: &Shared,
    read: &mut Option<(Epoch, bool)>,
) -> ClientResult<()> {
    let (Some((_, epoch)), Some(alpenglow)) =
        (frozen_now(remote), remote.read().unwrap().alpenglow)
    else {
        return Ok(());
    };
    if *read == Some((epoch, alpenglow)) {
        return Ok(());
    }
    let processed = CommitmentConfig::processed();
    let rent = rpc
        .get_minimum_balance_for_rent_exemption(VoteStateV4::size_of())
        .await?;
    // Every feature, for the fee payers' priority fees; the slot time gates are among them.
    let ids: Vec<Pubkey> = FEATURE_NAMES.keys().copied().collect();
    let mut features = HashMap::new();
    for chunk in ids.chunks(MAX_MULTIPLE_ACCOUNTS) {
        let read = rpc
            .get_multiple_accounts_with_commitment(chunk, processed)
            .await?
            .value;
        features.extend(chunk.iter().zip(read).filter_map(|(id, account)| {
            let feature: Feature = bincode::deserialize(&account?.data).ok()?;
            Some((*id, feature.activated_at?))
        }));
    }
    let blockhash = rpc.get_latest_blockhash().await?;
    let payer = Pubkey::new_from_array([1; 32]);
    let fee = rpc
        .get_fee_for_message(&Message::new_with_blockhash(&[], Some(&payer), &blockhash))
        .await?;
    let vote_reward = match alpenglow {
        true => rpc
            .get_account_with_commitment(&VOTE_REWARD_ACCOUNT, processed)
            .await?
            .value
            .map(|account| account.data),
        false => None,
    };
    // Of the bank's own epoch, which at a boundary can be the next one: the countdown checks.
    let clock = rpc
        .get_account_with_commitment(&sysvar::clock::id(), processed)
        .await?
        .value
        .and_then(|account| bincode::deserialize::<Clock>(&account.data).ok());
    let mut held = remote.write().unwrap();
    held.vote_reward = vote_reward;
    held.vote_state_rent = Some(rent);
    held.lamports_per_signature = Some(fee);
    held.features = features;
    held.epoch_start = clock.map(|clock| (clock.epoch, clock.epoch_start_timestamp));
    *read = Some((epoch, alpenglow));
    Ok(())
}

/// The SlotHistory sysvar, from the newest bank, while the slot collector has slots below the root
/// it cannot place and the last read does not cover them. It is a 128 KiB bitmap, so it is read
/// only then.
async fn read_slot_history(rpc: &RpcClient, remote: &Shared) -> ClientResult<()> {
    let stale = {
        let held = remote.read().unwrap();
        held.slot_history_wanted.is_some_and(|wanted| {
            held.slot_history
                .as_ref()
                .is_none_or(|history| history.next_slot <= wanted)
        })
    };
    if !stale {
        return Ok(());
    }
    let started = Instant::now();
    let Some(account) = rpc
        .get_account_with_commitment(
            &solana_sdk_ids::sysvar::slot_history::id(),
            CommitmentConfig::processed(),
        )
        .await?
        .value
    else {
        return Ok(());
    };
    match bincode::deserialize::<SlotHistorySysvar>(&account.data) {
        Ok(history) => {
            // The first read is logged for its size and time (check C7 of the plan); the rest
            // only at debug.
            static READ_BEFORE: AtomicBool = AtomicBool::new(false);
            let level = if READ_BEFORE.swap(true, Ordering::Relaxed) {
                log::Level::Debug
            } else {
                log::Level::Info
            };
            log::log!(
                level,
                "dashboard: read the SlotHistory sysvar ({} bytes, up to slot {}) in {} ms",
                account.data.len(),
                history.next_slot.saturating_sub(1),
                started.elapsed().as_millis()
            );
            remote.write().unwrap().slot_history = Some(Arc::new(history));
        }
        Err(err) => log::warn!("dashboard: the SlotHistory sysvar does not decode: {err}"),
    }
    Ok(())
}

/// Every slot's leader in the previous epoch, this one and the next, once each epoch's schedule is
/// known. The epoch schedule is the cluster's and read once, where genesis did not give it.
async fn poll_leader_slots(rpc: &RpcClient, remote: &Shared) -> ClientResult<()> {
    // Read before the match, so the guard is not held across the await.
    let held = remote.read().unwrap().epoch_schedule.clone();
    let schedule = match held {
        Some(schedule) => schedule,
        None => {
            let schedule = rpc.get_epoch_schedule().await?;
            remote.write().unwrap().epoch_schedule = Some(schedule.clone());
            schedule
        }
    };
    let Some((_, epoch)) = frozen_now(remote) else {
        return Ok(());
    };
    {
        let mut held = remote.write().unwrap();
        // The previous epoch's stay for the slots the strip still shows.
        held.schedules = held.schedules.split_off(&epoch.saturating_sub(1));
    }
    for wanted in [epoch.saturating_sub(1), epoch, epoch.saturating_add(1)] {
        if remote.read().unwrap().schedules.contains_key(&wanted) {
            continue;
        }
        let first = schedule.get_first_slot_in_epoch(wanted);
        // None until the schedule is derived, and for an epoch the bank no longer holds.
        let Some(leaders) = rpc
            .get_leader_schedule_with_config(Some(first), RpcLeaderScheduleConfig::default())
            .await?
        else {
            continue;
        };
        let Some(leaders) =
            EpochLeaders::from_indices(first, schedule.get_slots_in_epoch(wanted), &leaders)
        else {
            log::warn!(
                "dashboard: the leader schedule of epoch {wanted} does not name one leader for \
                 each of its slots; it is not used"
            );
            continue;
        };
        remote
            .write()
            .unwrap()
            .schedules
            .insert(wanted, Arc::new(leaders));
    }
    Ok(())
}

/// The cluster a genesis hash names; any other is a development cluster.
fn cluster_of(genesis: &str) -> &'static str {
    let cluster = [
        ClusterType::MainnetBeta,
        ClusterType::Testnet,
        ClusterType::Devnet,
    ]
    .into_iter()
    .find(|cluster| {
        cluster
            .get_genesis_hash()
            .is_some_and(|hash| hash.to_string() == genesis)
    })
    .unwrap_or(ClusterType::Development);
    cluster_name(cluster)
}

/// RPC's slot updates: each frozen bank's counts, and its failed transactions summed along its
/// fork from the first bank seen on it, read at the newest bank frozen.
pub async fn slot_updates(url: Option<String>, remote: Shared, failed_total: Arc<AtomicU64>) {
    let mut parents: BTreeMap<Slot, Slot> = BTreeMap::new();
    let mut totals: BTreeMap<Slot, u64> = BTreeMap::new();
    loop {
        sleep(RPC_POLL).await;
        let Some(pubsub) = rpc_url(&url, &remote).and_then(|rpc| pubsub_url(&rpc)) else {
            continue;
        };
        let Ok(client) = PubsubClient::new(pubsub.as_str()).await else {
            continue;
        };
        let Ok((mut updates, _unsubscribe)) = client.slot_updates_subscribe().await else {
            continue;
        };
        while let Some(update) = updates.next().await {
            match update {
                SlotUpdate::CreatedBank { slot, parent, .. } => {
                    insert_capped(&mut parents, slot, parent, FORK_SLOTS);
                }
                SlotUpdate::Frozen { slot, stats, .. } => {
                    let before = parents
                        .get(&slot)
                        .filter(|parent| **parent < slot)
                        .and_then(|parent| totals.get(parent))
                        .copied()
                        .unwrap_or(0);
                    let total = before.saturating_add(stats.num_failed_transactions);
                    insert_capped(&mut totals, slot, total, FORK_SLOTS);
                    let mut remote = remote.write().unwrap();
                    insert_capped(&mut remote.frozen, slot, stats, FORK_SLOTS);
                    if remote.alpenglow == Some(true)
                        && totals
                            .last_key_value()
                            .is_some_and(|(newest, _)| *newest == slot)
                    {
                        failed_total.store(total, Ordering::Relaxed);
                    }
                }
                _ => {}
            }
        }
    }
}

/// Jito's tip accounts, from RPC's account subscriptions: each change of a balance, with the bank
/// it was seen in. The balances the changes count from are read once the subscriptions run, and
/// again after each reconnection, which may have missed some.
pub async fn tip_writes(url: Option<String>, remote: Shared, frames: Frames) {
    let accounts = remote.read().unwrap().tip_accounts.clone();
    if accounts.is_empty() {
        return;
    }
    let config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: Some(UiDataSliceConfig {
            offset: 0,
            length: 0,
        }),
        commitment: Some(CommitmentConfig::processed()),
        min_context_slot: None,
    };
    loop {
        sleep(RPC_POLL).await;
        remote.write().unwrap().tip_base = None;
        let Some(rpc) = rpc_url(&url, &remote) else {
            continue;
        };
        let Some(pubsub) = pubsub_url(&rpc) else {
            continue;
        };
        let Ok(client) = PubsubClient::new(pubsub.as_str()).await else {
            continue;
        };
        let mut streams = Vec::new();
        for account in &accounts {
            let Ok((updates, _unsubscribe)) = client
                .account_subscribe(account, Some(config.clone()))
                .await
            else {
                break;
            };
            streams.push(updates.map(move |update| (*account, update)));
        }
        if streams.len() != accounts.len() {
            continue;
        }
        let mut updates = select_all(streams);
        let base = async {
            sleep(TIP_BASE_WAIT).await;
            RpcClient::new_with_timeout(rpc, RPC_TIMEOUT)
                .get_multiple_accounts_with_commitment(&accounts, CommitmentConfig::confirmed())
                .await
        };
        tokio::pin!(base);
        let mut based = false;
        loop {
            tokio::select! {
                read = &mut base, if !based => {
                    let Ok(read) = read else {
                        break;
                    };
                    based = true;
                    let balances = read
                        .value
                        .iter()
                        .map(|account| account.as_ref().map_or(0, |account| account.lamports))
                        .collect();
                    remote.write().unwrap().tip_base = Some((read.context.slot, balances));
                }
                update = updates.next() => {
                    let Some((account, update)) = update else {
                        break;
                    };
                    frames.send(Frame::Account {
                        slot: update.context.slot,
                        pubkey: account,
                        lamports: update.value.lamports,
                    });
                }
            }
        }
    }
}

/// Validator names, from the config program's accounts. Only where the validator indexes them by
/// program: RPC would otherwise read every account.
pub async fn validator_names(remote: Shared, info_cache: Arc<RwLock<ValidatorInfoCache>>) {
    let mut indexed = None;
    loop {
        let (address, pid) = {
            let held = remote.read().unwrap();
            (held.rpc_address, held.pid)
        };
        let Some((address, pid)) = address.zip(pid) else {
            sleep(RPC_POLL).await;
            continue;
        };
        if indexed.is_none_or(|(held, _)| held != pid) {
            let program_indexed = config_indexed(pid);
            if !program_indexed {
                log::info!(
                    "dashboard: the validator does not index the config program, so validator \
                     names are unavailable and the dashboard will show pubkeys"
                );
            }
            indexed = Some((pid, program_indexed));
        }
        if indexed.is_some_and(|(_, program_indexed)| program_indexed) {
            let rpc = RpcClient::new(format!("http://{address}"));
            let read = rpc.send::<Vec<RpcKeyedAccount>>(
                RpcRequest::GetProgramAccounts,
                serde_json::json!([
                    config::id().to_string(),
                    {"encoding": "base64", "commitment": "processed"},
                ]),
            );
            match read.await {
                Ok(read) => {
                    let table = read
                        .iter()
                        .filter_map(|keyed| validator_info::parse(&keyed.account.data.decode()?))
                        .collect();
                    info_cache.write().unwrap().replace(table);
                }
                Err(err) => log::warn!("dashboard: could not read validator info: {err}"),
            }
        }
        sleep(NAMES_READ).await;
    }
}

fn config_indexed(pid: u32) -> bool {
    let Ok(line) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
        return false;
    };
    let args: Vec<String> = line
        .split(|byte| *byte == 0)
        .map(|arg| String::from_utf8_lossy(arg).into_owned())
        .collect();
    indexes_config(&args)
}

/// As the validator reads `--account-index program-id` and its include or exclude keys.
fn indexes_config(args: &[String]) -> bool {
    let values = |flag: &str| -> Vec<String> {
        let mut values = Vec::new();
        let mut taking = false;
        for arg in args {
            if let Some(value) = arg
                .strip_prefix(flag)
                .and_then(|rest| rest.strip_prefix('='))
            {
                values.push(value.to_string());
                taking = false;
            } else if arg == flag {
                taking = true;
            } else if arg.starts_with('-') {
                taking = false;
            } else if taking {
                values.push(arg.clone());
            }
        }
        values
    };
    let program = config::id().to_string();
    let included = values("--account-index-include-key");
    values("--account-index")
        .iter()
        .any(|index| index == "program-id")
        && (included.is_empty() || included.contains(&program))
        && !values("--account-index-exclude-key").contains(&program)
}

/// The archive directories every [`SNAPSHOT_READ`]; the newest frozen block stands for the
/// completed slot the write's lag is counted against.
pub async fn snapshots(config: SnapshotConfig, remote: Shared, publisher: Arc<Publisher>) {
    let mut tracker = SnapshotTracker::default();
    let mut published = Debounced::<Option<Snapshots>>::new(TOPIC_SUMMARY, "snapshots");
    loop {
        let completed = remote
            .read()
            .unwrap()
            .working
            .map_or(0, |working| working.slot);
        let read = snapshot::read(&config);
        if let Some(behind) = remote.read().unwrap().behind_cluster {
            tracker.note_behind(behind);
        }
        published.publish(&publisher, tracker.observe(read, completed, timestamp()));
        remote.write().unwrap().snapshot_spans = tracker.spans().collect();
        sleep(SNAPSHOT_READ).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(pubkey: &Pubkey, version: &str) -> RpcContactInfo {
        serde_json::from_value(serde_json::json!({
            "pubkey": pubkey.to_string(),
            "gossip": "10.0.0.1:8001",
            "tvu": "10.0.0.1:8002",
            "tpuQuic": "10.0.0.1:8009",
            "rpc": "10.0.0.1:8899",
            "version": version,
            "clientId": "Agave",
            "shredVersion": 7,
        }))
        .unwrap()
    }

    #[test]
    fn test_the_gossip_table_is_read_whole_by_identity() {
        let (ours, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let table = gossip_table(vec![node(&ours, "4.4.1"), node(&other, "4.3.0")]);
        assert_eq!(table.len(), 2);
        let first = &table[&ours];
        assert_eq!(first.version, "4.4.1");
        assert_eq!((first.client.as_str(), first.shred_version), ("Agave", 7));
        assert_eq!(first.rpc, "10.0.0.1:8899".parse().ok());
        assert_eq!(first.addresses.tpu_vote_udp, None);
        assert_eq!(table[&other].version, "4.3.0");
    }

    #[test]
    fn test_pubsub_is_at_the_port_after_rpc_where_one_is_named() {
        assert_eq!(
            pubsub_url("http://127.0.0.1:8899").as_deref(),
            Some("ws://127.0.0.1:8900/")
        );
        assert_eq!(
            pubsub_url("https://rpc.example.org/path").as_deref(),
            Some("wss://rpc.example.org/path")
        );
        assert_eq!(pubsub_url("not a url"), None);
    }

    #[test]
    fn test_known_genesis_hashes_name_their_cluster() {
        assert_eq!(
            cluster_of("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"),
            "mainnet-beta"
        );
        assert_eq!(
            cluster_of("4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY"),
            "testnet"
        );
        assert_eq!(
            cluster_of("EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"),
            "devnet"
        );
        assert_eq!(
            cluster_of("11111111111111111111111111111111"),
            "development"
        );
    }

    #[test]
    fn test_names_are_read_only_where_the_config_program_is_indexed() {
        let program = config::id().to_string();
        let other = Pubkey::new_unique().to_string();
        let indexes = |line: &[&str]| {
            let args: Vec<String> = line.iter().map(|arg| arg.to_string()).collect();
            indexes_config(&args)
        };
        assert!(!indexes(&["agave-validator", "--ledger", "l"]));
        assert!(indexes(&[
            "agave-validator",
            "--account-index",
            "program-id"
        ]));
        assert!(indexes(&[
            "--account-index",
            "spl-token-owner",
            "program-id",
            "--ledger",
            "l"
        ]));
        assert!(indexes(&["--account-index=program-id"]));
        assert!(!indexes(&[
            "--account-index",
            "spl-token-mint",
            "--ledger",
            "program-id"
        ]));
        let include = [
            "--account-index",
            "program-id",
            "--account-index-include-key",
        ];
        assert!(indexes(&[&include[..], &[program.as_str()]].concat()));
        assert!(!indexes(&[&include[..], &[other.as_str()]].concat()));
        let exclude = [
            "--account-index",
            "program-id",
            "--account-index-exclude-key",
        ];
        assert!(!indexes(&[&exclude[..], &[program.as_str()]].concat()));
        assert!(indexes(&[&exclude[..], &[other.as_str()]].concat()));
    }
}
