/** Mirrors the payloads published by `dashboard/src/collect.rs`. */

export type SlotLevel =
  | "incomplete"
  | "completed"
  | "optimistically_confirmed"
  | "rooted"
  | "finalized"
  | "skipped";

export interface SlotEntry {
  slot: number;
  level: SlotLevel;
  mine: boolean;
  block: BlockDetail | null;
  duration_nanos: number | null;
  time_millis: number | null;
  shreds: ShredArrival | null;
  /** Null for a bank this validator built. */
  replayed_millis: number | null;
  /** Null until the reward certificate has been seen, and always under TowerBFT. */
  reward: Reward | null;
  left_out: number | null;
}

/** `no_certificate`: no rooted block eight slots on carried a certificate, so nobody was paid. */
export type Reward = "paid" | "unpaid" | "no_certificate";

export interface ShredArrival {
  count: number;
  /** Nought is the block arriving whole over turbine. */
  repaired: number;
  /** Rebuilt here from erasure coding rather than received. */
  recovered: number;
  full_millis: number;
}

export interface Shreds {
  received: number;
  repaired: number;
  repair_rate: number;
}

export interface AccountsCache {
  /** Covers only reads past the write cache; not the card's headline. */
  read: number;
  hit_rate: number;
  evictions: number;
  cache_bytes: number;
  cache_entries: number;
  /** In accounts; only `from_storage` touches a file. */
  from_write_cache: number;
  from_read_cache: number;
  from_storage: number;
  stored_accounts: number;
  stored_bytes: number;
  /** What the window spans, for turning totals into rates. */
  window_seconds: number;
  disk: AccountsDisk | null;
}

export interface AccountsDisk {
  used: number;
  allocated: number;
  /** What shrink reclaims. */
  fragmented: number;
  storages: number;
}

/** The index and write cache as they stand, then counts over the window. */
export interface AccountsDb {
  index_entries: number;
  /** The slots of the index's hash maps, which is what its memory follows. */
  index_capacity: number;
  index_bytes: number;
  write_cache_bytes: number;
  write_cache_slots: number;
  write_cache_accounts: number;
  cleaned_to: number;

  index_found: number;
  /** Lookups of accounts that do not exist. */
  index_missing: number;
  index_inserts: number;
  index_deletes: number;
  clean_runs: number;
  cleaned_slots: number;
  cleaned_accounts: number;
  shrunk_slots: number;
  shrunk_accounts: number;
  /** Dead bytes dropped, and live bytes written again to drop them. */
  shrunk_bytes: number;
  rewritten_bytes: number;
  /** How long each job ran, counted as it ends. */
  clean_micros: number;
  shrink_micros: number;
  flush_micros: number;
  window_seconds: number;
}

export interface ProgramCache {
  looked_up: number;
  hits: number;
  misses: number;
  hit_rate: number;
  evictions: number;
  reloads: number;
  insertions: number;
  lost_insertions: number;
  replacements: number;
  one_hit_wonders: number;
  prunes_orphan: number;
  prunes_environment: number;
  peak_entries: number | null;
  entry_limit: number;
}

/** Three arrays sharing an index, fetched on demand. */
export interface Displays {
  keys: string[];
  names: (string | null)[];
  icons: (string | null)[];
}

export interface Peer {
  identity: string;
  version: string | null;
  client: string | null;
  stake: number;
  ip: string | null;
  /** Two-letter country code of `ip`. */
  country: string | null;
  name: string | null;
  icon: string | null;
}

/** What a turn's card shows of its leader, whom the peer table may not hold. */
export type LeaderRow = Pick<Peer, "identity" | "client" | "version" | "ip" | "country" | "stake">;

export type GossipMessageKind = "push" | "pull_request" | "pull_response" | "ping" | "pong" | "prune";

/** Rates over a window, from gossip's own stats points; null while none arrive. */
export interface Gossip {
  table: {
    entries: number;
    pubkeys: number;
    /** Past this many distinct pubkeys gossip trims its table. */
    pubkey_capacity: number;
    nodes: number;
    staked_nodes: number;
    expired_per_second: number;
    evicted_last_minute: number;
  };
  /** Packets per second. */
  messages: { kind: GossipMessageKind; received: number; sent: number }[];
  /** Entries per second; `types` busiest first. */
  entries: {
    accepted_push: number;
    accepted_pull: number;
    duplicate_push: number;
    redundant_pull: number;
    rejected_push: number;
    rejected_pull: number;
    types: { kind: string; push: number; pull: number; rejected: number }[];
  };
  /** Per second. */
  pressure: {
    dropped_in: number;
    dropped_out: number;
    pull_no_budget: number;
    pull_scan_exhausted: number;
    other_shred_version: number;
    ping_check_failed: number;
    unverified_addresses: number;
    bad_prune_destination: number;
  };
  /** Milliseconds spent per second. */
  time: {
    push: number;
    pull_requests: number;
    pull_responses: number;
    ping_pong_prune: number;
    verify: number;
    other: number;
  };
  dropped_last_minute: number;
  /** Received and sent, against which drops are judged. */
  packets_last_minute: number;
  packets_per_second: number;
}

/** Every node in gossip, one array per column, most stake first. */
export interface GossipPeers {
  root: number;
  /** Lamports. */
  total_stake: number;
  /** `[client, version]`, indexed by `client`. */
  clients: [string, string][];
  identity: string[];
  name: (string | null)[];
  /** Lamports. */
  stake: number[];
  client: number[];
  ip: (string | null)[];
  /** Two-letter country code of `ip`. */
  country: (string | null)[];
  rpc: (number | null)[];
  /** Under Alpenglow, the share of recent blocks whose finalization certificate carried its vote. */
  finalization: (number | null)[];
  /** The epoch `vote_arrival` describes; null until votor has reported one. */
  vote_arrival_epoch: number | null;
  /** Under Alpenglow, microseconds from a slot starting here to the peer's votes arriving: notarize
   *  mean, spread and worst, then the finalize mean; then the notarize votes the mean is over. */
  vote_arrival: ([number, number | null, number | null, number | null, (number | null)?] | null)[];
  /** Staked and not voting now; absent from a validator older than the page. */
  delinquent?: boolean[];
  /** The oldest last vote among a staked node's vote accounts. */
  last_vote?: (number | null)[];
}

/** The schedule's matches summed up by the validator; fees in lamports, tips before jito's cut. */
export interface ScanFigures {
  slots: number;
  skipped: number;
  transactions: number;
  duration_ms: number | null;
  /** Duration of the first slot of each turn. */
  first_slot_ms?: number | null;
  /** Duration of each turn's other slots. */
  rest_of_turn_ms?: number | null;
  replay_ms: number | null;
  base_fee: number | null;
  priority_fee: number | null;
  tips: number | null;
  compute: number | null;
}

export interface ClientFigures extends ScanFigures {
  /** Null for leaders gossip names no client for. */
  client: string | null;
  /** The leaders these slots came from. */
  validators?: number;
}

export interface CountryFigures extends ScanFigures {
  /** Null for leaders with no country known. */
  country: string | null;
  /** The leaders these slots came from. */
  validators?: number;
}

export interface ScheduleSummary {
  turns: number;
  figures: ScanFigures;
  clients: ClientFigures[];
  /** Absent from a validator older than the page. */
  countries?: CountryFigures[];
  /** The oldest slot the validator holds. */
  held_from: { slot: number; millis: number | null } | null;
  /** The clients, countries and versions among staked nodes and scheduled leaders. */
  choices: { clients: string[]; countries: string[]; versions: string[] };
  /** Every staked node not voting now. */
  delinquent: { identity: string; last_vote: number | null }[];
}

/** A page of the turns to come that match, oldest first. */
export interface FutureReply {
  turns: number[];
  /** Each leader of those turns once. */
  leaders: LeaderRow[];
  /** Where the next page starts. */
  next: number | null;
  /** Matches after this page. */
  rest: number;
  /** The validator name conditions choose, where they hold for exactly one this epoch. */
  named: string | null;
}

/** Absent without a tip payment program; `commission_bps` absent without the flag. */
export interface TipRates {
  jito_cut_bps: number;
  commission_bps: number | null;
}

export interface BlockDetail {
  transactions: number;
  non_vote_transactions: number;
  failed_transactions: number;
  entries: number;
  block_cost: number;
  block_cost_limit: number;
  account_cost_limit: number;
  total_fees: number;
  priority_fees: number;
  /** Shares are derived in `tips.ts`; `null` where unmeasured. */
  tips: number | null;
  /** In microseconds. Null for a block this validator built. */
  replay_micros: number | null;
}

export interface Tps {
  total: number;
  vote: number;
  non_vote_success: number;
  non_vote_failed: number;
}

export interface TpsSample extends Tps {
  slot: number;
  timestamp_nanos: number;
}

export interface StakeSummary {
  activated_stake: number;
  total_stake: number;
  /** In [0, 1]. */
  share: number;
}

export interface ValidatorCounts {
  total: number;
  delinquent: number;
  rpc_nodes: number;
  non_delinquent_stake: number;
  delinquent_stake: number;
}

export interface VersionShare {
  version: string | null;
  validators: number;
  stake: number;
  other: boolean;
}

export type FeePayerAlert = "flagged" | "escalated";

/** Our rooted blocks' non-vote transactions; "away" is the same payers in other leaders' blocks. */
export interface FeePayerEpoch {
  epoch: number;
  blocks: number;
  transactions: number;
  failed: number;
  /** Lamports. */
  priority_failed: number;
  away_transactions: number;
  away_failed: number;
}

export interface FeePayerRow {
  epoch: number;
  payer: string;
  transactions: number;
  failed: number;
  priority_failed: number;
  away_transactions: number;
  away_failed: number;
  alert: FeePayerAlert | null;
  /** Our slot in which it first met the flag. */
  flagged_slot: number | null;
}

/** Newest epoch first; the payers are the newest epoch's, by priority fees on failed transactions. */
export interface FeePayerSummary {
  epochs: FeePayerEpoch[];
  payers: FeePayerRow[];
  alerts: FeePayerRow[];
}

export interface EpochInfo {
  epoch: number;
  start_slot: number;
  end_slot: number;
  slots_in_epoch: number;
  my_leader_slots: number[];

  leaders: string[];
  /** One index into `leaders` per turn of four slots, `leaders[turns[(slot - start_slot) / 4]]`.
   *  Empty where the schedule could not be derived. */
  turns: number[];

  block_cost_limit: number;
  account_cost_limit: number;
}

export type InterfaceKind = "physical" | "bond" | "bridge" | "vlan" | "tunnel" | "virtual";

/** One of the host's network interfaces in use. */
export interface NetInterface {
  name: string;
  kind: InterfaceKind;
  up: boolean;
  mtu: number | null;
  routes: number;
  /** The most routes it carried in any hour of the last day. */
  routes_peak: number;
  /** The interface it is a port of (its master), such as a bond, bridge or team. */
  member_of: string | null;
  /** On a tunnel, the interface holding the default route, which carries its packets. */
  inside: string | null;
  /** Null until a second reading is in. */
  received_per_second: number | null;
  sent_per_second: number | null;
}

export interface Network {
  received_per_second: number;
  sent_per_second: number;
}

export interface NetworkSample extends Network {
  timestamp_nanos: number;
}

/** In bytes per second; null until a sender has reported. */
export interface EgressSplit {
  gossip_per_second: number | null;
  repair_per_second: number | null;
}

export interface IngestPath {
  name: string;
  port: number;
  drops_recent: number;
  drops_total: number;
  queued_bytes: number;
  received_recent: number | null;
  received_total: number | null;
  /** False where the receiver counts messages, as votor counts votes. */
  received_in_packets: boolean;
  quic: boolean;
}

export interface ProducedBlock {
  slot: number;
  slot_time_millis: number | null;
  blockhash: string;
  duration_nanos: number | null;
  transactions: number;
  non_vote_transactions: number;
  failed_transactions: number;
  entries: number;
  block_cost: number;
  block_cost_limit: number;
  account_cost_limit: number;
  total_fees: number;
  priority_fees: number;
  /** `null` where unmeasured, nought where nobody tipped. */
  tips: number | null;
  bundles: { sanitized: number; executed: number } | null;
  versions: TxVersions | null;
  execution: Execution | null;
  certificate: BlockCertificate | null;
  /** Where the slot's time went on our side; its parts land over the slot's last moments. */
  leading: LeaderTiming | null;
}

export interface LeaderTiming {
  /** Under Alpenglow, from trying to start the slot to its bank existing, any wait for the parent
   *  included. */
  start_micros: number | null;
  /** The part of the start spent waiting for the parent's replay; reported from 4.4 on. */
  parent_wait_micros: number | null;
  /** The bank open to transactions, from set to cleared. */
  open_millis: number | null;
  /** From broadcast's first entries for the slot to its last shred sent. */
  broadcast_micros: number | null;
  shredding_micros: number | null;
  scheduler_receive_micros: number | null;
  scheduler_schedule_micros: number | null;
  fast_handover: boolean;
  parent_switched: boolean;
}

export interface BlockCertificate {
  rewards: number;
  leader: string | null;
  leader_name: string | null;
  /** No fewer notarize votes than skip votes. */
  notarized: boolean;
  paid: number;
  ranks: number;
  stake_paid: number;
  notar: number;
  skip: number;
  ours_in: boolean;
  usual: number | null;
  left_out: CertificateValidator[];
}

export interface CertificateValidator {
  identity: string;
  name: string | null;
  ip: string | null;
  country: string | null;
}

export interface TxVersions {
  legacy: number;
  v0: number;
  v1: number;
}

export interface StageTimes {
  cost_model: number;
  load_execute: number;
  freeze_lock: number;
  record: number;
  commit: number;
  send_votes: number;
}

export interface Execution {
  /** Thread time, not wall time. */
  non_vote: StageTimes;
  workers: number;
  longest_batch: number;
  votes: StageTimes | null;
  window_millis: number;
}

/** The TPU path's totals are differenced at its end: everything since the previous turn drained. */
export interface LeaderTurn {
  first: number;
  last: number;
  produced: number;
  drained_millis: number;
  since_millis: number | null;
  quic: QuicPort;
  verify: VerifyStage;
  executed: ExecutedStage;
}

/** BAM counts what arrived in batches. */
export type SchedulerSource = "scheduler" | "bam";

/** Where the transactions handed to the banking stage went, over the window. `received` equals
 *  `buffered` plus the losses through `nonce_conflict`; a BAM slot's first two count batches. */
export interface Waterfall {
  received: number;

  source?: SchedulerSource;

  /** On a BAM slot `not_held` counts batches sent past their deadline instead. */
  not_held: number;
  check_queue_full: number;
  unparsable: number;
  bad_locks: number;
  compute_budget: number;
  too_old: number;
  already_processed: number;
  fee_payer: number;
  filtered: number;
  nonce_conflict: number;

  buffered: number;

  queue_full: number;
  nonce_evicted: number;
  cleared: number;
  cleaned: number;

  scheduled: number;
  /** Not losses: work the scheduler had but could not place this pass. */
  blocked_conflicts: number;
  blocked_threads: number;

  finished: number;
  retried: number;
}

/** `open` and `active_streams` are levels. */
export interface QuicPort {
  name: string;

  offered: number;
  shed_all: number;
  shed_address: number;
  refused_full: number;
  handshake_timeout: number;
  handshake_error: number;
  handshook: number;
  /** Refused a place in the connection table, under four overlapping
   *  counters. Never summed; `refusedTable` in `tpuPath.ts` reconciles them. */
  add_failed: number;
  add_failed_staked: number;
  add_failed_unstaked: number;
  add_failed_banned: number;
  admitted_staked: number;
  admitted_unstaked: number;

  streams: number;
  throttled_staked: number;
  throttled_unstaked: number;
  read_timeouts: number;
  read_errors: number;
  invalid_size: number;

  handed_on: number;
  queue_full: number;
  disconnected: number;

  open: number;
  active_streams: number;

  kernel_drops: number | null;
}

export interface QuicPaths {
  ports: QuicPort[];
  tpu_offhost: boolean;
}

/** `counted_slots` short of `elapsed_slots` is a restart part way through. */
export interface EpochSpan {
  epoch: number;
  elapsed_slots: number;
  counted_slots: number;
  slots_in_epoch: number;
}

/** Counted on arrival, so an upper bound on the executed share. */
export interface BundleStage {
  received: number;
  packets: number;
}

export interface VerifyStage {
  received: number;
  duplicate: number;
  below_floor: number;
  verified: number;
  /** Batches, not transactions. Never added to the counts beside it. */
  evicted_batches: number;
}

export interface ExecutedStage {
  attempted: number;
  cost_throttled: number;
  retryable: number;
  expired_bank: number;
  processed: number;
  succeeded: number;

  /** Terminal reasons only; retries and instruction errors are drawn elsewhere. */
  too_many_locks: number;
  account_missing: number;
  fee_payer_broke: number;
  fee_payer_invalid: number;
  blockhash_missing: number;
  blockhash_old: number;
  already_processed: number;
  bad_compute_budget: number;
  program_not_executable: number;
}

export interface SlotWaterfall extends Waterfall {
  slot: number;
}

export interface CpuUse {
  busy: number;
  user: number;
  system: number;
  iowait: number;
  steal: number;
}

/** Milliseconds in each stage of a block's path on this node; null where it was not timed. */
export interface BlockLifeStages {
  /** From the first shred to the leader closing the block. */
  building: number | null;
  /** From the leader closing the block, by its clock, to the block being full here. */
  reaching: number | null;
  replay: number | null;
  /** From replay finishing to our notarize vote. */
  vote: number | null;
  /** From our notarize vote, or from replay where this node did not vote, to finalization. */
  finalize: number | null;
}

export type BlockLifeStage = "reaching" | "replay" | "vote" | "finalize";

export interface SlowBlock {
  slot: number;
  stages: BlockLifeStages;
  /** From the leader closing the block to its finalization. */
  total: number | null;
  /** Milliseconds the leader's clock runs behind ours, ahead where negative, set only where its
   *  close cannot be right. The stages then start from our first shred. */
  leader_clock_off: number | null;
  /** The stage furthest over its median. */
  cause: BlockLifeStage;
  shreds: number;
  repaired: number;
  recovered: number;
}

export interface BlockLifeSummary {
  /** Minutes the medians cover, an hour at most. */
  minutes: number;
  /** Each stage's own median. */
  median: BlockLifeStages;
  total: number | null;
  slowest: SlowBlock[];
}

/** The 5th, 50th and 95th percentiles. */
export interface Spread {
  p05: number;
  p50: number;
  p95: number;
}

/** One figure per bin of the Cluster page's window, each null where the bin holds too few
 *  readings. */
export interface ChartSeries {
  p05: (number | null)[];
  p25: (number | null)[];
  p50: (number | null)[];
  p75: (number | null)[];
  p95: (number | null)[];
  spread: Spread;
  /** The median over the last ten minutes. */
  recent: number | null;
  floor: number;
  top: number;
  /** Every reading counted in equal steps from `floor` to `top`. */
  hist: number[];
}

/** A share of blocks per bin, in percent. */
export interface ChartShares {
  shares: (number | null)[];
  count: number;
  total: number;
  share: number;
  recent: number | null;
  floor: number;
}

export interface SkipLeader {
  identity: string;
  name: string | null;
  country: string | null;
  slots: number;
  skipped: number;
}

export interface SkipChart {
  to_slot: number;
  from_millis: number;
  to_millis: number;
  /** Percent of each bin's settled slots skipped. */
  shares: (number | null)[];
  skipped: number;
  slots: number;
  recent: number | null;
  /** The previous epoch's percent. */
  previous: number | null;
  /** Percent of each leader's settled slots skipped; null until enough leaders have them. */
  per_leader: Spread | null;
  leaders: SkipLeader[];
}

export interface ClientShare {
  client: string;
  validators: number;
  stake: number;
  releases: string[];
  other: boolean;
}

export interface CountryStake {
  code: string | null;
  stake: number;
  validators: number;
  /** On the folded row, how many countries it holds. */
  others: number | null;
}

export interface StakeByCountry {
  stake: number;
  countries: number;
  ours: string | null;
  ours_continent: string | null;
  rows: CountryStake[];
  /** Two-letter continent codes, null for stake whose country is not known. */
  continents: { code: string | null; stake: number }[];
}

/** The Cluster page's charts, from where this node caught up to now. */
export interface ClusterCharts {
  epoch: number;
  from_millis: number;
  to_millis: number;
  cu: ChartSeries | null;
  tps: ChartSeries | null;
  succeeded: ChartSeries | null;
  failed: ChartSeries | null;
  slot_time: ChartSeries | null;
  notarized: ChartSeries | null;
  fast: ChartShares | null;
  failed_share: number | null;
  skips: SkipChart | null;
  clients: ClientShare[];
  countries: StakeByCountry | null;
}

/** Quartiles of handoff gaps in milliseconds. */
export interface HandoffBand {
  p25: number;
  p50: number;
  p75: number;
}

export interface LateLeader {
  leader: string;
  /** From the leader's gossip address. */
  country: string | null;
  n: number;
  p50: number;
  /** Handoffs with a gap above the p75 of its country's leaders. */
  over: number;
}

/** One settled leader, as listed for a bin of the leaders strip. */
export interface LeaderFigures {
  leader: string;
  country: string | null;
  n: number;
  p25: number;
  p50: number;
  p75: number;
  /** Late for its country; false where the country has too few settled leaders to judge. */
  late: boolean;
}

/** The gap before each leader's turn, from the previous slot's block filling here to the next
 *  leader's first shred arriving here, over an epoch of slots. */
export interface HandoffSummary {
  since_millis: number;
  /** Whether the window covers a whole epoch of slots. */
  full: boolean;
  cluster: HandoffBand;
  /** Handoffs per 10 ms from nought; the first bin holds negative gaps, the last every longer one. */
  gaps: number[];
  /** Settled leaders that are not late, per 6 ms of their median. */
  medians: number[];
  /** Settled leaders that are late, in the same bins; complete where `late` is cut. */
  late_medians: number[];
  settled: number;
  /** Latest first. */
  late: LateLeader[];
  late_count: number;
  /** Settled leaders whose country is unknown or has too few settled leaders. */
  unjudged: number;
  ours: { country: string | null; n: number; p50: number | null; over: number; judged: boolean };
  trend: HandoffTrend;
}

/** The mean gap over each minute of the last hour, oldest first; null for a minute without a handoff. */
export interface HandoffTrend {
  mean: (number | null)[];
  p25: (number | null)[];
  p75: (number | null)[];
  hour_p50: number | null;
}

/** The leader whose turn is arriving now. */
export interface HandoffNow {
  leader: string;
  country: string | null;
  /** Null until the slot before has filled, or where that slot was ours or skipped. */
  gap: number | null;
  n: number;
  /** Null until settled. */
  band: HandoffBand | null;
  over: number;
  late: boolean;
  /** Whether its country has enough settled leaders to judge against. */
  judged: boolean;
  /** Each handoff while too few for a band. */
  gaps: number[];
  minute_mean: number | null;
}

/** Under Alpenglow, the stake whose average vote had arrived by each millisecond after a slot began
 *  here, over the last epoch votor reported. Shares are in hundredths of a percent. */
export interface CertFormation {
  epoch: number;
  /** This node's mean slot time over that epoch; null until enough of it was timed. */
  slot_nanos: number | null;
  /** Index `t` is the share arrived before `t` ms. */
  notarize: number[];
  finalize: number[];
  /** Every measured vote, later ones included. */
  notarize_total: number;
  finalize_total: number;
  notarized_millis: number | null;
  fast_final_millis: number | null;
  second_round_millis: number | null;
  /** About the slots this node timed that epoch, fewer where it started partway through. */
  slots: number | null;
  /** Slots each path finalized first, over the same epoch since this node started. */
  fast_slots: number | null;
  second_round_slots: number | null;
}

/** How often blocks' finalization certificates carried each validator's vote, over ten minutes. */
export interface FinalizationShare {
  blocks: number;
  validators: number;
  /** Null when our vote account was not staked in the window. */
  ours: number | null;
  median: number | null;
  /** Share of the other validators ours is above. */
  above: number | null;
  /** Validators per tenth of the share, lowest first. */
  bands: number[];
  /** Finished minutes over the last hour, oldest first. */
  trend: FinalizationMinute[];
  /** Median from a block's last shred to our notarize vote, over the last minute. */
  vote_micros: number | null;
  /** Validators whose gossip address is in our country, ours among them; null where too few. */
  country: CountryShare | null;
}

export interface CountryShare {
  /** Two-letter code. */
  code: string;
  validators: number;
  median: number;
  /** Those validators per tenth of the share, lowest first. */
  bands: number[];
}

export interface FinalizationMinute {
  start_millis: number;
  ours: number | null;
  median: number | null;
}

export interface Host {
  cores: number;
  load_one: number;
  load_five: number;
  load_fifteen: number;
  threads: number;
  running: number;
  cpu: CpuUse | null;
  /** The first core's frequency governor; null where the kernel exposes none. */
  cpu_governor: string | null;

  memory_total: number;
  memory_available: number;
  /** Used, but handed back the moment it is wanted. */
  memory_reclaimable: number;
  memory_free: number;
  swap: { total: number; used: number } | null;
  process_resident: number | null;
  process_resident_hour_ago: number | null;
  /** Null on kernels before 4.5, which do not split the resident figure. */
  process_kinds: ResidentKinds | null;
  memory_parts: MemoryParts;
  snapshot_device: string | null;

  filesystems: FilesystemUsage[];
  devices: DeviceLoad[];
}

/** The three add up to `process_resident`. */
export interface ResidentKinds {
  anonymous: number;
  file: number;
  shared: number;
}

/** Sizes the validator's parts report of themselves; null until first reported. */
export interface MemoryParts {
  /** Agave's own estimate, which counts one slot list entry per account. */
  index_estimate: number | null;
  write_cache: number | null;
  read_cache: number | null;
  blockstore: number | null;
}

export interface ThreadGroup {
  name: string;
  count: number;
  cores: string | null;
  on_cpu: number;
  waiting: number;
  other: boolean;
}

export interface ThreadsSample {
  timestamp_nanos: number;
  threads: number;
  groups: ThreadGroup[];
}

export interface FilesystemUsage {
  name: string;
  path: string;
  total: number;
  available: number;
  /** Free to root alone, and so counted as used. */
  reserved: number;
  parts: DiskPart[];
  /** Directories named `*_to_be_deleted`; their size is not read. */
  leftovers: string[];
}

export type DiskPartKey =
  | "shreds"
  | "history"
  | "metadata"
  | "live"
  | "fragmented"
  | "full"
  | "incremental";

export interface DiskPart {
  key: DiskPartKey;
  bytes: number;
}

export interface DeviceLoad {
  device: string;
  roles: string[];
  /** Not a fill: a device can sit at 1 with the filesystem nearly empty. */
  busy: number;
  wait_ms: number | null;
  operations_per_second: number;
  read_per_second: number;
  write_per_second: number;
}

/** Means per slot bar the two peaks. `fetch`, `confirming` and `completing` are disjoint; the
 *  verify figures overlap. */
export interface ReplayWindow {
  slots: number;
  transactions: number;

  fetch: number;
  confirming: number;
  completing: number;
  /** The worst single slot's total, not the largest each field reached. */
  serial_peak: number;

  poh_verify: number;
  tx_verify: number;
  dispatch: number;

  execute: number;
  bytecode: number;
  serialising: number;
  deserialising: number;
  load: number;
  store: number;
  program_cache: number;
  compiling: number;
  program_cache_peak: number;
  checking: number;
  other: number;
}

export interface SlotCost {
  slot: number;
  costliest_account: string;
  costliest_cost: number;
  block_cost: number;
  accounts: number;
  /** Accounts within five percent of the per-account ceiling. */
  contended: number;
  new_account_data: number;
  in_flight: number;
}

/** How often an account was a block's costliest, over the blocks the validator holds. */
export interface Recurrence {
  blocks: number;
  of: number;
  peak_cost: number;
  peak_slot: number;
}

export interface CostDetail extends SlotCost {
  recurrence: Recurrence | null;
}

/** One block as the list draws it: slot, time, transactions, block cost, cost limit, total fees,
 *  priority fees, tips and duration. */
export type FigureRow = [
  number,
  number | null,
  number,
  number,
  number,
  number,
  number,
  number | null,
  number | null,
];

/** First slot, last slot, produced, drained and the previous drain. */
export type TurnHeadRow = [number, number, number, number, number | null];

/** A page of the blocks held, newest first; `next` is where the following page starts. */
export interface FiguresPage {
  figures: FigureRow[];
  turns: TurnHeadRow[];
  held: number;
  floor: number;
  next: number | null;
}

/** In lamports. */
export interface TopFeePayer {
  payer: string;
  transactions: number;
  failed: number;
  priority: number;
  paid: number;
}

/** In lamports; `failed` is null where the status cache held no result. */
export interface TopFeeTransaction {
  signature: string;
  payer: string;
  failed: boolean | null;
  priority: number;
  paid: number;
}

export interface BlockFees {
  slot: number;
  payer_count: number;
  payers: TopFeePayer[];
  transactions: TopFeeTransaction[];
}

/** Everything drawn for a span of our slots once opened. */
export interface ProducedDetail {
  blocks: ProducedBlock[];
  turns: LeaderTurn[];
  waterfalls: SlotWaterfall[];
  costs: CostDetail[];
  /** Sent only when one slot is asked for. */
  fees: BlockFees | null;
}

export interface IngestSummary {
  window_seconds: number;
  paths: IngestPath[];
}

export interface XdpConfig {
  /** The bind fails rather than falls back, so true is trustworthy. */
  zero_copy: boolean;
  driver: string;
  /** Both of these read "unknown" where the PCI database could not be read. */
  vendor: string;
  model: string;
  kernel_version: string;
}

export interface StartupProgress {
  phase: string;
  detail: string | null;
  running: boolean;
  fraction: number | null;
  /** A whole percent. Null outside the supermajority wait. */
  stake_percent: number | null;
  stake_in_gossip: StakeInGossip | null;
  phase_elapsed_nanos: number;
  phases_taken: PhaseTiming[];
}

export interface PhaseTiming {
  phase: string;
  elapsed_nanos: number;
}

export interface StakeInGossip {
  online: number;
  total: number;
}

export interface GossipStake {
  slot: number;
  shred_version: number;
  total: number;
  seen: number;
  validators: GossipValidator[];
}

export interface GossipValidator {
  identity: string;
  name: string | null;
  version: string | null;
  stake: number;
  seen: boolean;
}

export interface Health {
  replay: "not_started" | "running" | "stalled";
  vote: "not_voting" | "not_started" | "voting" | "delinquent";
}

/** Under alpenglow votes are not transactions. */
export type Consensus = "tower" | "alpenglow";

export interface SkipRate {
  epoch: number;
  rate: number | null;
}

/** This validator's leader slots in one epoch, as far as the root has passed them. */
export interface LeaderSlotCounts {
  epoch: number;
  slots: number;
  /** Includes any slot too old for the slot history to answer. */
  passed: number;
  produced: number;
  skipped: number;
}

export interface SnapshotArchive {
  slot: number;
  written_millis: number | null;
}

/** A null interval is disabled. */
export interface Snapshots {
  full: SnapshotArchive | null;
  incremental: SnapshotArchive | null;
  full_interval: number | null;
  incremental_interval: number | null;
  writing: SnapshotWriting | null;
  last_written: SnapshotWritten | null;
}

export interface SnapshotWriting {
  slot: number;
  since_millis: number;
}

export interface SnapshotWritten {
  slot: number;
  took_millis: number;
  fell_behind_slots: number;
}

export interface Turbine {
  root: number;
  layer_1: number;
  layer_2: number;
  layer_3: number;
  xdp_dropped: number;
  xdp: boolean | null;
}

export type VoteCost =
  | { kind: "fees"; per_day: number }
  | { kind: "ticket"; lamports: number; minimum: number; pending: number };

/** Under alpenglow the field holds lamports of reward. */
export interface VoteCredits {
  epoch: number;
  credits: number;
  cluster_max: number | null;
  /** Over the staked validators with credits this epoch. */
  cluster_median: number | null;
}

/** Lamports before commission; the maxima are for the whole epoch. */
export interface EpochEarnings {
  epoch: number;
  voting: number;
  voting_lost: number;
  voting_max: number;
  bonus: number;
  bonus_lost: number;
  bonus_max: number;
  commission_bps: number;
  /** Our blocks the ledger no longer holds; their bonus counts as voting. */
  unread: number;
}

export interface VoteParticipation {
  epoch: number;
  since_slot: number;
  paid: number;
  rewarded: number;
  cluster_max: number;
  /** Over the ranks paid at least once; null before any is. */
  cluster_median: number | null;
  misses: Misses;
  miss_bins: number[];
  lost_leaders: LostLeader[];
  ranks: number;
  /** A tenth under the epoch's median certificate; null until a hundred are in. */
  thin_below: number | null;
}

export interface LostLeader {
  identity: string;
  name: string | null;
  count: number;
}

export interface Admission {
  seat: boolean;
  next_seat: boolean | null;
  ticket_short: number | null;
}

export type MissPlace = "boundary" | "leader" | "snapshot" | "thin" | "late" | "lost";

/** Microseconds from votor's start on the slot; the first shred is reported only for a leader
 *  window's first slot. */
export interface VoteSent {
  first_shred_us: number | null;
  parent_ready_us: number | null;
  notarize_us: number | null;
  skip_us: number | null;
}

export interface MissWriter {
  identity: string;
  name: string | null;
  client: string | null;
  version: string | null;
  ip: string | null;
  country: string | null;
  certificates: number;
  misses: number;
}

export interface MissValidator {
  identity: string;
  name: string | null;
  ip: string | null;
  country: string | null;
  delinquent: boolean;
  /** Unheard for five minutes, or absent from this node's gossip table. */
  no_gossip: boolean;
}

export interface MissRow {
  slot: number;
  time_millis: number | null;
  place: MissPlace;
  paid_ranks: number;
  others: number[];
  writer: number | null;
  vote: VoteSent | null;
}

export interface MissList {
  epoch: number;
  rewarded: number;
  ranks: number;
  writers: MissWriter[];
  validators: MissValidator[];
  rows: MissRow[];
}

export interface WrittenList {
  rewarded: number;
  certificates: number;
  carried_all: number;
  rows: WrittenRow[];
}

export interface WrittenRow {
  identity: string;
  name: string | null;
  client: string | null;
  version: string | null;
  ip: string | null;
  country: string | null;
  left_out_of_ours: number;
  left_out_everywhere: number;
  /** The oldest last vote among its staked vote accounts; null where one never voted or it has none. */
  last_vote: number | null;
  delinquent: boolean;
  /** When this node last heard it over gossip, in unix milliseconds. */
  heard_millis: number | null;
  /** Unheard for five minutes, or absent from this node's gossip table. */
  no_gossip: boolean;
}

/** A slot in more than one place counts in the first. */
export interface Misses {
  boundary: number;
  leader: number;
  snapshot: number;
  thin: number;
  late: number;
  lost: number;
}

export interface Envelope {
  topic: string;
  key: string;
  id?: number;
  value: unknown;
}

/** `Store.get` is typed by it; a key missing here cannot be read. */
export interface Published {
  summary: {
    version: string;
    client: string;
    cluster: string;
    shred_version: number;
    /** Null while this node gossips no address. */
    gossip_address: { ip: string; country: string | null } | null;
    identity_key: string;
    identity_name: string | null;
    identity_icon: string | null;
    vote_key: string;
    uptime_nanos: number;
    server_time_nanos: number;
    caught_up_time_nanos: number;
    startup_progress: StartupProgress;
    gossip_stake: GossipStake | null;
    root_slot: number;
    optimistically_confirmed_slot: number;
    finalized_slot: number;
    completed_slot: number;
    estimated_slot: number;
    block_height: number;
    next_leader_slot: number | null;
    vote_slot: number | null;
    behind_cluster: number | null;
    replay_rate: number | null;
    identity_balance: number;
    vote_balance: number;
    vote_cost: VoteCost;
    vote_commission: number | null;
    stake: StakeSummary;
    validator_counts: ValidatorCounts;
    versions: VersionShare[];
    estimated_slot_duration_nanos: number;
    observed_slot_duration_nanos: number | null;
    /** Alpenglow only: median microseconds from the leader closing a block, by its clock, to its finalization here. */
    finality_from_leader_micros: number | null;
    /** Alpenglow only; null before a block has been seen. */
    finalization_share: FinalizationShare | null;
    epoch_remaining_nanos: number;
    skip_rate: SkipRate;
    /** The previous epoch, when its schedule is known, then the current one. */
    leader_slot_counts: LeaderSlotCounts[];
    health: Health;
    consensus: Consensus;
    program_cache: ProgramCache | null;
    accounts_cache: AccountsCache | null;
    accounts_db: AccountsDb | null;
    shreds: Shreds | null;
    quic_paths: QuicPaths | null;
    verify: VerifyStage | null;
    executed: ExecutedStage | null;
    bundles: BundleStage | null;
    epoch_span: EpochSpan | null;
    produced_blocks: ProducedBlock[];
    slot_waterfalls: SlotWaterfall[];
    slot_costs: SlotCost[];
    produced_turns: LeaderTurn[];
    /** The oldest of our slots the validator holds. */
    produced_floor: number;
    tip_rates: TipRates;
    replay: ReplayWindow | null;
    host: Host | null;
    network: Network;
    /** Idle interfaces, down with no routes and no traffic, are left out. */
    net_interfaces: NetInterface[];
    network_egress: EgressSplit;
    xdp: XdpConfig | null;
    ingest_paths: IngestSummary;
    snapshots: Snapshots | null;
    bls_key: boolean | null;
    admission: Admission | null;
    vote_credits: VoteCredits | null;
    epoch_earnings: EpochEarnings | null;
    vote_participation: VoteParticipation | null;
    turbine: Turbine | null;
    gossip: Gossip | null;
    /** Alpenglow only; null until a replayed block's footer has been read. */
    block_life: BlockLifeSummary | null;
    /** Alpenglow only; null until votor has reported a finished epoch. */
    cert_formation: CertFormation | null;
    handoff: HandoffSummary | null;
    handoff_now: HandoffNow | null;
    /** Null until a block of ours is rooted. */
    fee_payers: FeePayerSummary | null;
  };
  epoch: { new: EpochInfo };
  peers: { all: Peer[] };
}
