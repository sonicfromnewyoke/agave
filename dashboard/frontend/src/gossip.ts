import { buildLabel, count, decimal, percent, sol } from "./format";
import type { Gossip, GossipMessageKind, GossipPeers } from "./types";
import type { Tone } from "./verdict";

/** Entry types listed by name; the rest share a row. */
export const BUSIEST_TYPES = 5;

export interface GossipPeer {
  identity: string;
  name: string | null;
  stake: number;
  client: string;
  clientName: string;
  version: string;
  ip: string | null;
  country: string | null;
  rpc: number | null;
  /** Under Alpenglow, the share of recent blocks whose finalization certificate carried its vote. */
  finalization: number | null;
  voteArrival: VoteArrival | null;
  /** Staked and not voting now. */
  delinquent: boolean;
  /** Slots from its last vote to our root; null where it never voted. */
  voteBehind: number | null;
  haystack: string;
}

/** Microseconds from a slot starting here to the peer's votes arriving, over an epoch. */
export interface VoteArrival {
  mean: number;
  spread: number | null;
  worst: number | null;
  finalize: number | null;
  /** The notarize votes the mean is over. */
  votes: number | null;
}

function behind(root: number, slot: number | null | undefined): number | null {
  return slot === null || slot === undefined ? null : Math.max(0, root - slot);
}

export function gossipPeers(list: GossipPeers): GossipPeer[] {
  const labels = list.clients.map(([client, version]) => buildLabel(client, version));
  return list.identity.map((identity, index) => {
    const name = list.name[index] ?? null;
    const client = labels[list.client[index]] ?? "";
    const [clientName = "", version = ""] = list.clients[list.client[index]] ?? [];
    const ip = list.ip[index] ?? null;
    const country = list.country[index] ?? null;
    return {
      identity,
      name,
      stake: list.stake[index] ?? 0,
      client,
      clientName,
      version,
      ip,
      country,
      rpc: list.rpc[index] ?? null,
      finalization: list.finalization[index] ?? null,
      voteArrival: arrivalOf(list.vote_arrival?.[index] ?? null),
      delinquent: list.delinquent?.[index] ?? false,
      voteBehind: behind(list.root, list.last_vote?.[index]),
      haystack: [name ?? "", client, ip ?? "", country ?? "", identity].join(" ").toLowerCase(),
    };
  });
}

function arrivalOf(cell: GossipPeers["vote_arrival"][number]): VoteArrival | null {
  if (!cell) return null;
  const [mean, spread, worst, finalize, votes] = cell;
  return { mean, spread, worst, finalize, votes: votes ?? null };
}

/** Which of a peer's votes the arrival column shows: notarize unless a finalize bar was picked. */
export type ArrivalKind = "notarize" | "finalize";

export function arrivalMicros(arrival: VoteArrival | null, kind: ArrivalKind): number | null {
  if (!arrival) return null;
  return kind === "notarize" ? arrival.mean : arrival.finalize;
}

/** The middle peer's mean, which a peer far above is marked against. */
export function arrivalMedian(peers: readonly GossipPeer[], kind: ArrivalKind = "notarize"): number | null {
  const means = peers
    .flatMap((peer) => {
      const micros = arrivalMicros(peer.voteArrival, kind);
      return micros === null ? [] : [micros];
    })
    .sort((a, b) => a - b);
  return means.length === 0 ? null : (means[Math.floor(means.length / 2)] ?? null);
}

const millis = (micros: number) => `${count(Math.round(micros / 1000))} ms`;

export function arrivalText(arrival: VoteArrival, kind: ArrivalKind = "notarize"): string {
  const micros = arrivalMicros(arrival, kind);
  return micros === null ? "—" : millis(micros);
}

export function arrivalTitle(arrival: VoteArrival, epoch: number | null, kind: ArrivalKind = "notarize"): string {
  if (kind === "finalize") {
    const when =
      arrival.finalize === null
        ? "No finalize vote was timed"
        : `Finalize votes reached us ${millis(arrival.finalize)} after the slot began on average`;
    return `${when}${epoch === null ? "" : ` in epoch ${count(epoch)}`}; notarize votes ${millis(arrival.mean)}.`;
  }
  const votes = arrival.votes === null ? "" : ` over ${count(arrival.votes)} vote${arrival.votes === 1 ? "" : "s"}`;
  const over = `${votes}${epoch === null ? "" : ` in epoch ${count(epoch)}`}`;
  const spread = arrival.spread === null ? "" : `, spread ${millis(arrival.spread)}`;
  const worst = arrival.worst === null ? "" : `, worst ${millis(arrival.worst)}`;
  const finalize = arrival.finalize === null ? "" : `; finalize votes ${millis(arrival.finalize)}`;
  return `Notarize votes reached us ${millis(arrival.mean)} after the slot began on average${over}${spread}${worst}${finalize}.`;
}

export type PeerFilter = "staked" | "rpc" | "delinquent";

export const PEER_FILTERS: { filter: PeerFilter; label: string }[] = [
  { filter: "staked", label: "Staked" },
  { filter: "rpc", label: "RPC" },
  { filter: "delinquent", label: "Delinquent" },
];

export function inFilter(peer: GossipPeer, filter: PeerFilter): boolean {
  switch (filter) {
    case "staked":
      return peer.stake > 0;
    case "rpc":
      return peer.rpc !== null;
    case "delinquent":
      return peer.delinquent;
  }
}

export function filterCounts(peers: GossipPeer[]): Record<PeerFilter, number> {
  const counts: Record<PeerFilter, number> = { staked: 0, rpc: 0, delinquent: 0 };
  for (const peer of peers) {
    for (const { filter } of PEER_FILTERS) if (inFilter(peer, filter)) counts[filter] += 1;
  }
  return counts;
}

/** Any part of a name, client, IP, country code or identity, ignoring case. */
export function matchesSearch(peer: GossipPeer, query: string): boolean {
  const needle = query.trim().toLowerCase();
  return needle === "" || peer.haystack.includes(needle);
}

export type PeerColumn =
  | "stake"
  | "name"
  | "client"
  | "ip"
  | "rpc"
  | "finalization"
  | "arrival";

export interface PeerSort {
  column: PeerColumn;
  /** Against the column's first order: text A to Z, addresses low to high, and the rest high to low. */
  reversed: boolean;
}

export const FIRST_SORT: PeerSort = { column: "stake", reversed: false };

/** The same heading again reverses; a new one starts in its first order. */
export function nextSort(sort: PeerSort, column: PeerColumn): PeerSort {
  return sort.column === column ? { column, reversed: !sort.reversed } : { column, reversed: false };
}

function ipValue(ip: string | null): number | null {
  if (ip === null) return null;
  const parts = ip.split(".");
  if (parts.length !== 4) return Number.MAX_SAFE_INTEGER;
  return parts.reduce((value, part) => value * 256 + Number(part), 0);
}

type Key = (peer: GossipPeer, kind: ArrivalKind) => number | string | null;

/** A null sorts last either way. */
const COLUMNS: Record<PeerColumn, { key: Key; ascending: boolean }> = {
  stake: { key: (peer) => peer.stake, ascending: false },
  name: { key: (peer) => peer.name?.toLowerCase() ?? null, ascending: true },
  client: { key: (peer) => peer.client.toLowerCase(), ascending: true },
  ip: { key: (peer) => ipValue(peer.ip), ascending: true },
  rpc: { key: (peer) => peer.rpc, ascending: false },
  finalization: { key: (peer) => peer.finalization, ascending: false },
  arrival: { key: (peer, kind) => arrivalMicros(peer.voteArrival, kind), ascending: true },
};

function compareKeys(a: number | string, b: number | string): number {
  if (typeof a === "number" && typeof b === "number") return a - b || 0;
  return String(a).localeCompare(String(b));
}

/** Ties fall back to stake, then identity, so a reload keeps the order. */
export function sortPeers(
  peers: GossipPeer[],
  sort: PeerSort,
  kind: ArrivalKind = "notarize",
): GossipPeer[] {
  const { ascending, key } = COLUMNS[sort.column];
  const sign = ascending !== sort.reversed ? 1 : -1;
  const keyed = peers.map((peer) => ({ peer, value: key(peer, kind) }));
  keyed.sort((a, b) => {
    if (a.value !== b.value) {
      if (a.value === null) return 1;
      if (b.value === null) return -1;
      const order = compareKeys(a.value, b.value);
      if (order !== 0) return sign * order;
    }
    return b.peer.stake - a.peer.stake || a.peer.identity.localeCompare(b.peer.identity);
  });
  return keyed.map(({ peer }) => peer);
}

/** The ARIA order for a heading: what the reader sees, not the column's first order. */
export function ariaSort(sort: PeerSort, column: PeerColumn): "ascending" | "descending" | undefined {
  if (sort.column !== column) return undefined;
  return COLUMNS[column].ascending !== sort.reversed ? "ascending" : "descending";
}

/** Whole SOL, with two decimals under one so a small stake is not shown as none. */
export function stakeText(lamports: number): string {
  return sol(lamports, lamports > 0 && lamports < 1e9 ? 2 : 0);
}

/** Whole numbers from ten up; below that one decimal, so a trickle is not shown as nothing. */
export function rate(value: number): string {
  if (value <= 0) return "0";
  return value < 10 ? decimal(value, 1) : count(Math.round(value));
}

export const MESSAGE_LABELS: Record<GossipMessageKind, string> = {
  push: "Push",
  pull_request: "Pull request",
  pull_response: "Pull response",
  ping: "Ping",
  pong: "Pong",
  prune: "Prune",
};

export interface EntryRow {
  label: string;
  push: number;
  pull: number;
  rejected: number;
}

/** Idle types are left out. */
export function entryRows(types: Gossip["entries"]["types"]): { shown: EntryRow[]; others: EntryRow | null } {
  const active = types.filter((type) => type.push + type.pull + type.rejected > 0);
  const shown = active.slice(0, BUSIEST_TYPES).map((type) => ({ label: type.kind, ...type }));
  const rest = active.slice(BUSIEST_TYPES);
  if (rest.length === 0) return { shown, others: null };
  const others = rest.reduce(
    (sum, type) => ({
      ...sum,
      push: sum.push + type.push,
      pull: sum.pull + type.pull,
      rejected: sum.rejected + type.rejected,
    }),
    { label: `${rest.length} other ${rest.length === 1 ? "type" : "types"}`, push: 0, pull: 0, rejected: 0 },
  );
  return { shown, others };
}

export interface TimePart {
  key: keyof Gossip["time"];
  label: string;
  millis: number;
  share: number;
}

const TIME_LABELS: [keyof Gossip["time"], string][] = [
  ["push", "Handling pushes"],
  ["verify", "Verifying packets"],
  ["pull_requests", "Answering pull requests"],
  ["pull_responses", "Handling pull responses"],
  ["ping_pong_prune", "Ping, pong and prune"],
  ["other", "Other processing"],
];

export function timeParts(time: Gossip["time"]): { parts: TimePart[]; total: number } {
  const total = TIME_LABELS.reduce((sum, [key]) => sum + Math.max(0, time[key]), 0);
  const parts = TIME_LABELS.map(([key, label]) => {
    const millis = Math.max(0, time[key]);
    return { key, label, millis, share: total > 0 ? millis / total : 0 };
  });
  return { parts, total };
}

export interface GossipVerdict {
  tone: Tone;
  headline: string;
  detail: string;
}

export function gossipVerdict(
  gossip: Gossip | null | undefined,
  peers: GossipPeers | null,
): GossipVerdict {
  if (!gossip) {
    return {
      tone: "muted",
      headline: "Gossip figures are not arriving.",
      detail: "They come from gossip's own metrics points, which need info-level logging for solana_gossip.",
    };
  }
  const nodes = peers ? peers.identity.length : gossip.table.nodes;
  let heard = `${count(nodes)} ${nodes === 1 ? "node" : "nodes"} heard`;
  if (peers && peers.total_stake > 0) {
    const held = peers.stake.reduce((sum, stake) => sum + stake, 0);
    heard += `, ${percent(Math.min(1, held / peers.total_stake), 1)} of stake among them`;
  }
  const dropped = gossip.dropped_last_minute;
  if (dropped === 0) {
    return { tone: "good", headline: "Gossip is keeping up.", detail: `${heard}. Nothing dropped in the last minute.` };
  }
  const share = dropShare(dropped, gossip.packets_last_minute);
  const lost = `${count(dropped)} of ${count(gossip.packets_last_minute)} dropped in the last minute (${dropPercent(share)})`;
  if (share >= DROP_WARN_SHARE) {
    return { tone: "warn", headline: "Gossip is dropping packets.", detail: `${heard}. ${lost}.` };
  }
  return { tone: "good", headline: "Gossip is keeping up.", detail: `${heard}. ${lost}.` };
}

/** Gossip's queues shed a few batches a minute under normal load, so only a share warns. */
export const DROP_WARN_SHARE = 0.01;

/** Drops against packets received and sent; all of them when nothing was counted. */
export function dropShare(dropped: number, packets: number): number {
  if (dropped <= 0) return 0;
  return packets > 0 ? Math.min(1, dropped / packets) : 1;
}

/** Enough digits that a share under a tenth of a percent does not read as nothing. */
export function dropPercent(share: number): string {
  if (share > 0 && share < 0.0001) return "<0.01%";
  if (share > 0 && share < 0.001) return percent(share, 2);
  return percent(share, share < 0.1 ? 1 : 0);
}
