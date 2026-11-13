/** The Gossip peers search box: plain words search as typed, and expressions such as
 *  `finalization<50` or `geo:DE,NL` become filters. All of it is answered in the page. */

import { count, countryName, LAMPORTS_PER_SOL } from "./format";
import { arrivalMicros, filterCounts, PEER_FILTERS, type ArrivalKind, type GossipPeer, type PeerFilter } from "./gossip";
import {
  compared,
  LIST_OPS,
  nameHolds,
  NUMBER_OPS,
  OP_LABELS,
  versionNamed,
  type Op,
} from "./scheduleConditions";
import {
  EXPRESSION,
  MAX_SUGGESTIONS,
  OPS,
  splitValues,
  suggestValues,
  TYPED_OPS,
  type SearchRow,
  type Suggestion,
  type TypeAction,
} from "./scheduleSyntax";

const UNITS = /\s*(ms|sol|slots?|%)$/i;

type KeyName =
  | "name"
  | "client"
  | "version"
  | "geo"
  | "ip"
  | "stake"
  | "finalization"
  | "arrival";

type PeerGroup = "only" | "validator" | "votes";

export const PEER_TITLES: Readonly<Record<PeerGroup, string>> = {
  only: "Only peers that are",
  validator: "Validator",
  votes: "Votes",
};

interface PeerKey {
  name: KeyName;
  kind: "list" | "number";
  group: PeerGroup;
  label: string;
  unit: string;
  /** The comparison and value the examples use. */
  op: string;
  example: string;
  what: string;
}

const KEYS: readonly PeerKey[] = [
  { name: "name", kind: "list", group: "validator", label: "Validator", unit: "", op: ":", example: "Helius", what: "part of a validator's name or key" },
  { name: "client", kind: "list", group: "validator", label: "Client", unit: "", op: ":", example: "Agave,JitoLabs", what: "any of these clients" },
  { name: "version", kind: "list", group: "validator", label: "Version", unit: "", op: ":", example: "4.4", what: "these client versions, whole or by their start" },
  { name: "geo", kind: "list", group: "validator", label: "Country", unit: "", op: ":", example: "DE,NL", what: "any of these countries" },
  { name: "ip", kind: "list", group: "validator", label: "IP", unit: "", op: ":", example: "64.130.", what: "an address, whole or by its start" },
  { name: "stake", kind: "number", group: "validator", label: "Stake", unit: "SOL", op: ">", example: "1000000", what: "stake, SOL" },
  { name: "finalization", kind: "number", group: "votes", label: "Finalization", unit: "%", op: "<", example: "50", what: "finalization certificates carrying its vote, %" },
  { name: "arrival", kind: "number", group: "votes", label: "Vote arrival", unit: "ms", op: ">", example: "300", what: "vote arrival, ms" },
];

const ONLY_WHAT: Readonly<Record<PeerFilter, string>> = {
  staked: "holds stake",
  rpc: "serves RPC",
  delinquent: "staked and not voting now",
};

export interface PeerCondition {
  key: KeyName;
  op: Op;
  /** A number in the key's unit; null for a list. */
  value: number | null;
  values: readonly string[];
}

export type PeerFiltered = { kind: "condition"; condition: PeerCondition } | { kind: "only"; only: PeerFilter };

/** What the peers offer to filter on. */
export interface PeerOffered {
  counts: Readonly<Record<PeerFilter, number>>;
  /** Each most common first, with how many peers hold it. */
  clients: readonly (readonly [client: string, peers: number])[];
  versions: readonly (readonly [version: string, peers: number])[];
  countries: readonly (readonly [code: string, peers: number])[];
  /** Validator names to suggest, the most staked first. */
  names: readonly string[];
  /** Whether the table has these columns. */
  finalization: boolean;
  arrivals: boolean;
}

function tally(peers: readonly GossipPeer[], of: (peer: GossipPeer) => string | null): (readonly [string, number])[] {
  const held = new Map<string, number>();
  for (const peer of peers) {
    const value = of(peer);
    if (value) held.set(value, (held.get(value) ?? 0) + 1);
  }
  return [...held].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export function peerOffered(peers: GossipPeer[]): PeerOffered {
  return {
    counts: filterCounts(peers),
    clients: tally(peers, (peer) => peer.clientName),
    versions: tally(peers, (peer) => peer.version),
    countries: tally(peers, (peer) => peer.country),
    names: peers
      .filter((peer) => peer.name)
      .sort((a, b) => b.stake - a.stake)
      .map((peer) => peer.name ?? ""),
    finalization: peers.some((peer) => peer.finalization !== null),
    arrivals: peers.some((peer) => peer.voteArrival !== null),
  };
}

function keyOffered(key: PeerKey, offered: PeerOffered): boolean {
  if (key.name === "finalization") return offered.finalization;
  return key.name !== "arrival" || offered.arrivals;
}

function keyNamed(name: string): PeerKey | undefined {
  const lower = name.toLowerCase();
  const wanted = lower === "country" ? "geo" : lower;
  return KEYS.find((key) => key.name === wanted);
}

/** A list key's typed values as the peers hold them, or why one could not be read. */
function listValues(key: PeerKey, typed: readonly string[], offered: PeerOffered): string[] | string {
  const values: string[] = [];
  for (const value of typed) {
    const lower = value.toLowerCase();
    let named = value;
    if (key.name === "client") {
      const known = offered.clients.find(([client]) => client.toLowerCase() === lower)?.[0];
      if (!known) return `No peer runs a client named ${value}.`;
      named = known;
    } else if (key.name === "version") {
      named = lower.replace(/^v/, "");
    } else if (key.name === "geo") {
      if (!/^[a-z]{2}$/.test(lower)) return `${value} is not a country code, e.g. geo:DE.`;
      named = lower.toUpperCase();
    }
    if (!values.includes(named)) values.push(named);
  }
  return values;
}

function readNumber(key: PeerKey, op: string, text: string): PeerFiltered | string {
  const value = Number(text.replace(/[,_]/g, "").replace(UNITS, ""));
  if (text === "" || !Number.isFinite(value)) {
    return `${key.name} needs a number, e.g. ${key.name}${key.op}${key.example}.`;
  }
  const compare = OPS[op];
  if (!compare) return `${key.name} takes >, >=, <, <= or =.`;
  return { kind: "condition", condition: { key: key.name, op: compare, value, values: [] } };
}

function readList(key: PeerKey, op: string, text: string, not: boolean, offered: PeerOffered): PeerFiltered | string {
  if (op !== ":" && op !== "=") return `${key.name} takes ":", e.g. ${key.name}:${key.example}.`;
  const typed = splitValues(text);
  if (typed.length === 0) return `${key.name} needs a value, e.g. ${key.name}:${key.example}.`;
  const values = listValues(key, typed, offered);
  if (typeof values === "string") return values;
  return { kind: "condition", condition: { key: key.name, op: not ? "is_not" : "is", value: null, values } };
}

const ONLY_NAMES = PEER_FILTERS.map((entry) => entry.filter);

function readOnly(text: string): PeerFiltered[] | string {
  const help = `is takes ${ONLY_NAMES.join(", ")}.`;
  const typed = splitValues(text);
  if (typed.length === 0) return help;
  const read: PeerFiltered[] = [];
  for (const value of typed) {
    const only = ONLY_NAMES.find((name) => name === value.toLowerCase());
    if (!only) return help;
    read.push({ kind: "only", only });
  }
  return read;
}

export interface PeerRead {
  /** What is left to search names, clients, IPs and keys by. */
  words: string;
  filters: PeerFiltered[];
  /** Why an expression could not be read, one line each. */
  errors: string[];
}

function readExpression(name: string, op: string, value: string, not: boolean, offered: PeerOffered): PeerFiltered[] | string {
  if (name.toLowerCase() === "is") {
    if (not) return "is cannot be negated.";
    return op === ":" || op === "=" ? readOnly(value) : `is takes ":", e.g. is:delinquent.`;
  }
  const key = keyNamed(name);
  if (!key) return `No filter named ${name}.`;
  if (!keyOffered(key, offered)) return `This validator has no ${key.name} to filter on.`;
  if (key.kind !== "list" && not) return `Only name, client, version, geo and ip take a leading "-".`;
  const read = key.kind === "list" ? readList(key, op, value, not, offered) : readNumber(key, op, value);
  return typeof read === "string" ? read : [read];
}

/** Reads the box: expressions become filters, the rest searches. */
export function readPeerSearch(text: string, offered: PeerOffered): PeerRead {
  const words: string[] = [];
  const filters: PeerFiltered[] = [];
  const errors: string[] = [];
  let cursor = 0;
  for (const match of text.matchAll(EXPRESSION)) {
    const at = match.index + (match[1]?.length ?? 0);
    // Inside the value before it, as in a quoted name.
    if (at < cursor) continue;
    words.push(text.slice(cursor, at));
    const end = match.index + match[0].length;
    // A quoted run can hold spaces, and what looks like an expression.
    const value = /^(?:"[^"]*"?|[^\s"])*/.exec(text.slice(end))?.[0] ?? "";
    cursor = end + value.length;
    const read = readExpression(match[3] ?? "", match[4] ?? "", value, match[2] === "-", offered);
    if (typeof read === "string") errors.push(read);
    else filters.push(...read);
  }
  words.push(text.slice(cursor));
  return { words: words.join(" ").trim().replace(/\s+/g, " "), filters, errors };
}

/** Filters taken in: a condition replaces any on the same key and comparison. */
export function withFilters(
  only: readonly PeerFilter[],
  where: readonly PeerCondition[],
  filters: readonly PeerFiltered[],
): { only: PeerFilter[]; where: PeerCondition[] } {
  const nextOnly = new Set(only);
  const nextWhere = [...where];
  for (const filter of filters) {
    if (filter.kind === "only") {
      nextOnly.add(filter.only);
      continue;
    }
    const { key, op } = filter.condition;
    const at = nextWhere.findIndex((held) => held.key === key && held.op === op);
    if (at === -1) nextWhere.push(filter.condition);
    else nextWhere[at] = filter.condition;
  }
  return { only: ONLY_NAMES.filter((name) => nextOnly.has(name)), where: nextWhere };
}

export interface PeerClock {
  slotMillis: number | null;
  arrivalKind: ArrivalKind;
}

/** In the key's unit, rounded where the column rounds, so a filter agrees with what it shows. */
function figure(key: KeyName, peer: GossipPeer, clock: PeerClock): number | null {
  switch (key) {
    case "stake":
      return peer.stake / LAMPORTS_PER_SOL;
    case "finalization":
      return peer.finalization === null ? null : Math.round(peer.finalization * 100);
    case "arrival": {
      const micros = arrivalMicros(peer.voteArrival, clock.arrivalKind);
      return micros === null ? null : Math.round(micros / 1000);
    }
    default:
      return null;
  }
}

export function peerHolds(condition: PeerCondition, peer: GossipPeer, clock: PeerClock): boolean {
  if (condition.value !== null) return compared(condition.op, figure(condition.key, peer, clock), condition.value);
  if (condition.key === "name") return nameHolds(condition, peer.name, peer.identity);
  const { key, values } = condition;
  const held = key === "client" ? peer.clientName : key === "version" ? peer.version : key === "geo" ? peer.country : peer.ip;
  if (!held) return false;
  const named =
    key === "version"
      ? versionNamed(condition, held)
      : key === "ip"
        ? values.some((value) => held.startsWith(value))
        : values.includes(held);
  return condition.op === "is_not" ? !named : named;
}

/** The comparison a pill moves to on a click. */
export function nextPeerOp(condition: PeerCondition): Op {
  const ops = condition.value === null ? LIST_OPS : NUMBER_OPS;
  return ops[(ops.indexOf(condition.op) + 1) % ops.length] ?? condition.op;
}

export function peerConditionParts(condition: PeerCondition): { label: string; op: string; value: string; unit: string } {
  const key = keyNamed(condition.key);
  const value =
    condition.value === null
      ? condition.values.join(", ")
      : condition.value.toLocaleString("en-US", { maximumFractionDigits: 6 });
  return { label: key?.label ?? condition.key, op: OP_LABELS[condition.op], value, unit: key?.unit ?? "" };
}

/** A condition's value as typed in its pill. */
export function peerValueText(condition: PeerCondition): string {
  if (condition.value === null) return condition.values.map((value) => (value.includes(",") ? `"${value}"` : value)).join(", ");
  return String(condition.value);
}

/** The condition with the value typed in its pill, or why that cannot be read. */
export function withPeerValue(condition: PeerCondition, text: string, offered: PeerOffered): PeerCondition | string {
  const expression =
    condition.value === null
      ? `${condition.op === "is_not" ? "-" : ""}${condition.key}:${splitValues(text)
          .map((value) => `"${value}"`)
          .join(",")}`
      : `${condition.key}${TYPED_OPS[condition.op] ?? "="}${text.replace(/\s+/g, "")}`;
  const read = readPeerSearch(expression, offered);
  const [error] = read.errors;
  if (error) return error;
  const [filter] = read.filters;
  return filter?.kind === "condition" ? filter.condition : `${condition.key} needs a value.`;
}

const peersText = (peers: number): string => `${count(peers)} ${peers === 1 ? "peer" : "peers"}`;

function valuesOf(key: PeerKey, offered: PeerOffered): (readonly [value: string, what: string])[] {
  switch (key.name) {
    case "name":
      return offered.names.map((name) => [name, ""]);
    case "client":
      return offered.clients.map(([client, peers]) => [client, peersText(peers)]);
    case "version":
      return offered.versions.map(([version, peers]) => [version, peersText(peers)]);
    case "geo":
      return offered.countries.map(([code, peers]) => [code, `${countryName(code)}, ${peersText(peers)}`]);
    default:
      return [];
  }
}

/** What to offer for the word being typed, the text up to the caret: names, then their values. */
export function peerSuggest(before: string, offered: PeerOffered): Suggestion[] {
  const word = /(?:^|\s)(\S*)$/.exec(before)?.[1] ?? "";
  const bare = /^(-?)([a-z]+)$/i.exec(word);
  if (bare) {
    const [, not = "", typed = ""] = bare;
    const lower = typed.toLowerCase();
    const keys = KEYS.filter(
      (key) => key.name.startsWith(lower) && keyOffered(key, offered) && (!not || key.kind === "list"),
    ).flatMap((key): Suggestion[] => {
      const ghost = `${key.example}${key.unit ? ` ${key.unit}` : ""}`;
      return key.kind === "list"
        ? [{ insert: `${not}${key.name}:`, ghost: key.example, what: key.what }]
        : [
            { insert: `${key.name}>`, ghost, what: key.what },
            { insert: `${key.name}<`, ghost, what: key.what },
          ];
    });
    const only = not
      ? []
      : ONLY_NAMES.filter((name) => name.startsWith(lower)).map((name) => ({
          insert: `is:${name}`,
          ghost: "",
          what: `${ONLY_WHAT[name]}, ${peersText(offered.counts[name])}`,
        }));
    return [...keys, ...only].slice(0, MAX_SUGGESTIONS);
  }
  return suggestValues(word, (name) => {
    const key = keyNamed(name);
    if (key) return key.kind === "list" && keyOffered(key, offered) ? valuesOf(key, offered) : null;
    if (name.toLowerCase() !== "is") return null;
    return ONLY_NAMES.map((only) => [only, `${ONLY_WHAT[only]}, ${peersText(offered.counts[only])}`]);
  });
}

export interface PeerRow extends SearchRow {
  group: PeerGroup;
  action: PeerFiltered | TypeAction;
}

/** Every expression the box takes, offered when nothing is typed: the kinds of peer first, each one row. */
export function peerBrowse(offered: PeerOffered, only: readonly PeerFilter[]): PeerRow[] {
  const kinds = ONLY_NAMES.filter((name) => !only.includes(name)).map(
    (name): PeerRow => ({
      group: "only",
      syntax: `is:${name}`,
      figure: count(offered.counts[name]),
      what: ONLY_WHAT[name],
      action: { kind: "only", only: name },
    }),
  );
  const keys = KEYS.filter((key) => keyOffered(key, offered)).map((key): PeerRow => {
    if (key.kind === "list") {
      const text = `${key.name}:`;
      // An empty figure keeps the descriptions in line under the counted rows.
      return { group: key.group, syntax: text, figure: "", what: key.what, action: { kind: "type", text, from: text.length } };
    }
    const read = readNumber(key, key.op, key.example);
    const syntax = `${key.name}${key.op}${key.example}`;
    const action: PeerRow["action"] =
      typeof read === "string" ? { kind: "type", text: syntax, from: key.name.length + 1 } : read;
    return { group: key.group, syntax, figure: "", what: key.what, action };
  });
  return [...kinds, ...keys];
}
