import { memo, useCallback, useEffect, useMemo, useRef, useState, type ReactElement, type RefObject } from "react";
import { count, percent, shortKey } from "../format";
import { LOW_SHARE } from "../finalization";
import { delinquentTitle } from "../misses";
import {
  ariaSort,
  arrivalMedian,
  arrivalMicros,
  arrivalText,
  arrivalTitle,
  DROP_WARN_SHARE,
  dropPercent,
  dropShare,
  entryRows,
  FIRST_SORT,
  gossipPeers,
  gossipVerdict,
  inFilter,
  matchesSearch,
  MESSAGE_LABELS,
  nextSort,
  PEER_FILTERS,
  rate,
  sortPeers,
  stakeText,
  timeParts,
  type EntryRow,
  type GossipPeer,
  type PeerColumn,
  type ArrivalKind,
  type PeerFilter,
  type PeerSort,
} from "../gossip";
import { useAlpenglow } from "../consensus";
import { unitFor } from "../network";
import {
  nextPeerOp,
  peerBrowse,
  peerConditionParts,
  peerHolds,
  peerOffered,
  peerSuggest,
  PEER_TITLES,
  peerValueText,
  readPeerSearch,
  withFilters,
  withPeerValue,
  type PeerCondition,
  type PeerRow,
} from "../peerSyntax";
import type { Gossip, GossipPeers } from "../types";
import { useNarrow } from "../narrow";
import { usePolledRequest } from "../usePolledRequest";
import { useStore } from "../useStore";
import { CertificatesSection } from "./CertificatesSection";
import { CertFormationCard, type Pick } from "./CertFormationCard";
import { FinalizationCard } from "./FinalizationCard";
import { Copyable } from "./Copyable";
import { IpAddress } from "./IpAddress";
import { Card, Meter, Stat } from "./primitives";
import { SyntaxSearch, type Pill } from "./ScheduleTools";

/** The server gathers the list on its five-second tick, and only while it is being asked for. */
const POLL_MS = 5_000;

const ROWS_PER_PAGE = 200;

/** Held by the page only: the search does not go in the address. */
interface PeerSearch {
  text: string;
  only: readonly PeerFilter[];
  where: readonly PeerCondition[];
  picked: Pick | null;
}

const NO_SEARCH: PeerSearch = { text: "", only: [], where: [], picked: null };

const conditionKey = (condition: PeerCondition): string => `${condition.key}${condition.op}${peerValueText(condition)}`;

export function GossipPage(): ReactElement {
  const store = useStore();
  const gossip = store.get("summary", "gossip");
  const egress = store.get("summary", "network_egress")?.gossip_per_second ?? null;
  const ours = store.get("summary", "identity_key") ?? null;
  const slotNanos =
    store.get("summary", "observed_slot_duration_nanos") ?? store.get("summary", "estimated_slot_duration_nanos");
  // Rounded so the table redraws when the rate moves, not on every tick.
  const slotMillis = slotNanos ? Math.round(slotNanos / 1e7) * 10 : null;
  const list = usePolledRequest("peers.gossip", POLL_MS);
  const verdict = gossipVerdict(gossip, list);
  const alpenglow = useAlpenglow();
  const [search, setSearch] = useState(NO_SEARCH);
  const searchBox = useRef<HTMLInputElement>(null);
  const listSearched = (next: PeerSearch): void => {
    setSearch(next);
    searchBox.current?.closest("section")?.scrollIntoView({ block: "start", behavior: "smooth" });
  };
  const listPicked = (picked: Pick) => listSearched({ ...NO_SEARCH, picked });
  const findInPeers = (text: string) => listSearched({ ...NO_SEARCH, text });

  return (
    <>
      <h1 className="verdict">
        <span className={`verdict-dot tone-${verdict.tone}`} aria-hidden="true" />
        {verdict.headline}
      </h1>
      <p className="verdict-sub">{verdict.detail}</p>
      {gossip && (
        <>
          <TableCard table={gossip.table} />
          <div className="grid">
            <MessagesCard messages={gossip.messages} egress={egress} />
            <EntriesCard entries={gossip.entries} />
            <PressureCard pressure={gossip.pressure} packets={gossip.packets_per_second} />
            <TimeCard time={gossip.time} />
          </div>
        </>
      )}
      {alpenglow && <FinalizationCard />}
      {alpenglow && <CertFormationCard peers={list} onPick={listPicked} />}
      {alpenglow && <CertificatesSection peers={list} onFind={findInPeers} />}
      <PeersCard
        list={list}
        search={search}
        onSearch={setSearch}
        searchBox={searchBox}
        ours={ours}
        slotMillis={slotMillis}
      />
    </>
  );
}

/** Asked for while the page is open; a closed page stops the server gathering it. */

function TableCard({ table }: { table: Gossip["table"] }) {
  return (
    <Card title="Gossip table" className="stat-grid gossip-stats">
      <Stat
        label="entries held"
        value={count(table.entries)}
        sub={`from ${count(table.nodes)} nodes, ${count(table.staked_nodes)} staked`}
      />
      <Stat
        label={`distinct pubkeys, of ${count(table.pubkey_capacity)}`}
        explain="Past this many, gossip trims its table and keeps the staked nodes."
        value={count(table.pubkeys)}
        sub={<Meter fraction={table.pubkeys / table.pubkey_capacity} />}
      />
      <Stat label="entries expired per second" value={rate(table.expired_per_second)} sub="aged out after their timeout" />
      <Stat
        label="entries evicted by trimming"
        value={count(table.evicted_last_minute)}
        sub="last minute"
        tone={table.evicted_last_minute > 0 ? "warn" : undefined}
      />
    </Card>
  );
}

function MessagesCard({ messages, egress }: { messages: Gossip["messages"]; egress: number | null }) {
  const peak = Math.max(1, ...messages.flatMap((message) => [message.received, message.sent]));
  const unit = egress === null ? null : unitFor(egress);
  return (
    <Card title="Messages" aside="packets per second" className="gossip-card">
      <div className="gossip-scroll">
        <table className="gossip-table is-messages is-fixed">
          {/* Fixed, so the bars start in the same place whatever the figures. */}
          <colgroup>
            <col className="gossip-col-label" />
            <col className="gossip-col-num" />
            <col />
            <col className="gossip-col-num" />
            <col />
          </colgroup>
          <thead>
            <tr>
              <th>Type</th>
              <th className="is-num">In</th>
              <th aria-hidden="true" />
              <th className="is-num">Out</th>
              <th aria-hidden="true" />
            </tr>
          </thead>
          <tbody>
            {messages.map((message) => (
              <tr key={message.kind}>
                <td>{MESSAGE_LABELS[message.kind]}</td>
                <td className="is-num">{rate(message.received)}</td>
                <td className="gossip-bar-cell" aria-hidden="true">
                  {message.received > 0 && <i className="gossip-bar" style={{ width: `${(message.received / peak) * 100}%` }} />}
                </td>
                <td className="is-num">{rate(message.sent)}</td>
                <td className="gossip-bar-cell" aria-hidden="true">
                  {message.sent > 0 && <i className="gossip-bar is-out" style={{ width: `${(message.sent / peak) * 100}%` }} />}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {egress !== null && unit && (
        <div className="card-footnote">
          Gossip egress {(egress / unit.divisor).toFixed(1)} {unit.unit}/s
        </div>
      )}
    </Card>
  );
}

function EntriesCard({ entries }: { entries: Gossip["entries"] }) {
  const { shown, others } = entryRows(entries.types);
  return (
    <Card title="Entries" aside="per second" className="gossip-card">
      <div className="stat-grid gossip-entry-stats">
        <Stat label="accepted by push" value={rate(entries.accepted_push)} sub={`${rate(entries.accepted_pull)} by pull`} />
        <Stat
          label="duplicates by push"
          value={rate(entries.duplicate_push)}
          sub={`${rate(entries.redundant_pull)} redundant by pull`}
        />
        <Stat
          label="rejected by push"
          value={rate(entries.rejected_push)}
          sub={`${rate(entries.rejected_pull)} by pull`}
        />
      </div>
      <div className="gossip-scroll">
        <table className="gossip-table is-entries is-fixed">
          <colgroup>
            <col />
            <col className="gossip-col-num" />
            <col className="gossip-col-num" />
            <col className="gossip-col-wide-num" />
          </colgroup>
          <thead>
            <tr>
              <th>Entry type</th>
              <th className="is-num">Push</th>
              <th className="is-num">Pull</th>
              <th className="is-num">Rejected</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((row) => (
              <EntryLine key={row.label} row={row} />
            ))}
            {others && <EntryLine row={others} dim />}
            {shown.length === 0 && (
              <tr>
                <td colSpan={4} className="gossip-none">
                  No entries in the window.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function EntryLine({ row, dim }: { row: EntryRow; dim?: boolean }) {
  return (
    <tr className={dim ? "is-dim" : undefined}>
      <td title={row.label}>{row.label}</td>
      <td className="is-num">{rate(row.push)}</td>
      <td className="is-num">{rate(row.pull)}</td>
      <td className="is-num">{rate(row.rejected)}</td>
    </tr>
  );
}

/** A row with a share warns from `DROP_WARN_SHARE` of gossip's packets; the rest warn on any. */
const PRESSURE_ROWS: { key: keyof Gossip["pressure"]; label: string; warn: boolean; share?: true }[] = [
  { key: "dropped_in", label: "Packets dropped coming in", warn: true, share: true },
  { key: "dropped_out", label: "Packets dropped going out", warn: true, share: true },
  { key: "pull_no_budget", label: "Pull requests refused, no budget", warn: true },
  { key: "pull_scan_exhausted", label: "Pull scans cut short", warn: true },
  { key: "other_shred_version", label: "From another shred version", warn: false },
  { key: "ping_check_failed", label: "Failed ping checks", warn: false },
  { key: "unverified_addresses", label: "Contact records from unverified addresses", warn: false },
  { key: "bad_prune_destination", label: "Prunes to a bad destination", warn: false },
];

function PressureCard({ pressure, packets }: { pressure: Gossip["pressure"]; packets: number }) {
  return (
    <Card title="Pressure and rejects" aside="per second" className="gossip-rows">
      {PRESSURE_ROWS.map((row) => {
        const value = pressure[row.key];
        const share = row.share && value > 0 ? dropShare(value, packets) : null;
        const warn = row.warn && (share === null ? value > 0 : share >= DROP_WARN_SHARE);
        return (
          <div key={row.key} className="gossip-row">
            <span>{row.label}</span>
            <span className={`gossip-row-value${warn ? " tone-warn" : ""}`}>
              {rate(value)}
              {share !== null && <span className="gossip-row-share">{dropPercent(share)}</span>}
            </span>
          </div>
        );
      })}
    </Card>
  );
}

function TimeCard({ time }: { time: Gossip["time"] }) {
  const { parts, total } = timeParts(time);
  return (
    <Card title="Where gossip spends its time" aside={`${count(Math.round(total))} ms per second`}>
      <div className="replay-bar" aria-hidden="true">
        {parts.map((part, index) => (
          <i key={part.key} className={`is-${index + 1}`} style={{ flexGrow: part.share }} />
        ))}
      </div>
      <div className="replay-legend">
        {parts.map((part, index) => (
          <div key={part.key} className="replay-item">
            <i className={`replay-swatch is-${index + 1}`} aria-hidden="true" />
            <span className="replay-name">{part.label}</span>
            <span className="replay-value">{count(Math.round(part.millis))} ms</span>
            <span className="replay-share">{percent(part.share, 0)}</span>
          </div>
        ))}
      </div>
    </Card>
  );
}

/** Past this many times the middle peer's, a validator's votes are marked as arriving late. */
const LATE_ARRIVAL = 2;

const HEADINGS: { column: PeerColumn; label: string; numeric: boolean }[] = [
  { column: "stake", label: "Stake, SOL", numeric: true },
  { column: "name", label: "Validator", numeric: false },
  { column: "finalization", label: "Finalization", numeric: true },
  { column: "arrival", label: "Vote arrival", numeric: true },
  { column: "client", label: "Client", numeric: false },
  { column: "ip", label: "IP", numeric: false },
  { column: "rpc", label: "RPC", numeric: true },
];

function PeersCard({
  list,
  search,
  onSearch,
  searchBox,
  ours,
  slotMillis,
}: {
  list: GossipPeers | null;
  search: PeerSearch;
  onSearch: (search: PeerSearch) => void;
  searchBox: RefObject<HTMLInputElement>;
  ours: string | null;
  slotMillis: number | null;
}) {
  const { text, only, where, picked } = search;
  const [sort, setSort] = useState<PeerSort>(FIRST_SORT);
  const onSort = useCallback((column: PeerColumn) => setSort((was) => nextSort(was, column)), []);
  const arrivalKind: ArrivalKind = picked?.kind ?? "notarize";
  const peers = useMemo(() => (list ? gossipPeers(list) : []), [list]);
  const offered = useMemo(() => peerOffered(peers), [peers]);
  const words = useMemo(() => readPeerSearch(text, offered).words, [text, offered]);
  const rows = useMemo(() => {
    const clock = { slotMillis, arrivalKind };
    return sortPeers(
      peers.filter(
        (peer) =>
          (!picked || picked.identities.has(peer.identity)) &&
          only.every((filter) => inFilter(peer, filter)) &&
          where.every((condition) => peerHolds(condition, peer, clock)) &&
          matchesSearch(peer, words),
      ),
      sort,
      arrivalKind,
    );
  }, [peers, picked, only, where, words, sort, slotMillis, arrivalKind]);
  const searched = picked !== null || only.length > 0 || where.length > 0 || words !== "";

  const phone = useNarrow();
  const [editing, setEditing] = useState<string | null>(null);
  const [syntaxError, setSyntaxError] = useState<string | null>(null);
  const editPill = (key: string | null): void => {
    setEditing(key);
    setSyntaxError(null);
  };
  const suggestFor = useCallback((before: string) => peerSuggest(before, offered), [offered]);
  const browseRows = useMemo(() => peerBrowse(offered, only), [offered, only]);
  const addFromBox = (): void => {
    const read = readPeerSearch(text, offered);
    const [error] = read.errors;
    if (error) {
      setSyntaxError(error);
      return;
    }
    if (read.filters.length > 0) onSearch({ ...search, ...withFilters(only, where, read.filters), text: read.words });
  };
  // A picked condition replaces any on the same key and comparison, its value open to type.
  const pickRow = (row: PeerRow): void => {
    if (row.action.kind === "type") return;
    const next = withFilters(only, where, [row.action]);
    onSearch({ ...search, ...next });
    if (row.action.kind === "condition") {
      const { key, op } = row.action.condition;
      editPill(`where:${next.where.findIndex((held) => held.key === key && held.op === op)}`);
    }
  };
  const putCondition = (at: number, condition: PeerCondition): void =>
    onSearch({ ...search, where: where.map((held, index) => (index === at ? condition : held)) });
  const pills: Pill[] = [
    ...(picked
      ? [{ kind: "plain", key: "picked", label: picked.label, onRemove: () => onSearch({ ...search, picked: null }) } as const]
      : []),
    ...PEER_FILTERS.filter((entry) => only.includes(entry.filter)).map(
      (entry): Pill => ({
        kind: "plain",
        key: `only:${entry.filter}`,
        label: entry.label,
        onRemove: () => onSearch({ ...search, only: only.filter((held) => held !== entry.filter) }),
      }),
    ),
    ...where.map(
      (condition, at): Pill => ({
        kind: "condition",
        key: `where:${at}`,
        ...peerConditionParts(condition),
        draft: peerValueText(condition),
        onCycle: () => putCondition(at, { ...condition, op: nextPeerOp(condition) }),
        onValue: (typed) => {
          const read = withPeerValue(condition, typed, offered);
          if (typeof read === "string") {
            setSyntaxError(read);
            return read;
          }
          editPill(null);
          putCondition(at, read);
          return null;
        },
        onRemove: () => {
          editPill(null);
          onSearch({ ...search, where: where.filter((_, index) => index !== at) });
        },
      }),
    ),
  ];
  const editingOpen = pills.some((pill) => pill.key === editing);

  return (
    <Card
      title="Peers"
      aside={
        <span aria-live="polite">
          {list && (searched ? `${count(rows.length)} of ` : "")}
          {list && `${count(peers.length)} ${peers.length === 1 ? "node" : "nodes"}`}
        </span>
      }
      className="gossip-peers"
    >
      <div className="gossip-tools">
        <SyntaxSearch
          text={text}
          placeholder={
            pills.length > 0
              ? phone
                ? "Filter or search…"
                : "Add another filter or search…"
              : phone
                ? "Search or filter: geo:DE"
                : "Search or filter: name, client:Agave, finalization<50, geo:DE,NL"
          }
          label="Search peers by name, client, IP or key, or filter, e.g. geo:DE"
          searches="names, clients, IPs and keys"
          suggestFor={suggestFor}
          rows={browseRows}
          titles={PEER_TITLES}
          foot=""
          pills={pills}
          editing={editing}
          inputRef={searchBox}
          onText={(next) => {
            setSyntaxError(null);
            onSearch({ ...search, text: next });
          }}
          onAdd={addFromBox}
          onBrowse={pickRow}
          onEdit={editPill}
          onClear={() => {
            editPill(null);
            onSearch(NO_SEARCH);
          }}
        />
      </div>
      {(syntaxError || editingOpen) && (
        <div className="schedule-from">
          {syntaxError && <span className="schedule-warn">{syntaxError}</span>}
          {editingOpen && <span className="schedule-from-hint">Enter applies, Esc cancels.</span>}
        </div>
      )}
      {list === null ? (
        <div className="gossip-empty">Gathering the peer list…</div>
      ) : (
        // Keyed so a new filter, search or order starts again from the top.
        <PeersTable
          key={`${picked?.label}|${only.join()}|${where.map(conditionKey).join()}|${words}|${sort.column}|${sort.reversed}`}
          rows={rows}
          sort={sort}
          onSort={onSort}
          ours={ours}
          slotMillis={slotMillis}
          searched={searched}
          arrivalEpoch={list.vote_arrival_epoch ?? null}
          arrivalKind={arrivalKind}
        />
      )}
    </Card>
  );
}

/** Kept out of the page's render, which runs on every store update; mainnet has thousands of rows. */
const PeersTable = memo(function PeersTable({
  rows,
  sort,
  onSort,
  ours,
  slotMillis,
  searched,
  arrivalEpoch,
  arrivalKind,
}: {
  rows: GossipPeer[];
  sort: PeerSort;
  onSort: (column: PeerColumn) => void;
  ours: string | null;
  slotMillis: number | null;
  searched: boolean;
  arrivalEpoch: number | null;
  arrivalKind: ArrivalKind;
  /** On 4.3, whose arrival averages lean late. */
}) {
  const [limit, setLimit] = useState(ROWS_PER_PAGE);
  // Only under Alpenglow, where the shares exist, and the arrivals once votor has reported an epoch.
  const finalization = useMemo(() => rows.some((row) => row.finalization !== null), [rows]);
  const arrivals = useMemo(() => rows.some((row) => row.voteArrival !== null), [rows]);
  const lateAfter = useMemo(() => {
    const median = arrivalMedian(rows, arrivalKind);
    return median === null ? null : median * LATE_ARRIVAL;
  }, [rows, arrivalKind]);
  const headings = HEADINGS.filter(
    (heading) => (finalization || heading.column !== "finalization") && (arrivals || heading.column !== "arrival"),
  );
  const scroller = useRef<HTMLDivElement>(null);
  const end = useRef<HTMLTableRowElement>(null);
  const more = limit < rows.length;

  // Observed again after each page, so a page that leaves the marker in view loads the next.
  useEffect(() => {
    const root = scroller.current;
    const marker = end.current;
    if (!more || !root || !marker) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) setLimit((was) => was + ROWS_PER_PAGE);
      },
      { root, rootMargin: "0px 0px 400px 0px" },
    );
    observer.observe(marker);
    return () => observer.disconnect();
  }, [more, limit]);

  return (
    <div className={`gossip-peers-scroll${searched ? " is-searched" : ""}`} ref={scroller}>
      <table className="gossip-table is-peers">
        <thead>
          <tr>
            {headings.map((heading) => {
              const order = ariaSort(sort, heading.column);
              return (
                <th
                  key={heading.column}
                  className={heading.numeric ? "is-num" : heading.column === "name" ? "is-name" : undefined}
                  aria-sort={order}
                >
                  <button
                    type="button"
                    className="gossip-sort"
                    onClick={() => onSort(heading.column)}
                    title={heading.column === "arrival" ? arrivalHeadingTitle(arrivalEpoch, arrivalKind) : undefined}
                  >
                    {heading.column === "arrival" && arrivalKind === "finalize" ? "Finalize arrival" : heading.label}
                    {/* Out of the button's name; `aria-sort` says the same. */}
                    {order && <span aria-hidden="true">{order === "ascending" ? " ↑" : " ↓"}</span>}
                  </button>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, limit).map((peer) => (
            <PeerLine
              key={peer.identity}
              peer={peer}
              ours={peer.identity === ours}
              slotMillis={slotMillis}
              finalization={finalization}
              arrivals={arrivals}
              arrivalEpoch={arrivalEpoch}
              arrivalKind={arrivalKind}
              lateAfter={lateAfter}
            />
          ))}
          {rows.length === 0 && (
            <tr>
              <td colSpan={headings.length} className="gossip-none">
                {searched ? "No peer matches this search." : "No peer listed."}
              </td>
            </tr>
          )}
          {more && <tr ref={end} aria-hidden="true" />}
        </tbody>
      </table>
    </div>
  );
});

const PeerLine = memo(function PeerLine({
  peer,
  ours,
  slotMillis,
  finalization,
  arrivals,
  arrivalEpoch,
  arrivalKind,
  lateAfter,
}: {
  peer: GossipPeer;
  ours: boolean;
  slotMillis: number | null;
  finalization: boolean;
  arrivals: boolean;
  arrivalEpoch: number | null;
  arrivalKind: ArrivalKind;
  lateAfter: number | null;
}) {
  const arrival = peer.voteArrival;
  const arrivalAt = arrivalMicros(arrival, arrivalKind);
  return (
    <tr className={[ours ? "is-ours" : "", peer.delinquent ? "is-delinquent" : ""].join(" ").trim() || undefined}>
      <td className="is-num">{stakeText(peer.stake)}</td>
      <td className="is-name">
        <div className="gossip-who">
          <span className="gossip-name">
            <span className={`gossip-name-text${peer.name ? "" : " is-dim"}`} title={peer.name ?? undefined}>
              {peer.name ?? "unnamed"}
            </span>
            {ours && <span className="gossip-ours">ours</span>}
            {peer.delinquent && <DelinquentTag behind={peer.voteBehind} slotMillis={slotMillis} />}
          </span>
          <span className="gossip-key-line">
            <Copyable text={peer.identity} label={shortKey(peer.identity, 4, 4)} className="gossip-key" />
            {/* On a phone the tag moves here, where it does not cut the name. */}
            {peer.delinquent && <DelinquentTag behind={peer.voteBehind} slotMillis={slotMillis} />}
          </span>
        </div>
      </td>
      {finalization && (
        <td
          className={`is-num${peer.finalization !== null && peer.finalization < LOW_SHARE ? " tone-warn" : ""}`}
        >
          {percent(peer.finalization, 0)}
        </td>
      )}
      {arrivals && (
        <td
          className={`is-num${arrivalAt !== null && lateAfter !== null && arrivalAt > lateAfter ? " tone-warn" : ""}`}
          title={
            arrival
              ? arrivalTitle(arrival, arrivalEpoch, arrivalKind)
              : ours
                ? "Our own votes are not timed."
                : undefined
          }
        >
          {arrival ? arrivalText(arrival, arrivalKind) : "—"}
        </td>
      )}
      <td>{peer.client || "—"}</td>
      <td className="is-mono">
        {peer.ip ? (
          <IpAddress ip={peer.ip} country={peer.country} />
        ) : (
          "—"
        )}
      </td>
      <td className="is-num">{peer.rpc ?? "—"}</td>
    </tr>
  );
});

function DelinquentTag({ behind, slotMillis }: { behind: number | null; slotMillis: number | null }) {
  return (
    <span className="tag-delinquent" title={delinquentTitle(behind, slotMillis === null ? undefined : slotMillis * 1e6)}>
      delinquent
    </span>
  );
}

function arrivalHeadingTitle(epoch: number | null, kind: ArrivalKind): string {
  const over = epoch === null ? "the last epoch" : `epoch ${count(epoch)}`;
  return `When each validator's ${kind} votes reached us after the slot began, on average over ${over}.`;
}
