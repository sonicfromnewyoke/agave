import { useCallback, useEffect, useMemo, useRef, useState, type ReactElement } from "react";
import { buildLabel, count, percent, shortKey } from "../format";
import type { Requests } from "../store";
import type { GossipPeers, MissPlace } from "../types";
import { readCertificatesOpen, writeCertificatesOpen } from "../layout";
import { useStore } from "../useStore";
import { delinquentText, noGossipText, placeExplain } from "../misses";
import {
  averageLeftOut,
  leftUsOutLine,
  leftUsOutRule,
  WRITER_KINDS,
  writerFigures,
  writerKinds,
  WRITTEN_KINDS,
  writtenFigures,
  writtenKinds,
  writtenLine,
  WRITTEN_RULE,
  type WriterFigure,
  type WriterKind,
  type WrittenFigure,
  type WrittenKind,
} from "../written";
import { Copyable } from "./Copyable";
import { IpAddress } from "./IpAddress";
import { Hinted } from "./MissesPanel";
import { WriterName } from "./WriterName";

/** Inside the validator's 30 s asked-for window, so both lists are rebuilt every five seconds while mounted. */
const POLL_MS = 15_000;

const LINE_TITLE =
  "Everyone is every validator paid in at least nine of ten of the epoch's certificates so far.";

const KIND_WORD: Record<WrittenKind, string> = {
  worse: "fare worse in ours",
  missing: "missing everywhere",
  delinquent: "delinquent",
  "no-gossip": "no gossip",
};

const KIND_TITLE: Record<WrittenKind, string> = {
  worse: "Left out of our certificates far more often than of everyone's.",
  missing: "Left out of nearly every certificate from any writer.",
  delinquent: "Not voting now, which accounts for being left out.",
  "no-gossip": "Not heard by this node over gossip in five minutes.",
};

/** The bar's class for each kind; faring worse keeps the default. */
const KIND_BAR: Record<WrittenKind, string | undefined> = {
  worse: undefined,
  missing: "is-down",
  delinquent: "is-delinquent",
  "no-gossip": "is-no-gossip",
};

const WRITER_WORD: Record<WriterKind, string> = {
  worse: "leave us out more than most",
  "no-gossip": "no gossip",
};

const WRITER_TITLE: Record<WriterKind, string> = {
  worse: "Its certificates left us out far more often than the average.",
  "no-gossip": "Not heard by this node over gossip in five minutes.",
};

type Side = "ours" | "theirs";

/** Both sides of the epoch's reward certificates: whom ours left out, and whose left us out. */
export function CertificatesSection({
  peers,
  onFind,
}: {
  peers: GossipPeers | null;
  onFind: (identity: string) => void;
}): ReactElement {
  const [side, setSide] = useState<Side>("ours");
  const [open, setOpen] = useState(readCertificatesOpen);
  const toggle = useCallback(() => {
    const next = !open;
    setOpen(next);
    writeCertificatesOpen(next);
  }, [open]);

  return (
    <section className="misses-panel written" aria-label="Reward certificates this epoch">
      <div className="written-top">
        <b>Reward certificates this epoch</b>
        <div className="toggle-group" role="group" aria-label="Which side to show">
          <button type="button" aria-pressed={side === "ours"} onClick={() => setSide("ours")}>
            Left out by us
          </button>
          <button type="button" aria-pressed={side === "theirs"} onClick={() => setSide("theirs")}>
            Left us out
          </button>
        </div>
      </div>
      {side === "ours" ? (
        <OursSide open={open} onToggle={toggle} onFind={onFind} />
      ) : (
        <TheirsSide peers={peers} open={open} onToggle={toggle} onFind={onFind} />
      )}
    </section>
  );
}

/** Asked for while mounted; a reply after unmounting is dropped. */
function usePolled<R extends "summary.written" | "summary.misses">(
  route: R,
): { value: Requests[R]["reply"] | null; failed: boolean } {
  const store = useStore();
  const [value, setValue] = useState<Requests[R]["reply"] | null>(null);
  const [failed, setFailed] = useState(false);
  const live = useRef(true);
  // Asked once the socket is open, and again on each new one.
  const open = store.getConnection() === "open";

  useEffect(() => {
    if (!open) return;
    live.current = true;
    const load = () => {
      store.request(route, {}).then(
        (got) => {
          if (!live.current) return;
          setValue(got);
          setFailed(false);
        },
        () => {
          if (live.current) setFailed(true);
        },
      );
    };
    load();
    const timer = setInterval(load, POLL_MS);
    return () => {
      live.current = false;
      clearInterval(timer);
    };
  }, [store, route, open]);

  return { value, failed };
}

function OursSide({
  open,
  onToggle,
  onFind,
}: {
  open: boolean;
  onToggle: () => void;
  onFind: (identity: string) => void;
}) {
  const store = useStore();
  const slot = store.get("summary", "completed_slot");
  const slotNanos = store.get("summary", "observed_slot_duration_nanos") ?? undefined;
  const serverNanos = store.get("summary", "server_time_nanos");
  const nowMillis = serverNanos === undefined ? undefined : serverNanos / 1e6;
  const { value: list, failed } = usePolled("summary.written");
  const [filter, setFilter] = useState<WrittenKind | null>(null);

  const figures = list ? writtenFigures(list) : [];
  const kinds = writtenKinds(figures);
  const shown = figures.filter((figure) => filter === null || figure.kind === filter);

  return (
    <>
      <div className="written-head">
        {list === null && <span>{failed ? "could not be read" : "reading…"}</span>}
        {list !== null && <span title={LINE_TITLE}>{writtenLine(list)}</span>}
        {list !== null && (
          <span className="misses-legend written-legend">
            {WRITTEN_KINDS.filter((kind) => kinds[kind] > 0).map((kind) => (
              <button
                key={kind}
                type="button"
                className={`explain-trigger${filter === kind ? " is-on" : ""}`}
                title={KIND_TITLE[kind]}
                onClick={() => setFilter(filter === kind ? null : kind)}
              >
                <i className={`misses-swatch is-${kind}`} />
                <span>
                  <b>{count(kinds[kind])}</b> {KIND_WORD[kind]}
                </span>
              </button>
            ))}
          </span>
        )}
        {figures.length > 0 && (
          <button type="button" className="stat-trigger" onClick={onToggle}>
            {open ? "hide the list" : "show the list"}
          </button>
        )}
      </div>
      {list !== null && <Rule text={WRITTEN_RULE} hint={null} />}
      {open && list !== null && figures.length > 0 && (
        <div className="misses-table">
          <div className="written-row is-head">
            <span>validator</span>
            <span>ip</span>
            <span title="Our certificates this epoch that did not pay it.">left out of ours</span>
            <span>share</span>
            <span title="Certificates from any writer that did not pay it, as a share of all seen.">network-wide</span>
            <span title="Its share of ours as a bar, its share network-wide as the mark.">ours against the network</span>
          </div>
          {shown.map((figure) => (
            <WrittenRowView
              key={figure.row.identity}
              figure={figure}
              written={list.certificates}
              slot={slot}
              slotNanos={slotNanos}
              nowMillis={nowMillis}
              onFind={onFind}
            />
          ))}
        </div>
      )}
    </>
  );
}

/** Module-level, or it remounts every tick. */
function WrittenRowView({
  figure,
  written,
  slot,
  slotNanos,
  nowMillis,
  onFind,
}: {
  figure: WrittenFigure;
  written: number;
  slot: number | undefined;
  slotNanos: number | undefined;
  nowMillis: number | undefined;
  onFind: (identity: string) => void;
}) {
  const { row, ours, everywhere, kind } = figure;
  const build = buildLabel(row.client ?? undefined, row.version ?? undefined);
  return (
    <div className="written-row">
      <span className="misses-writer">
        <WriterName name={row.name} identity={row.identity} />
        <span>
          <Copyable text={row.identity} label={shortKey(row.identity, 8, 8)} className="misses-key" />
          {row.no_gossip && (
            <>
              {" · "}
              <span className="written-no-gossip">{noGossipText(row.heard_millis, nowMillis)}</span>
            </>
          )}
          {row.delinquent && (
            <>
              {" · "}
              <span className="written-delinquent">{delinquentText(row.last_vote, slot, slotNanos)}</span>
            </>
          )}
          {kind === "missing" && (
            <>
              {" · "}
              <span className="written-missing">missing everywhere</span>
            </>
          )}
          {kind === "worse" && build && ` · ${build}`}
          <FindInPeers identity={row.identity} onFind={onFind} />
        </span>
      </span>
      <span className="written-ip">{row.ip ? <IpAddress ip={row.ip} country={row.country} /> : "—"}</span>
      <span className="written-ours">
        {count(row.left_out_of_ours)} of {count(written)}
      </span>
      <span className="written-share">{percent(ours, 1)}</span>
      <span className="written-all">{percent(everywhere, 1)}</span>
      <Against share={ours} mark={everywhere} bar={KIND_BAR[kind]} />
    </div>
  );
}

function TheirsSide({
  peers,
  open,
  onToggle,
  onFind,
}: {
  peers: GossipPeers | null;
  open: boolean;
  onToggle: () => void;
  onFind: (identity: string) => void;
}) {
  const store = useStore();
  const participation = store.get("summary", "vote_participation");
  const { value: list, failed } = usePolled("summary.misses");
  const [filter, setFilter] = useState<WriterKind | null>(null);
  const [hint, setHint] = useState<string | null>(null);
  // Unknown until the peer list is in, so nobody is called gone from gossip before it is read.
  // The list says who is in the table, not when each was heard: one in it reads as heard now.
  const heard = useMemo(() => {
    if (!peers) return () => undefined;
    const at = Date.now();
    const listed = new Set(peers.identity);
    return (identity: string) => (listed.has(identity) ? at : null);
  }, [peers]);
  const figures = useMemo(() => (list ? writerFigures(list, heard, Date.now()) : []), [list, heard]);
  const average = list ? averageLeftOut(list) : null;
  const kinds = writerKinds(figures);
  const shown = figures.filter((figure) => filter === null || figure.kind === filter);

  return (
    <>
      <div className="written-head">
        {list === null && <span>{failed ? "could not be read" : "reading…"}</span>}
        {list !== null && <span>{leftUsOutLine(list)}</span>}
        {list !== null && (
          <span className="misses-legend written-legend">
            {WRITER_KINDS.filter((kind) => kinds[kind] > 0).map((kind) => (
              <button
                key={kind}
                type="button"
                className={`explain-trigger${filter === kind ? " is-on" : ""}`}
                title={WRITER_TITLE[kind]}
                onClick={() => setFilter(filter === kind ? null : kind)}
              >
                <i className={`misses-swatch is-${kind}`} />
                <span>
                  <b>{count(kinds[kind])}</b> {WRITER_WORD[kind]}
                </span>
              </button>
            ))}
          </span>
        )}
        {figures.length > 0 && (
          <button type="button" className="stat-trigger" onClick={onToggle}>
            {open ? "hide the list" : "show the list"}
          </button>
        )}
      </div>
      {list !== null && <Rule text={leftUsOutRule(average)} hint={hint} />}
      {open && list !== null && figures.length > 0 && (
        <div className="misses-table">
          <div className="written-row is-head">
            <span>writer</span>
            <span>ip</span>
            <span title="Its certificates this epoch that did not pay us.">left us out</span>
            <span>share</span>
            <span>cause</span>
            <span title="Its share as a bar, our average over every certificate as the mark.">against our average</span>
          </div>
          {shown.map((figure) => (
            <WriterRowView
              key={figure.writer.identity}
              figure={figure}
              average={average}
              explain={(place) => placeExplain(place, participation)}
              onHint={setHint}
              onFind={onFind}
            />
          ))}
        </div>
      )}
    </>
  );
}

function WriterRowView({
  figure,
  average,
  explain,
  onHint,
  onFind,
}: {
  figure: WriterFigure;
  average: number | null;
  explain: (place: MissPlace) => string;
  onHint: (hint: string | null) => void;
  onFind: (identity: string) => void;
}) {
  const { writer, share, causes, kind, heardMillis } = figure;
  const build = buildLabel(writer.client ?? undefined, writer.version ?? undefined);
  return (
    <div className="written-row">
      <span className="misses-writer">
        <WriterName name={writer.name} identity={writer.identity} />
        <span>
          <Copyable text={writer.identity} label={shortKey(writer.identity, 8, 8)} className="misses-key" />
          {kind === "no-gossip" ? (
            <>
              {" · "}
              <span className="written-no-gossip">{noGossipText(heardMillis, Date.now())}</span>
            </>
          ) : (
            build && ` · ${build}`
          )}
          <FindInPeers identity={writer.identity} onFind={onFind} />
        </span>
      </span>
      <span className="written-ip">{writer.ip ? <IpAddress ip={writer.ip} country={writer.country} /> : "—"}</span>
      <span className="written-ours">
        {count(writer.misses)} of {count(writer.certificates)}
      </span>
      <span className="written-share">{percent(share, 1)}</span>
      <span className="written-all written-causes">
        {causes.slice(0, 2).map((cause) => (
          <Hinted key={cause.place} className="written-cause" hint={explain(cause.place)} onHint={onHint}>
            {/* One child, so the trigger's flex layout keeps the space before the count. */}
            <span>
              <i className={`misses-swatch is-${cause.place}`} />
              {cause.place} <b>{count(cause.count)}</b>
            </span>
          </Hinted>
        ))}
      </span>
      <Against share={share} mark={average} bar={kind === "no-gossip" ? "is-no-gossip" : undefined} />
    </div>
  );
}

function FindInPeers({ identity, onFind }: { identity: string; onFind: (identity: string) => void }) {
  return (
    <>
      {" · "}
      <button type="button" className="written-find" onClick={() => onFind(identity)}>
        in peers
      </button>
    </>
  );
}

function Against({ share, mark, bar }: { share: number | null; mark: number | null; bar: string | undefined }) {
  return (
    <span className="written-gap" aria-hidden="true">
      <i className={bar} style={{ width: `${Math.min(100, (share ?? 0) * 100)}%` }} />
      <b style={{ left: `clamp(0px, calc(${Math.min(100, (mark ?? 0) * 100)}% - 1px), calc(100% - 2px))` }} />
    </span>
  );
}

/** The rule for the list, or a hovered cause's description in its place, held at the taller of the two. */
function Rule({ text, hint }: { text: string; hint: string | null }) {
  return (
    <p className="written-rule">
      <span className={hint === null ? undefined : "is-held"}>{text}</span>
      {hint !== null && <span>{hint}</span>}
    </p>
  );
}
