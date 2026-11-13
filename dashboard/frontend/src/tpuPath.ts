import { count, percent } from "./format";
import { readStored, writeStored } from "./storage";
import type {
  BundleStage,
  EpochSpan,
  ExecutedStage,
  QuicPort,
  VerifyStage,
} from "./types";


export interface PathLoss {
  key: string;
  label: string;
  count: number;
  share: number;
  /** Whether it means the validator could not keep up, rather than a refusal working as designed.
   *  */
  warn: boolean;
  explain: string;
}

/** In a different unit from the bar: drawn beside the heading, with no segment or share. */
export interface PathAside {
  label: string;
  count: number;
  unit: string;
  warn: boolean;
  explain: string;
}

export interface PathSection {
  key: string;
  title: string;
  /** No section is drawn against the one above it. */
  note: string;
  explain: string;
  total: number;
  through: { label: string; count: number };
  losses: PathLoss[];
  /** Shown when expanded, never in the bar, where they would count twice. */
  detail: PathLoss[];
  zeros: number;
  aside: PathAside | null;
}

export const LOSSES_SHOWN = 6;
export const LOSSES_SHOWN_NARROW = 3;

const REFUSAL_NAMES = 4;

/** Connections refused a place in the table, from four overlapping counters: the larger of
 *  `add_failed` and the other three together. */
export function refusedTable(q: QuicPort): number {
  return Math.max(
    q.add_failed,
    q.add_failed_staked + q.add_failed_unstaked + q.add_failed_banned,
  );
}

function shareOf(total: number, count: number): number {
  if (total <= 0) return 0;
  return Math.min(1, count / total);
}

function sorted(
  total: number,
  rows: Array<
    [key: string, label: string, count: number, warn: boolean, explain: string]
  >,
): { losses: PathLoss[]; zeros: number } {
  const losses = rows
    .filter(([, , count]) => count > 0)
    .map(([key, label, count, warn, explain]) => ({
      key,
      label,
      count,
      share: shareOf(total, count),
      warn,
      explain,
    }))
    .sort((a, b) => b.count - a.count);
  return { losses, zeros: rows.length - losses.length };
}

/** The connection funnel, drawn against the offer and clipped, since a connection can be
 *  rate-limited after its handshake. The uncounted gaps either side of the handshake are rows. */
export function doorSection(
  q: QuicPort,
  kernelDrops: number | null,
): PathSection {
  const admitted = q.admitted_staked + q.admitted_unstaked;
  const refused = refusedTable(q);
  // The rate limits fire on either side of the handshake, and the split cancels here.
  const beforeHandshake = Math.max(
    0,
    q.offered -
      (q.shed_all +
        q.shed_address +
        q.refused_full +
        q.handshake_timeout +
        q.handshake_error +
        q.handshook),
  );
  const afterHandshake = Math.max(0, q.handshook - refused - admitted);
  const derivedAtZero =
    (beforeHandshake > 0 ? 0 : 1) + (afterHandshake > 0 ? 0 : 1);
  const { losses, zeros } = sorted(q.offered, [
    [
      "door_shed_address",
      "over one address's rate",
      q.shed_address,
      false,
      "Turned away before a handshake because that address was opening connections too quickly.",
    ],
    [
      "door_shed_all",
      "over the port's rate",
      q.shed_all,
      false,
      "Turned away before a handshake because the port as a whole was over its connection rate.",
    ],
    [
      "door_handshake_timeout",
      "handshake timed out",
      q.handshake_timeout,
      false,
      "Accepted for a handshake that did not finish in time.",
    ],
    [
      "door_refused_full",
      "no room in the table",
      q.refused_full,
      true,
      "Refused because the endpoint already held every connection it is configured to hold.",
    ],
    [
      "door_handshake_error",
      "handshake failed",
      q.handshake_error,
      false,
      "Accepted for a handshake that ended in an error: a transport fault, a rejected certificate, or the peer closing it.",
    ],
    [
      "door_add_failed",
      "refused after handshake",
      refused,
      false,
      "Handshook and then refused a place in the connection table. Counted under four overlapping names, so this is the largest of them rather than their sum.",
    ],
    [
      "door_unaccounted_pre",
      "unaccounted, before handshake",
      beforeHandshake,
      false,
      "Offered and neither shed at a gate nor handshook. Derived: one branch of the listener counts nothing, so this is what remains after everything it does count.",
    ],
    [
      "door_unaccounted_post",
      "unaccounted, after handshake",
      afterHandshake,
      false,
      "Handshook and then neither refused nor admitted. Derived by subtraction; one such path is an unstaked peer at the vote port, which is by design.",
    ],
  ]);

  const names: Array<[key: string, label: string, count: number]> = [
    ["door_add_failed_unstaked", "unstaked table full", q.add_failed_unstaked],
    ["door_add_failed_staked", "staked table full", q.add_failed_staked],
    ["door_add_failed_banned", "peer banned", q.add_failed_banned],
    ["door_add_failed_insert", "table insert failed", q.add_failed],
  ];
  const detail = names
    .map(([key, label, count]) => ({
      key,
      label,
      count,
      share: shareOf(refused, count),
      warn: false,
      explain:
        "One of the names the listener refuses under, as a share of the refusals above. They overlap, so they are not added up.",
    }))
    .filter((reason) => reason.count > 0);

  return {
    key: "door",
    title: "Connections offered",
    note: "to this port",
    explain:
      "Connections, not transactions, each counted at the first gate that closed. The table refusal carries four overlapping names and two branches count nothing, which the unaccounted rows show.",
    total: q.offered,
    through: { label: "admitted", count: admitted },
    losses,
    detail,
    // The unaccounted rows are derived, not counters, so they come out of
    // this tally and the four refusal names go in.
    zeros: zeros - derivedAtZero + (REFUSAL_NAMES - detail.length),
    aside:
      kernelDrops === null
        ? null
        : {
            label: "kernel dropped",
            count: kernelDrops,
            unit: "datagrams",
            warn: false,
            explain:
              "Datagrams the kernel discarded on this port before the listener read them. Counted in datagrams, not connections, so it sits beside the bar rather than in it.",
          },
  };
}

export function streamSection(q: QuicPort): PathSection {
  const { losses, zeros } = sorted(q.streams, [
    [
      "stream_throttled_unstaked",
      "throttled, unstaked",
      q.throttled_unstaked,
      false,
      "Streams from unstaked peers held back at the lower limit they share.",
    ],
    [
      "stream_throttled_staked",
      "throttled, staked",
      q.throttled_staked,
      true,
      "Streams from staked peers held back because the peer was over the capacity its stake earns.",
    ],
    [
      "stream_read_timeout",
      "stopped arriving",
      q.read_timeouts,
      false,
      "Opened and left unfinished long enough to be abandoned.",
    ],
    [
      "stream_read_error",
      "read error",
      q.read_errors,
      false,
      "Failed while being read, rather than merely stalling.",
    ],
    [
      "stream_invalid_size",
      "impossible size",
      q.invalid_size,
      false,
      "Refused for declaring a length that could not be a transaction.",
    ],
  ]);
  const lost = losses.reduce((sum, loss) => sum + loss.count, 0);

  return {
    key: "streams",
    title: "Streams opened",
    note: "on admitted connections",
    explain:
      "What the admitted connections sent, and what the stream limits did with it. One stream carries one transaction.",
    total: q.streams,
    through: { label: "carried", count: Math.max(0, q.streams - lost) },
    losses,
    detail: [],
    zeros,
    aside: null,
  };
}

export function listenerSection(q: QuicPort): PathSection {
  const read = q.handed_on + q.queue_full + q.disconnected;
  const { losses, zeros } = sorted(read, [
    [
      "handed_queue_full",
      "fetch queue full",
      q.queue_full,
      true,
      "Read and then dropped because the queue towards signature verification was full.",
    ],
    [
      "handed_disconnected",
      "queue closed",
      q.disconnected,
      true,
      "Dropped because the queue onward had been closed, which is a validator shutting down.",
    ],
  ]);

  return {
    key: "listener",
    title: "Transactions read",
    note: "out of those streams",
    explain:
      "Transactions assembled from streams, and what became of them. Not packets, so not comparable with the socket card.",
    total: read,
    through: { label: "passed to verify", count: q.handed_on },
    losses,
    detail: [],
    zeros,
    aside: null,
  };
}

export function verifySection(v: VerifyStage): PathSection {
  // Sigverify discards at one step, so what is left after the other three is exactly the bad.
  const bad = Math.max(0, v.received - v.duplicate - v.below_floor - v.verified);
  const { losses, zeros } = sorted(v.received, [
    [
      "verify_duplicate",
      "duplicate",
      v.duplicate,
      false,
      "Seen before. The network resends transactions as a matter of course.",
    ],
    [
      "verify_bad",
      "bad signature",
      bad,
      false,
      "Failed verification. Derived: received less duplicates, underpaying and verified.",
    ],
    [
      "verify_below_floor",
      "below priority floor",
      v.below_floor,
      false,
      "Dropped for paying under the priority floor, where one is configured.",
    ],
  ]);

  return {
    key: "verify",
    title: "Verify",
    note: "signatures and duplicates",
    explain:
      "Signature verification and deduplication for everything but votes, counted over the epoch.",
    total: v.received,
    through: { label: "verified", count: v.verified },
    losses,
    detail: [],
    zeros,
    aside:
      v.evicted_batches > 0
        ? {
            label: "dropped",
            count: v.evicted_batches,
            unit: "batches, not transactions",
            warn: true,
            explain:
              "Batches dropped because the queue to the banking stage was full. A batch's transaction count is not reported, so this cannot be added to the counts here.",
          }
        : null,
  };
}

const LOAD_REASONS: Array<[key: string, label: string, field: keyof ExecutedStage]> = [
  ["exec_blockhash_missing", "blockhash not found", "blockhash_missing"],
  ["exec_blockhash_old", "blockhash too old", "blockhash_old"],
  ["exec_already_processed", "already processed", "already_processed"],
  ["exec_fee_payer_broke", "fee payer could not pay", "fee_payer_broke"],
  ["exec_fee_payer_invalid", "fee payer not usable", "fee_payer_invalid"],
  ["exec_account_missing", "account not found", "account_missing"],
  ["exec_too_many_locks", "too many account locks", "too_many_locks"],
  ["exec_bad_compute_budget", "compute budget", "bad_compute_budget"],
  ["exec_program_not_executable", "program not executable", "program_not_executable"],
];

function executedDetail(
  e: ExecutedStage,
  failedToLoad: number,
): { detail: PathLoss[]; zeros: number } {
  const named = LOAD_REASONS.map(([key, label, field]): [string, string, number] => [
    key,
    label,
    e[field],
  ]);
  // The rarer errors gathered into one row. Floored: the two counters are reported separately.
  const other = failedToLoad - named.reduce((sum, [, , count]) => sum + count, 0);
  named.push(["exec_other_reasons", "other reasons", Math.max(0, other)]);
  const loadReasons = named
    .map(([key, label, count]) => ({
      key,
      label,
      count,
      share: shareOf(failedToLoad, count),
      warn: false,
      explain:
        "One reason a transaction could not be loaded, as a share of those that failed to load.",
    }))
    .filter((reason) => reason.count > 0);
  // Both are counted inside the segments already, so reasons behind them and not segments beside.
  const expired = e.expired_bank;
  const throttled = e.cost_throttled;
  const counted: PathLoss[] = [
    {
      key: "exec_expired_bank",
      label: "retried because the bank had gone",
      count: expired,
      share: shareOf(e.retryable, expired),
      warn: false,
      explain:
        "Of those sent back to retry, the ones handed back before running because " +
        "their slot had ended.",
    },
    {
      key: "exec_cost_throttled",
      label: "no room in the block",
      count: throttled,
      share: shareOf(e.attempted, throttled),
      warn: false,
      explain:
        "Of those taken up, the ones the cost model left out for want of room, each also counted " +
        "as sent back to retry or failed to load.",
    },
  ].filter((reason) => reason.count > 0);
  const zeros = named.length - loadReasons.length + 2 - counted.length;
  return { detail: [...counted, ...loadReasons], zeros };
}

export function executedSection(
  e: ExecutedStage,
  bundles: BundleStage | null,
): PathSection {
  const attempted = e.attempted;
  const failed = Math.max(0, e.processed - e.succeeded);
  // Derived: no counter holds it, and without it the section does not close.
  const dropped = Math.max(0, e.attempted - e.processed - e.retryable);
  const { losses, zeros } = sorted(attempted, [
    [
      "exec_failed",
      "failed, but still in the block",
      failed,
      false,
      "Executed, failed, and committed anyway. A failed transaction still pays its fee and takes room in the block.",
    ],
    [
      "exec_dropped",
      "failed to load",
      dropped,
      false,
      "Never executed because its accounts or blockhash could not be loaded. The reasons are behind the control below.",
    ],
    [
      "exec_retryable",
      "sent back to retry",
      e.retryable,
      false,
      "Handed back to be tried again: its accounts were locked by another transaction in flight, " +
        "the block had no room, or its slot had ended. The last two are behind the control below.",
    ],
  ]);

  const behind = executedDetail(e, dropped);

  return {
    key: "executed",
    title: "Executed",
    note: "taken up by workers",
    explain:
      "The worker threads added together, counted over the epoch. Absent until the first leader slot of the epoch.",
    total: attempted,
    through: { label: "succeeded", count: e.succeeded },
    losses,
    detail: behind.detail,
    zeros: zeros + behind.zeros,
    // A note on the section's composition, not a stage of it: bundle
    // transactions are already inside the figures above.
    aside:
      bundles === null
        ? null
        : {
            label: `${count(bundles.received)} bundles arrived, carrying`,
            count: bundles.packets,
            unit: "transactions",
            warn: false,
            explain:
              "Bundles the block engine sent this epoch, with their transactions. They skip the sections above and are already counted in the figures beside this line, so this carries no percentage.",
          },
  };
}

/** Slots of an epoch that may go uncounted before the gap is worth saying:
 *  the totals start over a tick after the epoch turns. */
const EPOCH_START_SLACK = 32;

export function epochSpanLabel(span: EpochSpan): string {
  if (span.slots_in_epoch <= 0) return `Epoch ${span.epoch}`;
  const elapsed = percent(span.elapsed_slots / span.slots_in_epoch, 0);
  const missed = span.elapsed_slots - span.counted_slots;
  if (missed <= EPOCH_START_SLACK)
    return `Epoch ${span.epoch}, ${elapsed} elapsed`;
  const from = percent(missed / span.slots_in_epoch, 0);
  return `Epoch ${span.epoch}, ${elapsed} elapsed · counted from ${from}`;
}

export function admittedShare(q: QuicPort): number | null {
  if (q.offered <= 0) return null;
  return Math.min(1, (q.admitted_staked + q.admitted_unstaked) / q.offered);
}

export function stakedShare(q: QuicPort): number | null {
  const admitted = q.admitted_staked + q.admitted_unstaked;
  if (admitted <= 0) return null;
  return q.admitted_staked / admitted;
}

export function portNamed(ports: QuicPort[], name: string): QuicPort | null {
  return ports.find((port) => port.name === name) ?? null;
}

/** Ties keep the order sent. */
export function portsBusiestFirst(ports: QuicPort[]): QuicPort[] {
  return [...ports].sort((a, b) => b.offered - a.offered);
}

export const TPU_PATH_STORAGE_KEY = "agave-dashboard-tpu-path-open";

export function readOpenPorts(): string[] {
  const stored = readStored(TPU_PATH_STORAGE_KEY);
  return stored ? stored.split(",").filter(Boolean) : [];
}

export function writeOpenPorts(open: string[]): void {
  writeStored(TPU_PATH_STORAGE_KEY, open.join(","));
}
