import { describe, expect, it } from "vitest";
import {
  COUNTERS,
  schedulerSection,
  sumWaterfalls,
  turnOf,
  turnRangeLabel,
  turnRangeShortLabel,
  turnSections,
  turnSpanLabel,
} from "./turns";
import type { ExecutedStage, LeaderTurn, QuicPort, SlotWaterfall, VerifyStage } from "./types";

function waterfall(over: Partial<SlotWaterfall> = {}): SlotWaterfall {
  return {
    slot: 100,
    source: "scheduler",
    received: 0,
    not_held: 0,
    check_queue_full: 0,
    unparsable: 0,
    bad_locks: 0,
    compute_budget: 0,
    too_old: 0,
    already_processed: 0,
    fee_payer: 0,
    filtered: 0,
    nonce_conflict: 0,
    buffered: 0,
    queue_full: 0,
    nonce_evicted: 0,
    cleared: 0,
    cleaned: 0,
    scheduled: 0,
    blocked_conflicts: 0,
    blocked_threads: 0,
    finished: 0,
    retried: 0,
    ...over,
  };
}

const quic: QuicPort = {
  name: "tpu",
  offered: 0,
  shed_all: 0,
  shed_address: 0,
  refused_full: 0,
  handshake_timeout: 0,
  handshake_error: 0,
  handshook: 0,
  add_failed: 0,
  add_failed_staked: 0,
  add_failed_unstaked: 0,
  add_failed_banned: 0,
  admitted_staked: 0,
  admitted_unstaked: 0,
  streams: 0,
  throttled_staked: 0,
  throttled_unstaked: 0,
  read_timeouts: 0,
  read_errors: 0,
  invalid_size: 0,
  handed_on: 8_168,
  queue_full: 0,
  disconnected: 0,
  open: 0,
  active_streams: 0,
  kernel_drops: null,
};

describe("COUNTERS", () => {
  it("names every counter a waterfall carries, so a new one cannot be left out of the sum", () => {
    const { slot: _slot, source: _source, ...counters } = waterfall();
    expect([...COUNTERS].sort()).toEqual(Object.keys(counters).sort());
  });
});

const verify: VerifyStage = {
  received: 15_753,
  duplicate: 5,
  below_floor: 0,
  verified: 15_748,
  evicted_batches: 0,
};

const executed: ExecutedStage = {
  attempted: 6_459,
  cost_throttled: 0,
  retryable: 737,
  expired_bank: 725,
  processed: 6_459,
  succeeded: 6_459,
  too_many_locks: 0,
  account_missing: 0,
  fee_payer_broke: 0,
  fee_payer_invalid: 0,
  blockhash_missing: 0,
  blockhash_old: 0,
  already_processed: 0,
  bad_compute_budget: 0,
  program_not_executable: 0,
};

const turn: LeaderTurn = {
  first: 88,
  last: 91,
  produced: 4,
  drained_millis: 1_000_000,
  since_millis: 748_000,
  quic,
  verify,
  executed,
};

describe("turnOf", () => {
  it("keys every slot of a turn to it", () => {
    const map = turnOf([turn, { ...turn, first: 12, last: 12 }]);
    expect(map.get(88)).toBe(turn);
    expect(map.get(91)).toBe(turn);
    expect(map.get(92)).toBeUndefined();
    expect(map.get(12)?.first).toBe(12);
  });
});

describe("labels", () => {
  it("ranges the slots and names the span", () => {
    expect(turnRangeLabel(turn)).toBe("88–91");
    expect(turnRangeLabel({ ...turn, last: 88 })).toBe("88");
    expect(turnRangeShortLabel({ first: 462_797_736, last: 462_797_739 })).toBe("462,797,736–739");
    expect(turnRangeShortLabel({ first: 462_797_999, last: 462_798_002 })).toBe("462,797,999–798,002");
    expect(turnRangeShortLabel({ first: 996, last: 1_000 })).toBe("996–1,000");
    expect(turnRangeShortLabel({ first: 88, last: 88 })).toBe("88");
    expect(turnSpanLabel(turn)).toBe("4m 12s since the previous turn drained");
    expect(turnSpanLabel({ ...turn, since_millis: null })).toBe("since the dashboard started");
  });
});

describe("sumWaterfalls", () => {
  it("adds the counts and takes the newest slot's source", () => {
    const sum = sumWaterfalls([
      waterfall({ slot: 88, received: 100, scheduled: 10 }),
      waterfall({ slot: 89, received: 50, scheduled: 5, source: "bam" }),
    ]);
    expect(sum?.received).toBe(150);
    expect(sum?.scheduled).toBe(15);
    expect(sum?.source).toBe("bam");
    expect(sumWaterfalls([])).toBeNull();
  });
});

describe("turnSections", () => {
  it("sets what landed beside the executions, which count runs", () => {
    const sections = turnSections(turn, [], 6_447);
    expect(sections.map((section) => section.key)).toEqual(["listener", "verify", "executed"]);
    const last = sections[2];
    expect(last.through).toEqual({ label: "executed · 6,447 landed", count: 6_459 });
    expect(last.note).toBe("workers · 4m 12s since the previous turn drained");
  });

  it("keeps the bank-gone retries behind the retry row rather than beside it", () => {
    const last = turnSections(turn, [], 6_447)[2];
    expect(last.losses.map((loss) => loss.key)).toEqual(["exec_retryable"]);
    expect(last.detail[0]).toMatchObject({ key: "exec_expired_bank", count: 725 });
    expect(last.detail[0].share).toBeCloseTo(725 / 737, 6);
  });
});

describe("schedulerSection", () => {
  it("draws the intake losses against what arrived, scheduled as the way through", () => {
    const section = schedulerSection(
      waterfall({
        received: 1000,
        too_old: 300,
        already_processed: 500,
        buffered: 200,
        scheduled: 150,
      }),
    );
    expect(section.total).toBe(1000);
    expect(section.through).toEqual({ label: "scheduled", count: 150 });
    expect(section.losses.map((loss) => loss.key)).toEqual(["already_processed", "too_old"]);
    expect(section.losses[0].share).toBeCloseTo(0.5, 6);
    expect(section.zeros).toBeGreaterThan(0);
    expect(section.aside).toBeNull();
  });

  it("draws a BAM turn against buffered, with the batches as an aside", () => {
    const section = schedulerSection(
      waterfall({ source: "bam", received: 700, buffered: 690, scheduled: 680 }),
    );
    expect(section.total).toBe(690);
    expect(section.aside?.count).toBe(700);
    expect(section.note).toContain("BAM");
  });
});
