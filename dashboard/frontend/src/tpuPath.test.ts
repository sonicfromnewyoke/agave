import { describe, expect, it } from "vitest";
import {
  admittedShare,
  doorSection,
  epochSpanLabel,
  executedSection,
  listenerSection,
  portNamed,
  portsBusiestFirst,
  refusedTable,
  stakedShare,
  streamSection,
  verifySection,
  type PathLoss,
} from "./tpuPath";
import type { EpochSpan, ExecutedStage, QuicPort } from "./types";

function quiet(over: Partial<QuicPort> = {}): QuicPort {
  return {
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
    handed_on: 0,
    queue_full: 0,
    disconnected: 0,
    open: 0,
    active_streams: 0,
    kernel_drops: null,
    ...over,
  };
}

const BUSY = quiet({
  offered: 18_420,
  shed_all: 2_140,
  shed_address: 6_880,
  refused_full: 412,
  handshake_timeout: 1_205,
  handshake_error: 338,
  // 18_420 offered, less the 10_975 shed or failed above, so neither uncounted branch has anything.
  handshook: 7_445,
  add_failed: 7,
  admitted_staked: 1_890,
  admitted_unstaked: 5_548,
  streams: 42_880,
  throttled_unstaked: 3_412,
  read_timeouts: 288,
  read_errors: 41,
  invalid_size: 12,
  handed_on: 38_220,
  queue_full: 40,
  open: 1_284,
  active_streams: 46,
});

function keys(losses: PathLoss[]): string[] {
  return losses.map((loss) => loss.key);
}

describe("doorSection", () => {
  it("draws every gate against what was offered", () => {
    const section = doorSection(BUSY, null);
    expect(section.total).toBe(18_420);
    expect(section.through.count).toBe(7_438);
    const shed = section.losses.find(
      (loss) => loss.key === "door_shed_address",
    );
    expect(shed?.share).toBeCloseTo(6880 / 18420, 10);
  });

  it("lists the losses largest first rather than in the order they happen", () => {
    expect(keys(doorSection(BUSY, null).losses)).toEqual([
      "door_shed_address",
      "door_shed_all",
      "door_handshake_timeout",
      "door_refused_full",
      "door_handshake_error",
      "door_add_failed",
    ]);
  });

  it("leaves out the gates that did not fire and counts them instead", () => {
    const section = doorSection(
      quiet({ offered: 10, admitted_unstaked: 8, shed_all: 2, handshook: 8 }),
      null,
    );
    expect(keys(section.losses)).toEqual(["door_shed_all"]);
    expect(section.zeros).toBe(9);
  });

  it("caps a gate that ran past the offer rather than reporting over a whole", () => {
    // Which happens: the rate limiter is checked a second time after the
    // handshake, so a connection can be charged to a gate it already passed.
    const section = doorSection(
      quiet({ offered: 100, shed_address: 140 }),
      null,
    );
    expect(section.losses[0].share).toBe(1);
  });

  it("keeps the kernel's datagrams out of the bar and out of the shares", () => {
    const section = doorSection(BUSY, 512);
    expect(section.aside?.count).toBe(512);
    expect(section.aside?.unit).toBe("datagrams");
    expect(keys(section.losses)).not.toContain("door_kernel");
  });

  it("has no such line where the port was never found among the sockets", () => {
    expect(doorSection(BUSY, null).aside).toBeNull();
  });
});

describe("the connections nothing accounted for", () => {
  const VOTE = { offered: 317, admitted_staked: 18 };

  it("puts them before the handshake where few connections reached one", () => {
    const section = doorSection(quiet({ ...VOTE, handshook: 18 }), null);
    const loss = section.losses.find((l) => l.key === "door_unaccounted_pre");
    expect(loss?.count).toBe(299);
    expect(keys(section.losses)).not.toContain("door_unaccounted_post");
  });

  it("puts them after it where they all reached one", () => {
    const section = doorSection(quiet({ ...VOTE, handshook: 317 }), null);
    const loss = section.losses.find((l) => l.key === "door_unaccounted_post");
    expect(loss?.count).toBe(299);
    expect(loss?.share).toBeCloseTo(299 / 317, 10);
    expect(keys(section.losses)).not.toContain("door_unaccounted_pre");
  });

  it("says nothing where the listener accounted for everything", () => {
    const shown = keys(doorSection(BUSY, null).losses);
    expect(shown).not.toContain("door_unaccounted_pre");
    expect(shown).not.toContain("door_unaccounted_post");
  });

  it("never runs negative when a gate is charged twice", () => {
    // The rate limiter is checked again after the handshake, so the gates can
    // total more than the offer and the subtraction can go the wrong way.
    const section = doorSection(
      quiet({
        offered: 100,
        shed_address: 140,
        handshook: 10,
        admitted_staked: 10,
      }),
      null,
    );
    expect(keys(section.losses)).not.toContain("door_unaccounted_pre");
  });
});

describe("refusedTable", () => {
  it("takes the larger reading rather than adding the overlapping names", () => {
    // An unstaked peer turned away raises both of these, being one refusal
    // counted twice. Added they would report forty refusals for twenty.
    expect(
      refusedTable(quiet({ add_failed: 20, add_failed_unstaked: 20 })),
    ).toBe(20);
  });

  it("adds the three that are mutually exclusive with each other", () => {
    const q = quiet({
      add_failed_staked: 3,
      add_failed_unstaked: 9,
      add_failed_banned: 2,
    });
    expect(refusedTable(q)).toBe(14);
  });

  it("keeps the one name the vote port has where the others are silent", () => {
    // The vote listener raises none of the stake-weighted names, so on that
    // port this counter is the only refusal signal there is.
    expect(refusedTable(quiet({ add_failed: 6 }))).toBe(6);
  });

  it("lists the names it did not add, and drops the ones at nought", () => {
    const section = doorSection(
      quiet({
        offered: 100,
        handshook: 40,
        add_failed: 20,
        add_failed_unstaked: 20,
      }),
      null,
    );
    expect(keys(section.detail)).toEqual([
      "door_add_failed_unstaked",
      "door_add_failed_insert",
    ]);
    expect(section.detail[0].share).toBeCloseTo(1, 10);
    expect(keys(section.losses)).toContain("door_add_failed");
  });
});

describe("streamSection", () => {
  it("takes what was carried as the streams less what was lost", () => {
    const section = streamSection(BUSY);
    expect(section.total).toBe(42_880);
    expect(section.through.count).toBe(42_880 - 3_412 - 288 - 41 - 12);
  });

  it("marks a staked throttle and leaves an unstaked one unmarked", () => {
    // Throttling unstaked traffic is the limiter working. Throttling staked
    // traffic is the limiter biting on what it is meant to favour.
    const section = streamSection(
      quiet({ streams: 100, throttled_staked: 5, throttled_unstaked: 9 }),
    );
    expect(
      section.losses.find((loss) => loss.key === "stream_throttled_staked")
        ?.warn,
    ).toBe(true);
    expect(
      section.losses.find((loss) => loss.key === "stream_throttled_unstaked")
        ?.warn,
    ).toBe(false);
  });
});

describe("listenerSection", () => {
  it("makes the total from the outcomes, having no count of its own", () => {
    const section = listenerSection(
      quiet({ handed_on: 900, queue_full: 80, disconnected: 20 }),
    );
    expect(section.total).toBe(1000);
    expect(section.through.count).toBe(900);
    expect(section.losses[0].share).toBeCloseTo(0.08, 10);
  });

  it("marks both of its losses, since either means something was already in", () => {
    const section = listenerSection(
      quiet({ handed_on: 10, queue_full: 1, disconnected: 1 }),
    );
    expect(section.losses.every((loss) => loss.warn)).toBe(true);
  });
});

describe("verifySection", () => {
  const stage = {
    received: 39_100,
    duplicate: 8_200,
    below_floor: 0,
    verified: 30_480,
  };

  it("works the bad signatures out of what is left over", () => {
    const section = verifySection({ ...stage, evicted_batches: 0 });
    expect(
      section.losses.find((loss) => loss.key === "verify_bad")?.count,
    ).toBe(420);
  });

  it("never reports a negative count when the parts do not line up", () => {
    const section = verifySection({ ...stage, duplicate: 39_000, evicted_batches: 0 });
    expect(keys(section.losses)).not.toContain("verify_bad");
  });

  it("keeps the evicted batches out of the bar, being a different unit", () => {
    const section = verifySection({ ...stage, evicted_batches: 3 });
    expect(section.aside?.count).toBe(3);
    expect(section.aside?.unit).toContain("batches");
    expect(keys(section.losses)).not.toContain("verify_evicted");
  });

  it("says nothing at all where no batch was evicted", () => {
    expect(verifySection({ ...stage, evicted_batches: 0 }).aside).toBeNull();
  });
});

describe("executedSection", () => {
  const stage = (over: Partial<ExecutedStage> = {}): ExecutedStage => ({
    attempted: 0,
    cost_throttled: 0,
    retryable: 0,
    expired_bank: 0,
    processed: 0,
    succeeded: 0,
    too_many_locks: 0,
    account_missing: 0,
    fee_payer_broke: 0,
    fee_payer_invalid: 0,
    blockhash_missing: 0,
    blockhash_old: 0,
    already_processed: 0,
    bad_compute_budget: 0,
    program_not_executable: 0,
    ...over,
  });

  const countOf = (losses: PathLoss[], key: string) =>
    losses.find((loss) => loss.key === key)?.count ?? 0;

  it("accounts for everything the workers took up", () => {
    // A testnet reading: 101 attempted, 13 handed back, 63 committed, and 25 in no row at all.
    const section = executedSection(
      stage({ attempted: 101, retryable: 13, processed: 63, succeeded: 63 }),
      null,
    );
    expect(countOf(section.losses, "exec_dropped")).toBe(25);
    expect(countOf(section.detail, "exec_other_reasons")).toBe(25);
  });

  it("names the reasons it has and gathers the rest", () => {
    const section = executedSection(
      stage({
        attempted: 100,
        retryable: 10,
        processed: 60,
        succeeded: 60,
        blockhash_missing: 12,
        fee_payer_broke: 8,
        already_processed: 4,
      }),
      null,
    );
    expect(countOf(section.losses, "exec_dropped")).toBe(30);
    expect(countOf(section.detail, "exec_blockhash_missing")).toBe(12);
    expect(countOf(section.detail, "exec_fee_payer_broke")).toBe(8);
    expect(countOf(section.detail, "exec_other_reasons")).toBe(6);
  });

  it("does not go negative when the two points disagree", () => {
    // The outcomes and the reasons are reported separately, so a window can
    // catch more reasons than it caught loss. Nought, not a negative row.
    const section = executedSection(
      stage({ attempted: 10, processed: 10, succeeded: 10, account_missing: 4 }),
      null,
    );
    expect(keys(section.losses)).not.toContain("exec_dropped");
    expect(keys(section.detail)).not.toContain("exec_other_reasons");
  });

  it("derives the failures from committed less succeeded", () => {
    const section = executedSection(stage({ attempted: 100, processed: 90, succeeded: 80 }), null);
    expect(countOf(section.losses, "exec_failed")).toBe(10);
  });

  it("does not go negative if succeeded outruns committed across a reset", () => {
    const section = executedSection(stage({ attempted: 10, processed: 5, succeeded: 9 }), null);
    expect(keys(section.losses)).not.toContain("exec_failed");
  });

  it("keeps the load reasons out of the losses and out of the bar", () => {
    const section = executedSection(
      stage({
        attempted: 1000,
        processed: 900,
        succeeded: 850,
        blockhash_old: 60,
      }),
      null,
    );
    expect(keys(section.losses)).not.toContain("exec_blockhash_old");
    expect(keys(section.detail)).toContain("exec_blockhash_old");
  });

  it("keeps no room in the block out of the bar, being counted in the retries already", () => {
    const section = executedSection(
      stage({ attempted: 100, processed: 60, succeeded: 55, retryable: 30, cost_throttled: 25 }),
      null,
    );
    const drawn = section.through.count + section.losses.reduce((sum, loss) => sum + loss.count, 0);
    expect(drawn).toBe(100);
    expect(section.detail.find((loss) => loss.key === "exec_cost_throttled")?.share).toBe(0.25);
  });

  it("draws a load reason against the loads that failed, not everything attempted", () => {
    const section = executedSection(
      stage({
        attempted: 1000,
        processed: 900,
        succeeded: 900,
        blockhash_old: 100,
      }),
      null,
    );
    const reason = section.detail.find(
      (loss) => loss.key === "exec_blockhash_old",
    );
    expect(reason?.share).toBeCloseTo(1, 10);
  });

  it("notes the bundles on the heading without making them a share of the bar", () => {
    const section = executedSection(
      stage({ attempted: 1000, processed: 900, succeeded: 850 }),
      { received: 1284, packets: 4617 },
    );
    expect(section.aside?.label).toContain("1,284 bundles");
    expect(section.aside?.count).toBe(4617);
    expect(section.aside?.unit).toBe("transactions");
    expect(section.aside?.warn).toBe(false);
    expect(keys(section.losses)).not.toContain("exec_bundles");
    expect(keys(section.detail)).not.toContain("exec_bundles");
    expect(section.total).toBe(1000);
  });

  it("says nothing at all where no bundle arrived", () => {
    const section = executedSection(
      stage({ attempted: 1000, processed: 900, succeeded: 850 }),
      null,
    );
    expect(section.aside).toBeNull();
  });

  it("counts a load reason at nought among the quiet counters", () => {
    const section = executedSection(
      stage({ attempted: 10, processed: 10, succeeded: 10 }),
      null,
    );
    expect(section.detail).toHaveLength(0);
    expect(section.zeros).toBeGreaterThanOrEqual(12);
  });
});

describe("the headline", () => {
  it("is the admitted share of the offer", () => {
    expect(admittedShare(BUSY)).toBeCloseTo((1890 + 5548) / 18420, 10);
  });

  it("is absent on a port nothing has used", () => {
    expect(admittedShare(quiet())).toBeNull();
  });

  it("caps at everything, so a second window's admissions cannot exceed it", () => {
    expect(admittedShare(quiet({ offered: 10, admitted_staked: 14 }))).toBe(1);
  });

  it("reads the staked share against what was admitted, not what was offered", () => {
    expect(stakedShare(BUSY)).toBeCloseTo(1890 / (1890 + 5548), 10);
    expect(stakedShare(quiet())).toBeNull();
  });
});

describe("picking a port out of the list", () => {
  it("finds one by name and returns nothing for a port the node has not got", () => {
    const ports = [quiet({ name: "tpu" }), quiet({ name: "tpu forwards" })];
    expect(portNamed(ports, "tpu forwards")?.name).toBe("tpu forwards");
    expect(portNamed(ports, "tpu vote quic")).toBeNull();
  });
});

describe("ordering the folded ports", () => {
  it("puts the busiest first, whichever port that turns out to be", () => {
    const ports = [
      quiet({ name: "tpu", offered: 1 }),
      quiet({ name: "tpu forwards", offered: 1 }),
      quiet({ name: "tpu vote quic", offered: 296 }),
    ];
    expect(portsBusiestFirst(ports).map((port) => port.name)).toEqual([
      "tpu vote quic",
      "tpu",
      "tpu forwards",
    ]);
  });

  it("leaves ties in the order they were sent, so rows do not swap while read", () => {
    const ports = [
      quiet({ name: "tpu", offered: 4 }),
      quiet({ name: "tpu forwards", offered: 4 }),
      quiet({ name: "tpu vote quic", offered: 4 }),
    ];
    expect(portsBusiestFirst(ports).map((port) => port.name)).toEqual([
      "tpu",
      "tpu forwards",
      "tpu vote quic",
    ]);
  });

  it("does not reorder the list it was given", () => {
    const ports = [
      quiet({ name: "tpu", offered: 1 }),
      quiet({ name: "tpu vote quic", offered: 9 }),
    ];
    portsBusiestFirst(ports);
    expect(ports.map((port) => port.name)).toEqual(["tpu", "tpu vote quic"]);
  });
});

describe("each section is drawn against its own total", () => {
  it("does not measure one stage against another's denominator", () => {
    const listener = listenerSection(quiet({ handed_on: 900 }));
    const verify = verifySection({
      received: 1000,
      duplicate: 0,
      below_floor: 0,
      verified: 1000,
      evicted_batches: 0,
    });
    expect(listener.through.count / listener.total).toBe(1);
    expect(verify.through.count / verify.total).toBe(1);
  });
});

describe("the span the per-epoch sections are counted over", () => {
  const span = (over: Partial<EpochSpan> = {}): EpochSpan => ({
    epoch: 842,
    elapsed_slots: 264_000,
    counted_slots: 264_000,
    slots_in_epoch: 432_000,
    ...over,
  });

  it("says how far into the epoch the figures reach", () => {
    expect(epochSpanLabel(span())).toBe("Epoch 842, 61% elapsed");
  });

  it("says where counting began where the validator came up part way in", () => {
    // Without this a node that started an hour ago and one that sat idle all
    // epoch read exactly alike, and the second is a fault.
    expect(epochSpanLabel(span({ counted_slots: 21_600 }))).toBe(
      "Epoch 842, 61% elapsed · counted from 56%",
    );
  });

  it("does not caveat an epoch that was only missed by the tick that noticed it", () => {
    expect(epochSpanLabel(span({ counted_slots: 263_995 }))).toBe(
      "Epoch 842, 61% elapsed",
    );
  });

  it("names the epoch and nothing else where the schedule gives no length", () => {
    expect(epochSpanLabel(span({ slots_in_epoch: 0 }))).toBe("Epoch 842");
  });
});
