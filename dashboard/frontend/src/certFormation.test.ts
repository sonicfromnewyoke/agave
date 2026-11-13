import { describe, expect, it } from "vitest";
import {
  HOLD_MS,
  REPLAY_SLOWDOWN,
  axisRange,
  binAt,
  barTone,
  bars,
  geometry,
  halfLabelClear,
  milestoneLabel,
  milestones,
  msAt,
  pathShare,
  overlaps,
  replayAt,
  runningPath,
  shareBy,
  ticks,
  unmeasured,
  votersIn,
  xAt,
  type Milestone,
} from "./certFormation";
import { gossipPeers } from "./gossip";
import type { CertFormation, GossipPeers } from "./types";

/** A curve with `steps` of `[ms, share in hundredths of a percent]` arriving at each ms. */
function curve(steps: [number, number][]): number[] {
  const points = new Array<number>(481).fill(0);
  for (const [ms, share] of steps) {
    for (let at = ms + 1; at <= 480; at++) points[at] = (points[at] ?? 0) + share;
  }
  return points;
}

/** Shaped like testnet's epoch 1063 on 4.4: votes from about 250 ms to past 470. */
function testnet(over: Partial<CertFormation> = {}): CertFormation {
  return {
    epoch: 1_063,
    slot_nanos: 222_000_000,
    notarize: curve([
      [243, 500],
      [270, 3_500],
      [285, 2_000],
      [310, 2_000],
    ]),
    finalize: curve([
      [300, 3_000],
      [412, 3_000],
      [470, 500],
    ]),
    notarize_total: 9_150,
    finalize_total: 9_000,
    notarized_millis: 285,
    fast_final_millis: 310,
    second_round_millis: 412,
    slots: 431_000,
    fast_slots: null,
    second_round_slots: null,
    ...over,
  };
}

/** Shaped like the alpenglow cluster's epoch 19 on 4.3: everything before 160 ms, the last at 367. */
function early(): CertFormation {
  return {
    epoch: 19,
    slot_nanos: 208_000_000,
    notarize: curve([
      [64, 3_000],
      [70, 3_500],
      [125, 1_500],
      [150, 1_800],
    ]),
    finalize: curve([
      [81, 2_000],
      [87, 4_000],
      [367, 600],
    ]),
    notarize_total: 9_800,
    finalize_total: 9_800,
    notarized_millis: 70,
    fast_final_millis: 150,
    second_round_millis: 87,
    slots: 12_700,
    fast_slots: 2_070,
    second_round_slots: 99_660,
  };
}

describe("axisRange", () => {
  it("breaks the axis a step before the first vote and ends a step past the last", () => {
    expect(axisRange(testnet())).toEqual({ floor: 200, end: 480 });
  });

  it("starts at zero without a break when the votes come early", () => {
    expect(axisRange(early())).toEqual({ floor: 0, end: 400 });
  });

  it("keeps the whole curve when nothing arrived", () => {
    expect(axisRange(testnet({ notarize: curve([]), finalize: curve([]) }))).toEqual({ floor: 0, end: 480 });
  });
});

describe("geometry", () => {
  it("uses the phone sizes below 360 px and a stub only with a floor", () => {
    expect(geometry(1_208, { floor: 200, end: 480 })).toMatchObject({ phone: false, half: 120, bin: 5, stub: 18 });
    expect(geometry(295, { floor: 200, end: 480 })).toMatchObject({ phone: true, half: 84, bin: 10, stub: 14 });
    expect(geometry(1_208, { floor: 0, end: 400 }).stub).toBe(0);
  });
});

describe("the axis", () => {
  it("squeezes the time before the floor into the stub", () => {
    const g = geometry(1_000, { floor: 200, end: 480 });
    expect(xAt(g, 0)).toBe(g.left);
    expect(xAt(g, 200)).toBe(g.left + g.stub);
    expect(xAt(g, 480)).toBe(g.right);
    expect(xAt(g, 600)).toBe(g.right);
    expect(msAt(g, xAt(g, 300))).toBeCloseTo(300);
    expect(msAt(g, g.left)).toBe(200);
  });

  it("runs straight from zero without a floor", () => {
    const g = geometry(1_000, { floor: 0, end: 400 });
    expect(xAt(g, 0)).toBe(g.left);
    expect(xAt(g, 200)).toBeCloseTo((g.left + g.right) / 2);
    expect(msAt(g, xAt(g, 86))).toBeCloseTo(86);
  });
});

describe("shares", () => {
  it("reads what had arrived by a time", () => {
    const f = testnet();
    expect(shareBy(f.notarize, 269)).toBe(0.05);
    expect(shareBy(f.notarize, 270)).toBe(0.4);
    expect(shareBy(f.notarize, 480)).toBe(0.8);
  });

  it("counts stake with no measured vote", () => {
    expect(unmeasured(testnet())).toBeCloseTo(0.085);
  });
});

describe("bars", () => {
  it("puts each arrival in its bin from the floor and caps the tall ones", () => {
    const g = geometry(1_000, axisRange(testnet()));
    const list = bars(g, testnet().notarize);
    expect(list.map((bar) => bar.from)).toEqual([240, 270, 285, 310]);
    expect(list[1]).toMatchObject({ capped: true, height: g.half - 4 });
    expect(list[2]).toMatchObject({ capped: false, height: 100 });
  });

  it("draws early votes when the axis starts at zero", () => {
    const g = geometry(1_000, axisRange(early()));
    expect(bars(g, early().notarize).map((bar) => bar.from)).toEqual([60, 70, 125, 150]);
  });
});

describe("runningPath", () => {
  it("steps at each arrival and joins later votes at the axis end", () => {
    const g = geometry(1_000, { floor: 200, end: 480 });
    const path = runningPath(g, curve([[300, 5_000]]), 7_000, -1);
    expect(path.startsWith(`M${g.left} ${g.base}`)).toBe(true);
    expect(path).toContain(`H${xAt(g, 300).toFixed(1)} V${(g.base - 0.5 * g.half).toFixed(1)}`);
    expect(path.endsWith(`H${g.right} V${(g.base - 0.7 * g.half).toFixed(1)}`)).toBe(true);
  });
});

describe("ticks", () => {
  it("steps from past the floor and names the ends", () => {
    const list = ticks(geometry(1_000, { floor: 200, end: 480 }));
    expect(list.map((tick) => tick.ms)).toEqual([0, 240, 280, 320, 360, 400, 440, 480]);
    expect(list[0]).toEqual({ ms: 0, label: "slot start", align: "start" });
    expect(list.at(-1)).toEqual({ ms: 480, label: "480 ms", align: "end" });
  });

  it("steps from zero without a floor", () => {
    expect(ticks(geometry(1_000, { floor: 0, end: 400 })).map((tick) => tick.ms)).toEqual([
      0, 40, 80, 120, 160, 200, 240, 280, 320, 360, 400,
    ]);
  });

  it("leaves out a step whose label would meet the end's", () => {
    expect(ticks(geometry(240, { floor: 200, end: 480 })).map((tick) => tick.ms)).toEqual([0, 240, 320, 480]);
    expect(ticks(geometry(240, { floor: 0, end: 400 })).map((tick) => tick.ms)).toEqual([0, 80, 160, 240, 400]);
  });
});

describe("milestones", () => {
  it("lists the certificates within the axis, earliest first", () => {
    expect(milestones(testnet(), 480).map((mark) => mark.key)).toEqual(["notarized", "fast", "second"]);
    expect(milestones(early(), 400).map((mark) => mark.key)).toEqual(["notarized", "second", "fast"]);
    expect(milestones(testnet({ second_round_millis: 520, fast_final_millis: null }), 480)).toEqual([
      { key: "notarized", ms: 285, direction: -1, level: 0.6 },
    ]);
  });
});

describe("milestone labels", () => {
  const second: Milestone = { key: "second", ms: 412, direction: 1, level: 0.6 };
  const fast: Milestone = { key: "fast", ms: 310, direction: -1, level: 0.8 };

  it("puts second round left of its rule, past the 60% line", () => {
    const g = geometry(1_000, { floor: 200, end: 480 });
    const place = milestoneLabel(g, second, "second round 412", []);
    expect(place.left + place.width).toBeCloseTo(xAt(g, 412) - 5);
    expect(place.top).toBe(g.base + 0.6 * g.half + 3);
  });

  it("moves second round right and short of the line where the left has no room", () => {
    const g = geometry(1_000, { floor: 0, end: 400 });
    const place = milestoneLabel(g, { ...second, ms: 30 }, "second round 30", []);
    expect(place.left).toBeCloseTo(xAt(g, 30) + 5);
    expect(place.top).toBe(g.base + 0.6 * g.half - 16);
  });

  it("moves second round right where the left would cover a bar", () => {
    const g = geometry(1_000, axisRange(early()));
    const down = bars(g, early().finalize);
    const place = milestoneLabel(g, { ...second, ms: 104 }, "second round 104", down);
    expect(place.left).toBeCloseTo(xAt(g, 104) + 5);
  });

  it("moves fast final above its line to the left at the right edge", () => {
    const g = geometry(1_000, { floor: 0, end: 400 });
    const place = milestoneLabel(g, { ...fast, ms: 395 }, "fast final 395", []);
    expect(place.left + place.width).toBeCloseTo(xAt(g, 395) - 5);
    expect(place.top).toBe(g.base - 0.8 * g.half - 16);
  });

  it("flips notarized left of its rule at the right edge", () => {
    const g = geometry(1_000, { floor: 0, end: 400 });
    const place = milestoneLabel(g, { key: "notarized", ms: 395, direction: -1, level: 0.6 }, "notarized 395", []);
    expect(place.left + place.width).toBeCloseTo(xAt(g, 395) - 5);
  });

  it("tells touching labels apart from separate ones", () => {
    const a = { left: 100, top: 2, width: 80 };
    expect(overlaps(a, { left: 150, top: 4, width: 60 })).toBe(true);
    expect(overlaps(a, { left: 300, top: 2, width: 60 })).toBe(false);
    expect(overlaps(a, { left: 120, top: 40, width: 60 })).toBe(false);
  });

  it("hides a half label under a milestone label or a bar", () => {
    const g = geometry(240, axisRange(early()));
    const lower = { left: g.left, top: g.bottom - 14, width: 54 };
    expect(halfLabelClear(g, lower, [], [], 1)).toBe(true);
    expect(halfLabelClear(g, lower, [{ left: 60, top: g.bottom - 12, width: 80 }], [], 1)).toBe(false);
    expect(halfLabelClear(g, lower, [], bars(g, early().finalize), 1)).toBe(false);
  });
});

describe("barTone", () => {
  const f = testnet();

  it("shades notarize bars by the certificate they count toward", () => {
    expect(barTone(-1, 282.5, f, null)).toBe("notarize");
    expect(barTone(-1, 297.5, f, null)).toBe("notarize-late");
    expect(barTone(-1, 320, f, null)).toBe("after");
  });

  it("holds bars as pending until their certificate forms in the replay", () => {
    expect(barTone(-1, 282.5, f, 280)).toBe("pending");
    expect(barTone(-1, 282.5, f, 285)).toBe("notarize");
    expect(barTone(-1, 297.5, f, 300)).toBe("pending");
    expect(barTone(1, 302.5, f, 400)).toBe("pending");
    expect(barTone(1, 302.5, f, 412)).toBe("finalize");
    expect(barTone(1, 450, f, 450)).toBe("after");
  });
});

describe("replayAt", () => {
  const marks = [285, 310, 412];

  it("runs at a tenth of speed from the floor", () => {
    expect(replayAt(0, marks, 200, 480)).toEqual({ ms: 200, hold: null, done: false });
    expect(replayAt(60 * REPLAY_SLOWDOWN, marks, 200, 480).ms).toBe(260);
  });

  it("stops at each milestone for the hold", () => {
    const reach = (285 - 200) * REPLAY_SLOWDOWN;
    expect(replayAt(reach + HOLD_MS / 2, marks, 200, 480)).toEqual({
      ms: 285,
      hold: { index: 0, fraction: 0.5 },
      done: false,
    });
    expect(replayAt(reach + HOLD_MS + 10 * REPLAY_SLOWDOWN, marks, 200, 480).ms).toBe(295);
  });

  it("ends at the axis end", () => {
    const total = (480 - 200) * REPLAY_SLOWDOWN + marks.length * HOLD_MS;
    expect(replayAt(total - 1, marks, 200, 480).done).toBe(false);
    expect(replayAt(total, marks, 200, 480)).toEqual({ ms: 480, hold: null, done: true });
  });
});

describe("picking a bar", () => {
  it("finds the bar under a time, or none off the bars", () => {
    const g = geometry(1_000, { floor: 200, end: 480 });
    expect(binAt(g, 272.3)).toBe(270);
    expect(binAt(g, 150)).toBeNull();
    expect(binAt(g, 480)).toBeNull();
  });

  it("lists the validators whose vote of the half's kind landed in it", () => {
    const list: GossipPeers = {
      root: 0,
      total_stake: 100,
      clients: [["Agave", "4.3.2"]],
      identity: ["A", "B", "C"],
      name: [null, null, null],
      stake: [50, 30, 20],
      client: [0, 0, 0],
      ip: [null, null, null],
      country: [null, null, null],
      rpc: [null, null, null],
      finalization: [null, null, null],
      vote_arrival_epoch: 19,
      vote_arrival: [[70_400, null, null, 88_000, 10], [74_900, null, null, 0, 10], [75_000, null, null, null, 10]],
    };
    const peers = gossipPeers(list);
    expect(votersIn(peers, -1, 70, 75)).toEqual(["A", "B"]);
    // A zero finalize mean is no measurement.
    expect(votersIn(peers, 1, 85, 90)).toEqual(["A"]);
    expect(votersIn(peers, 1, 0, 5)).toEqual([]);
  });
});

describe("pathShare", () => {
  it("gives whole percents with the winner marked", () => {
    expect(pathShare(99_660, 2_070)).toEqual({ text: "won 98%", won: true });
    expect(pathShare(2_070, 99_660)).toEqual({ text: "won 2%", won: false });
  });

  it("keeps a tenth near either end and bounds the extremes", () => {
    expect(pathShare(500, 99_500)).toEqual({ text: "won 0.5%", won: false });
    expect(pathShare(99_127, 3)).toEqual({ text: "won >99.9%", won: true });
    expect(pathShare(3, 99_127)).toEqual({ text: "won <0.1%", won: false });
    expect(pathShare(432_000, 0)).toEqual({ text: "won 100%", won: true });
    expect(pathShare(0, 432_000)).toEqual({ text: "won 0%", won: false });
  });

  it("marks no winner on an even split and shows nothing without counts", () => {
    expect(pathShare(50, 50)?.won).toBe(false);
    expect(pathShare(null, 5)).toBeNull();
    expect(pathShare(0, 0)).toBeNull();
  });
});
