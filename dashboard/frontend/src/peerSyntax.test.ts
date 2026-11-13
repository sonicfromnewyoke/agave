import { describe, expect, it } from "vitest";
import { gossipPeers } from "./gossip";
import {
  nextPeerOp,
  peerBrowse,
  peerConditionParts,
  peerHolds,
  peerOffered,
  peerSuggest,
  peerValueText,
  readPeerSearch,
  withFilters,
  withPeerValue,
  type PeerCondition,
} from "./peerSyntax";
import type { GossipPeers } from "./types";

const LIST: GossipPeers = {
  root: 10_000,
  total_stake: 1_000,
  clients: [
    ["Agave", "4.4.0"],
    ["JitoLabs", "4.3.2"],
  ],
  identity: ["Big1111", "Mid2222", "Rpc3333", "Old4444"],
  name: ["Northwind", "Harbor, Inc", null, "Pinecrest"],
  stake: [600e9, 300e9, 0, 50e9],
  client: [0, 1, 0, 0],
  ip: ["189.1.171.215", "67.202.63.79", "198.51.100.7", null],
  country: ["BR", "US", "US", null],
  rpc: [null, null, 8899, null],
  finalization: [0.98, 0.254, null, 0.93],
  vote_arrival_epoch: 1_048,
  vote_arrival: [[92_400, 21_000, 388_000, 141_000, 1_802], [412_000, null, null, null, 1], null, [151_000, 34_000, 612_000, 203_000]],
  delinquent: [false, false, false, true],
  last_vote: [9_990, 9_995, null, 1_000],
};

const PEERS = gossipPeers(LIST);
const OFFERED = peerOffered(PEERS);
/** Before votor has reported an epoch. */
const NO_ARRIVALS = peerOffered(PEERS.map((peer) => ({ ...peer, voteArrival: null })));
const CLOCK = { slotMillis: 400, arrivalKind: "notarize" as const };

const number = (key: PeerCondition["key"], op: PeerCondition["op"], value: number): PeerCondition => ({ key, op, value, values: [] });
const list = (key: PeerCondition["key"], values: string[], op: PeerCondition["op"] = "is"): PeerCondition => ({ key, op, value: null, values });
const matching = (condition: PeerCondition) =>
  PEERS.filter((peer) => peerHolds(condition, peer, CLOCK)).map((peer) => peer.identity);

describe("peerOffered", () => {
  it("counts what the peers hold, the most common first", () => {
    expect(OFFERED.clients).toEqual([["Agave", 3], ["JitoLabs", 1]]);
    expect(OFFERED.countries).toEqual([["US", 2], ["BR", 1]]);
    expect(OFFERED.names).toEqual(["Northwind", "Harbor, Inc", "Pinecrest"]);
    expect(OFFERED.counts).toEqual({ staked: 3, rpc: 1, delinquent: 1 });
  });

  it("offers arrivals only where the table shows them", () => {
    expect(NO_ARRIVALS.arrivals).toBe(false);
    expect(readPeerSearch("arrival>300", NO_ARRIVALS).errors).toHaveLength(1);
  });
});

describe("readPeerSearch", () => {
  it("leaves plain words to search", () => {
    expect(readPeerSearch("  north  wind ", OFFERED)).toEqual({ words: "north wind", filters: [], errors: [] });
  });

  it("reads numbers, lists and kinds beside words", () => {
    const read = readPeerSearch('finalization<50 harbor -client:agave geo:de,nl is:staked name:"Harbor, Inc"', OFFERED);
    expect(read.words).toBe("harbor");
    expect(read.errors).toEqual([]);
    expect(read.filters).toEqual([
      { kind: "condition", condition: number("finalization", "lt", 50) },
      { kind: "condition", condition: list("client", ["Agave"], "is_not") },
      { kind: "condition", condition: list("geo", ["DE", "NL"]) },
      { kind: "only", only: "staked" },
      { kind: "condition", condition: list("name", ["Harbor, Inc"]) },
    ]);
  });

  it("says why an expression cannot be read", () => {
    const errors = (text: string) => readPeerSearch(text, OFFERED).errors;
    expect(errors("nope:1")).toEqual(["No filter named nope."]);
    expect(errors("stake>lots")).toEqual(["stake needs a number, e.g. stake>1000000."]);
    expect(errors("client:nobody")).toEqual(["No peer runs a client named nobody."]);
    expect(errors("geo:Germany")).toEqual(["Germany is not a country code, e.g. geo:DE."]);
    expect(errors("is:slow")).toEqual(["is takes staked, rpc, delinquent."]);
    expect(errors("-is:rpc")).toEqual(["is cannot be negated."]);
    expect(errors("-stake>1")).toEqual([`Only name, client, version, geo and ip take a leading "-".`]);
  });

  it("takes units, and the other name for a country", () => {
    expect(readPeerSearch("arrival>300ms", OFFERED).filters).toEqual([{ kind: "condition", condition: number("arrival", "gt", 300) }]);
    expect(readPeerSearch("country:us", OFFERED).filters).toEqual([{ kind: "condition", condition: list("geo", ["US"]) }]);
  });
});

describe("peerHolds", () => {
  it("compares what each column shows", () => {
    expect(matching(number("stake", "gt", 100))).toEqual(["Big1111", "Mid2222"]);
    expect(matching(number("finalization", "eq", 25))).toEqual(["Mid2222"]);
    expect(matching(number("arrival", "gt", 300))).toEqual(["Mid2222"]);
  });

  it("follows a finalize bar's votes when one is picked", () => {
    const finalize = { ...CLOCK, arrivalKind: "finalize" as const };
    expect(PEERS.filter((peer) => peerHolds(number("arrival", "gt", 150), peer, finalize)).map((peer) => peer.identity)).toEqual(["Old4444"]);
  });

  it("matches lists, a missing value matching neither way", () => {
    expect(matching(list("client", ["JitoLabs"]))).toEqual(["Mid2222"]);
    expect(matching(list("client", ["JitoLabs"], "is_not"))).toEqual(["Big1111", "Rpc3333", "Old4444"]);
    expect(matching(list("geo", ["US"]))).toEqual(["Mid2222", "Rpc3333"]);
    expect(matching(list("geo", ["US"], "is_not"))).toEqual(["Big1111"]);
    expect(matching(list("ip", ["198.51."]))).toEqual(["Rpc3333"]);
    expect(matching(list("version", ["4.4"]))).toEqual(["Big1111", "Rpc3333", "Old4444"]);
    expect(matching(list("version", ["4.3"]))).toEqual(["Mid2222"]);
    expect(matching(list("name", ["north", "old4444"]))).toEqual(["Big1111", "Old4444"]);
  });
});

describe("withFilters", () => {
  it("replaces a condition on the same key and comparison, and keeps kinds in order", () => {
    const held = withFilters(["delinquent"], [number("stake", "gt", 1), number("stake", "lt", 9)], [
      { kind: "condition", condition: number("stake", "gt", 5) },
      { kind: "only", only: "staked" },
    ]);
    expect(held.where).toEqual([number("stake", "gt", 5), number("stake", "lt", 9)]);
    expect(held.only).toEqual(["staked", "delinquent"]);
  });
});

describe("pills", () => {
  it("shows a condition's parts and the value to edit", () => {
    expect(peerConditionParts(number("finalization", "lt", 50))).toEqual({ label: "Finalization", op: "<", value: "50", unit: "%" });
    expect(peerConditionParts(list("geo", ["DE", "NL"], "is_not"))).toEqual({ label: "Country", op: "is not", value: "DE, NL", unit: "" });
    expect(peerValueText(list("name", ["Harbor, Inc", "x"]))).toBe('"Harbor, Inc", x');
  });

  it("cycles the comparison", () => {
    expect(nextPeerOp(number("stake", "gt", 1))).toBe("ge");
    expect(nextPeerOp(number("stake", "eq", 1))).toBe("gt");
    expect(nextPeerOp(list("geo", ["DE"]))).toBe("is_not");
    expect(nextPeerOp(list("geo", ["DE"], "is_not"))).toBe("is");
  });

  it("takes a typed value, keeping the comparison", () => {
    expect(withPeerValue(number("stake", "lt", 1), "2,000", OFFERED)).toEqual(number("stake", "lt", 2_000));
    expect(withPeerValue(list("geo", ["DE"], "is_not"), "nl, fr", OFFERED)).toEqual(list("geo", ["NL", "FR"], "is_not"));
    expect(withPeerValue(list("name", ["x"]), '"Harbor, Inc"', OFFERED)).toEqual(list("name", ["Harbor, Inc"]));
    expect(withPeerValue(number("stake", "gt", 1), "lots", OFFERED)).toBe("stake needs a number, e.g. stake>1000000.");
    expect(withPeerValue(list("geo", ["DE"]), "", OFFERED)).toBe("geo needs a value, e.g. geo:DE,NL.");
  });
});

describe("peerSuggest", () => {
  const inserts = (before: string) => peerSuggest(before, OFFERED).map((option) => option.insert);

  it("offers keys, and kinds of peer by their own names", () => {
    expect(inserts("fin")).toEqual(["finalization>", "finalization<"]);
    expect(inserts("del")).toEqual(["is:delinquent"]);
    expect(inserts("-na")).toEqual(["-name:"]);
    expect(inserts("zzz")).toEqual([]);
  });

  it("offers values with how many peers hold them", () => {
    expect(peerSuggest("client:", OFFERED).map((option) => [option.insert, option.what])).toEqual([
      ["client:Agave", "3 peers"],
      ["client:JitoLabs", "1 peer"],
    ]);
    expect(peerSuggest("geo:u", OFFERED)[0]?.what).toBe("United States, 2 peers");
    expect(inserts("is:r")).toEqual(["is:rpc"]);
    expect(inserts("geo:US,")).toEqual(["geo:US,BR"]);
    expect(inserts("name:bor")).toEqual(['name:"Harbor, Inc"']);
    expect(inserts("ip:1")).toEqual([]);
  });

  it("leaves out what the table does not show", () => {
    expect(peerSuggest("arr", NO_ARRIVALS)).toEqual([]);
  });
});

describe("peerBrowse", () => {
  it("offers the kinds of peer with their counts, then every key", () => {
    const rows = peerBrowse(OFFERED, []);
    expect(rows.slice(0, 2).map((row) => [row.group, row.syntax, row.figure])).toEqual([
      ["only", "is:staked", "3"],
      ["only", "is:rpc", "1"],
    ]);
    const find = (syntax: string) => rows.find((row) => row.syntax === syntax)?.action;
    expect(find("client:")).toEqual({ kind: "type", text: "client:", from: 7 });
    expect(find("finalization<50")).toEqual({ kind: "condition", condition: number("finalization", "lt", 50) });
  });

  it("drops a kind already in the box, and keys the table does not show", () => {
    const rows = peerBrowse(NO_ARRIVALS, ["staked"]);
    expect(rows.some((row) => row.syntax === "is:staked")).toBe(false);
    expect(rows.some((row) => row.syntax.startsWith("arrival"))).toBe(false);
  });
});
