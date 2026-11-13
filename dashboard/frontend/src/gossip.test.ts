import { describe, expect, it } from "vitest";
import {
  ariaSort,
  dropPercent,
  dropShare,
  arrivalMedian,
  arrivalText,
  arrivalTitle,
  entryRows,
  filterCounts,
  FIRST_SORT,
  gossipPeers,
  gossipVerdict,
  matchesSearch,
  nextSort,
  rate,
  sortPeers,
  stakeText,
  timeParts,
  type PeerColumn,
} from "./gossip";
import type { Gossip, GossipPeers } from "./types";

const LIST: GossipPeers = {
  root: 10_000,
  total_stake: 1_000,
  clients: [
    ["Agave", "4.3.0"],
    ["JitoLabs", "4.3.0"],
  ],
  identity: ["Big1111", "Mid2222", "Rpc3333", "Old4444"],
  name: ["Northwind", "harbor", null, "Pinecrest"],
  stake: [600, 300, 0, 50],
  client: [0, 1, 0, 0],
  ip: ["189.1.171.215", "67.202.63.79", "198.51.100.7", null],
  country: ["BR", "US", null, null],
  rpc: [null, null, 8899, null],
  finalization: [0.98, 0.25, null, 0.93],
  vote_arrival_epoch: 1_048,
  vote_arrival: [
    [92_400, 21_000, 388_000, 141_000, 1_802],
    [412_000, null, null, null, 1],
    null,
    [151_000, 34_000, 612_000, 203_000],
  ],
  delinquent: [false, false, false, true],
  last_vote: [9_990, 9_995, null, 1_000],
};

const PEERS = gossipPeers(LIST);

function order(column: PeerColumn, reversed = false): string[] {
  return sortPeers(PEERS, { column, reversed }).map((peer) => peer.identity);
}

describe("gossipPeers", () => {
  it("turns the columns into rows read against our root", () => {
    expect(PEERS[0]).toMatchObject({
      identity: "Big1111",
      client: "Agave v4.3.0",
    });
    expect(PEERS[1].client).toBe("JitoLabs v4.3.0");
    expect(PEERS[2]).toMatchObject({ rpc: 8899 });
  });

  it("carries each address's country and finds a peer by it", () => {
    expect(PEERS.map((peer) => peer.country)).toEqual(["BR", "US", null, null]);
    expect(PEERS.filter((peer) => matchesSearch(peer, "br")).map((peer) => peer.identity)).toEqual(["Big1111"]);
  });
});

describe("vote arrival", () => {
  it("reads each peer's cell, and none where votor sent nothing", () => {
    expect(PEERS[0].voteArrival).toEqual({ mean: 92_400, spread: 21_000, worst: 388_000, finalize: 141_000, votes: 1_802 });
    expect(PEERS[3].voteArrival?.votes, "a reply without the count").toBeNull();
    expect(PEERS[2].voteArrival).toBeNull();
  });

  it("takes the middle mean of the peers that have one", () => {
    expect(arrivalMedian(PEERS)).toBe(151_000);
    expect(arrivalMedian([PEERS[2]])).toBeNull();
  });

  it("says the epoch and whichever figures votor sent", () => {
    expect(arrivalTitle(PEERS[3].voteArrival!, 1_048)).toBe(
      "Notarize votes reached us 151 ms after the slot began on average in epoch 1,048, spread 34 ms, worst 612 ms; finalize votes 203 ms.",
    );
    expect(arrivalTitle(PEERS[0].voteArrival!, 1_048)).toBe(
      "Notarize votes reached us 92 ms after the slot began on average over 1,802 votes in epoch 1,048, spread 21 ms, worst 388 ms; finalize votes 141 ms.",
    );
    expect(arrivalTitle(PEERS[1].voteArrival!, null)).toBe(
      "Notarize votes reached us 412 ms after the slot began on average over 1 vote.",
    );
  });
});

describe("finalize arrival", () => {
  it("reads, marks and sorts by finalize votes when asked", () => {
    expect(arrivalText(PEERS[0].voteArrival!, "finalize")).toBe("141 ms");
    expect(arrivalText(PEERS[1].voteArrival!, "finalize")).toBe("—");
    expect(arrivalMedian(PEERS, "finalize")).toBe(203_000);
    expect(
      sortPeers(PEERS, { column: "arrival", reversed: false }, "finalize").map((peer) => peer.identity),
    ).toEqual(["Big1111", "Old4444", "Mid2222", "Rpc3333"]);
  });

  it("titles a row by its finalize votes", () => {
    expect(arrivalTitle(PEERS[3].voteArrival!, 1_048, "finalize")).toBe(
      "Finalize votes reached us 203 ms after the slot began on average in epoch 1,048; notarize votes 151 ms.",
    );
    expect(arrivalTitle(PEERS[1].voteArrival!, null, "finalize")).toBe(
      "No finalize vote was timed; notarize votes 412 ms.",
    );
  });
});

describe("filters and search", () => {
  it("counts every filter over the same rows", () => {
    expect(filterCounts(PEERS)).toEqual({ staked: 3, rpc: 1, delinquent: 1 });
  });

  it("reads delinquency, and how far the last vote is behind our root", () => {
    const old = PEERS.find((peer) => peer.identity === "Old4444");
    expect([old?.delinquent, old?.voteBehind]).toEqual([true, 9_000]);
    const older = gossipPeers({ ...LIST, delinquent: undefined, last_vote: undefined });
    expect(older.every((peer) => !peer.delinquent && peer.voteBehind === null)).toBe(true);
  });

  it("matches any part of a name, client, IP or identity", () => {
    const found = (query: string) => PEERS.filter((peer) => matchesSearch(peer, query)).map((peer) => peer.identity);
    expect(found("HARBOR")).toEqual(["Mid2222"]);
    expect(found("jito")).toEqual(["Mid2222"]);
    expect(found("198.51")).toEqual(["Rpc3333"]);
    expect(found("old4444")).toEqual(["Old4444"]);
    expect(found("  ")).toHaveLength(4);
  });
});

describe("sortPeers", () => {
  it("orders numbers high to low and text A to Z first", () => {
    expect(order("stake")).toEqual(["Big1111", "Mid2222", "Old4444", "Rpc3333"]);
    expect(order("name")).toEqual(["Mid2222", "Big1111", "Old4444", "Rpc3333"]);
  });

  it("reverses on the same heading and keeps missing values last", () => {
    expect(order("name", true)).toEqual(["Old4444", "Big1111", "Mid2222", "Rpc3333"]);
    expect(order("rpc")[0]).toBe("Rpc3333");
    expect(order("rpc", true)[0]).toBe("Rpc3333");
  });

  it("puts the highest finalization share first and a node without one last", () => {
    expect(order("finalization")).toEqual(["Big1111", "Old4444", "Mid2222", "Rpc3333"]);
    expect(order("finalization", true)).toEqual(["Mid2222", "Old4444", "Big1111", "Rpc3333"]);
  });

  it("puts the earliest vote arrival first and a node without one last", () => {
    expect(order("arrival")).toEqual(["Big1111", "Old4444", "Mid2222", "Rpc3333"]);
    expect(order("arrival", true)).toEqual(["Mid2222", "Old4444", "Big1111", "Rpc3333"]);
  });

  it("sorts addresses by value", () => {
    expect(order("ip")).toEqual(["Mid2222", "Big1111", "Rpc3333", "Old4444"]);
  });

  it("steps through a heading's two orders and names them for ARIA", () => {
    const byName = nextSort(FIRST_SORT, "name");
    expect(byName).toEqual({ column: "name", reversed: false });
    expect(nextSort(byName, "name")).toEqual({ column: "name", reversed: true });
    expect(ariaSort(byName, "name")).toBe("ascending");
    expect(ariaSort(FIRST_SORT, "stake")).toBe("descending");
    expect(ariaSort(FIRST_SORT, "name")).toBeUndefined();
  });
});

describe("text", () => {
  it("keeps two decimals of stake under one SOL", () => {
    expect(stakeText(17_840_232_000_000_000)).toBe("17,840,232");
    expect(stakeText(500_000_000)).toBe("0.50");
    expect(stakeText(0)).toBe("0");
  });

  it("keeps a decimal below ten", () => {
    expect(rate(0)).toBe("0");
    expect(rate(0.25)).toBe("0.3");
    expect(rate(11_534.6)).toBe("11,535");
  });
});

const GOSSIP: Gossip = {
  table: {
    entries: 231_904,
    pubkeys: 4_112,
    pubkey_capacity: 8_192,
    nodes: 4_000,
    staked_nodes: 1_212,
    expired_per_second: 176,
    evicted_last_minute: 0,
  },
  messages: [],
  entries: {
    accepted_push: 0,
    accepted_pull: 0,
    duplicate_push: 0,
    redundant_pull: 0,
    rejected_push: 0,
    rejected_pull: 0,
    types: [
      { kind: "Vote", push: 10, pull: 1, rejected: 2 },
      { kind: "EpochSlots", push: 5, pull: 1, rejected: 0 },
      { kind: "ContactInfo", push: 4, pull: 1, rejected: 0 },
      { kind: "SnapshotHashes", push: 3, pull: 0, rejected: 0 },
      { kind: "LowestSlot", push: 2, pull: 0, rejected: 0 },
      { kind: "Version", push: 1, pull: 0, rejected: 1 },
      { kind: "NodeInstance", push: 1, pull: 0, rejected: 0 },
      { kind: "LegacyVersion", push: 0, pull: 0, rejected: 0 },
    ],
  },
  pressure: {
    dropped_in: 0,
    dropped_out: 0,
    pull_no_budget: 0,
    pull_scan_exhausted: 0,
    other_shred_version: 0,
    ping_check_failed: 0,
    unverified_addresses: 0,
    bad_prune_destination: 0,
  },
  time: { push: 150, pull_requests: 90, pull_responses: 50, ping_pong_prune: 10, verify: 75, other: -5 },
  dropped_last_minute: 0,
  packets_last_minute: 600_000,
  packets_per_second: 10_000,
};

describe("cards", () => {
  it("names the five busiest types and sums the active rest", () => {
    const { shown, others } = entryRows(GOSSIP.entries.types);
    expect(shown.map((row) => row.label)).toEqual(["Vote", "EpochSlots", "ContactInfo", "SnapshotHashes", "LowestSlot"]);
    expect(others).toEqual({ label: "2 other types", push: 2, pull: 0, rejected: 1 });
    expect(entryRows(GOSSIP.entries.types.slice(0, 5)).others).toBeNull();
    const idle = GOSSIP.entries.types.map((type) => (type.kind === "Vote" ? type : { ...type, push: 0, pull: 0, rejected: 0 }));
    expect(entryRows(idle)).toEqual({ shown: [{ label: "Vote", kind: "Vote", push: 10, pull: 1, rejected: 2 }], others: null });
  });

  it("shares the time out, with a negative remainder as nothing", () => {
    const { parts, total } = timeParts(GOSSIP.time);
    expect(total).toBe(375);
    expect(parts[0]).toMatchObject({ label: "Handling pushes", millis: 150, share: 0.4 });
    expect(parts.find((part) => part.key === "other")?.millis).toBe(0);
  });
});

describe("gossipVerdict", () => {
  it("says when the points stop", () => {
    expect(gossipVerdict(null, null).tone).toBe("muted");
  });

  it("counts nodes and their stake from the peer list when it is in", () => {
    expect(gossipVerdict(GOSSIP, null).detail).toBe("4,000 nodes heard. Nothing dropped in the last minute.");
    expect(gossipVerdict(GOSSIP, LIST)).toEqual({
      tone: "good",
      headline: "Gossip is keeping up.",
      detail: "4 nodes heard, 95.0% of stake among them. Nothing dropped in the last minute.",
    });
  });

  it("keeps a few drops in a busy minute good", () => {
    const verdict = gossipVerdict({ ...GOSSIP, dropped_last_minute: 384 }, LIST);
    expect(verdict.tone).toBe("good");
    expect(verdict.detail).toMatch(/384 of 600,000 dropped in the last minute \(0\.06%\)\.$/);
  });

  it("warns from one percent of packets", () => {
    const verdict = gossipVerdict({ ...GOSSIP, dropped_last_minute: 6_000 }, LIST);
    expect(verdict.tone).toBe("warn");
    expect(verdict.headline).toBe("Gossip is dropping packets.");
    expect(verdict.detail).toMatch(/6,000 of 600,000 dropped in the last minute \(1\.0%\)\.$/);
    expect(gossipVerdict({ ...GOSSIP, dropped_last_minute: 5_999 }, LIST).tone).toBe("good");
  });

  it("warns on drops with no packets counted", () => {
    expect(gossipVerdict({ ...GOSSIP, dropped_last_minute: 3, packets_last_minute: 0 }, LIST).tone).toBe("warn");
  });
});

describe("drop share", () => {
  it("is nothing without drops and all of it without packets", () => {
    expect(dropShare(0, 0)).toBe(0);
    expect(dropShare(5, 0)).toBe(1);
    expect(dropShare(50, 10)).toBe(1);
    expect(dropShare(6.4, 8_000)).toBeCloseTo(0.0008);
  });

  it("keeps a trickle from reading as nothing", () => {
    expect(dropPercent(0.000007)).toBe("<0.01%");
    expect(dropPercent(0.0008)).toBe("0.08%");
    expect(dropPercent(0.017)).toBe("1.7%");
    expect(dropPercent(0.25)).toBe("25%");
    expect(dropPercent(0)).toBe("0.0%");
  });
});
