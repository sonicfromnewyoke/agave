import { describe, expect, it } from "vitest";
import { accountsDevices, cacheHits, indexFound, perSecond, upkeep } from "./accounts";
import type { AccountsCache, AccountsDb, DeviceLoad, FilesystemUsage } from "./types";

function db(over: Partial<AccountsDb> = {}): AccountsDb {
  return {
    index_entries: 1_100_000_000,
    index_capacity: 1_400_000_000,
    index_bytes: 96_000_000_000,
    write_cache_bytes: 700_000_000,
    write_cache_slots: 40,
    write_cache_accounts: 90_000,
    cleaned_to: 465_584_886,
    index_found: 900,
    index_missing: 100,
    index_inserts: 30,
    index_deletes: 20,
    clean_runs: 2,
    cleaned_slots: 200,
    cleaned_accounts: 50_000,
    shrunk_slots: 12,
    shrunk_accounts: 4_000,
    shrunk_bytes: 8_000_000,
    rewritten_bytes: 30_000_000,
    clean_micros: 3_000_000,
    shrink_micros: 600_000,
    flush_micros: 90_000_000,
    window_seconds: 60,
    ...over,
  };
}

function filesystem(name: string, path: string, live = false): FilesystemUsage {
  return {
    name,
    path,
    total: 1,
    available: 1,
    reserved: 0,
    parts: live ? [{ key: "live", bytes: 1 }] : [],
    leftovers: [],
  };
}

function device(roles: string[], read: number, write: number, operations: number): DeviceLoad {
  return {
    device: "nvme0n1",
    roles,
    busy: 0,
    wait_ms: null,
    operations_per_second: operations,
    read_per_second: read,
    write_per_second: write,
  };
}

describe("perSecond", () => {
  it("is nought before the window holds a sample", () => {
    expect(perSecond(600, 60)).toBe(10);
    expect(perSecond(600, 0)).toBe(0);
  });
});

describe("cacheHits", () => {
  it("splits the reads by the rate", () => {
    const cache = { read: 2_210_362, hit_rate: 2_151_345 / 2_210_362 } as AccountsCache;
    expect(cacheHits(cache)).toEqual({ hits: 2_151_345, misses: 59_017 });
  });
});

describe("accountsDevices", () => {
  it("sums the devices under every accounts path", () => {
    const host = {
      filesystems: [
        filesystem("ledger", "/mnt/ledger"),
        filesystem("accounts 1", "/mnt/a"),
        filesystem("accounts 2", "/mnt/b"),
      ],
      devices: [
        device(["ledger"], 1, 1, 1),
        device(["accounts 1"], 100, 200, 30),
        device(["accounts 2"], 10, 20, 3),
      ],
    };
    expect(accountsDevices(host)).toEqual({
      paths: ["/mnt/a", "/mnt/b"],
      read: 110,
      write: 220,
      operations: 33,
    });
  });

  it("follows storages kept on the ledger's filesystem", () => {
    const host = {
      filesystems: [filesystem("ledger", "/mnt/ledger", true), filesystem("snapshots", "/mnt/s")],
      devices: [device(["ledger"], 5, 6, 7), device(["snapshots"], 1, 1, 1)],
    };
    expect(accountsDevices(host)).toEqual({ paths: ["/mnt/ledger"], read: 5, write: 6, operations: 7 });
  });

  it("has no rates where no device stands behind the path", () => {
    const host = { filesystems: [filesystem("accounts", "/dev/shm/accounts")], devices: [] };
    expect(accountsDevices(host)).toEqual({
      paths: ["/dev/shm/accounts"],
      read: null,
      write: null,
      operations: null,
    });
    expect(accountsDevices(null).paths).toEqual([]);
  });
});

describe("upkeep", () => {
  it("shares each job's time of the window, capped at all of it", () => {
    expect(upkeep(db()).map((job) => [job.key, job.share])).toEqual([
      ["clean", 0.05],
      ["shrink", 0.01],
      ["flush", 1],
    ]);
    expect(upkeep(db({ window_seconds: 0 })).every((job) => job.share === 0)).toBe(true);
  });
});

describe("indexFound", () => {
  it("is null before the index has been asked", () => {
    expect(indexFound(db())).toBe(0.9);
    expect(indexFound(db({ index_found: 0, index_missing: 0 }))).toBeNull();
  });
});
