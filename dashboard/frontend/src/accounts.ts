/** The accounts page's figures, from the cache and database summaries and the host's disks. */

import type { AccountsCache, AccountsDb, Host } from "./types";

/** A window's total as a rate; nought before the window holds a sample. */
export function perSecond(total: number, windowSeconds: number): number {
  return windowSeconds > 0 ? total / windowSeconds : 0;
}

/** The read cache's own hits and misses, which cover only reads past the write cache. */
export function cacheHits(cache: AccountsCache): { hits: number; misses: number } {
  const hits = Math.round(cache.read * cache.hit_rate);
  return { hits, misses: cache.read - hits };
}

export interface AccountsDevices {
  /** Where the storages live, a path per filesystem. */
  paths: string[];
  /** Bytes a second, summed over the devices; null where no device stands behind the path. */
  read: number | null;
  write: number | null;
  operations: number | null;
}

/** The filesystems holding the storages are named for them, or share the ledger's and carry
 *  the live part. */
export function accountsDevices(
  host: Pick<Host, "filesystems" | "devices"> | null | undefined,
): AccountsDevices {
  const filesystems = (host?.filesystems ?? []).filter(
    (filesystem) =>
      filesystem.name.startsWith("accounts") || filesystem.parts.some((part) => part.key === "live"),
  );
  const devices = (host?.devices ?? []).filter((device) =>
    filesystems.some((filesystem) => device.roles.includes(filesystem.name)),
  );
  const sum = (of: (device: (typeof devices)[number]) => number) =>
    devices.length > 0 ? devices.reduce((total, device) => total + of(device), 0) : null;
  return {
    paths: filesystems.map((filesystem) => filesystem.path),
    read: sum((device) => device.read_per_second),
    write: sum((device) => device.write_per_second),
    operations: sum((device) => device.operations_per_second),
  };
}

export interface Upkeep {
  key: "clean" | "shrink" | "flush";
  label: string;
  micros: number;
  /** Of the window. Jobs run on threads of their own, so the shares need not sum to one. */
  share: number;
}

export function upkeep(db: AccountsDb): Upkeep[] {
  const windowMicros = db.window_seconds * 1_000_000;
  const job = (key: Upkeep["key"], label: string, micros: number): Upkeep => ({
    key,
    label,
    micros,
    share: windowMicros > 0 ? Math.min(1, micros / windowMicros) : 0,
  });
  return [
    job("clean", "Clean", db.clean_micros),
    job("shrink", "Shrink", db.shrink_micros),
    job("flush", "Flush", db.flush_micros),
  ];
}

/** Null before the index has been asked anything in the window. */
export function indexFound(db: AccountsDb): number | null {
  const asked = db.index_found + db.index_missing;
  return asked > 0 ? db.index_found / asked : null;
}
