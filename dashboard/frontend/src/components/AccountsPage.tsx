import type { ReactElement, ReactNode } from "react";
import { accountsDevices, cacheHits, indexFound, perSecond, upkeep } from "../accounts";
import { rateTone, servedFromMemory } from "../caches";
import { bytes, count, micros, percent } from "../format";
import type { AccountsCache, AccountsDb, AccountsDisk } from "../types";
import { useStore } from "../useStore";
import { Card, Explain, Meter, Stat } from "./primitives";

/** The accounts database: its caches, its storage files, the jobs that tidy them, and its index. */
export function AccountsPage(): ReactElement {
  const store = useStore();
  const cache = store.get("summary", "accounts_cache");
  const db = store.get("summary", "accounts_db");
  const host = store.get("summary", "host");
  if (!cache && !db) {
    return <p className="acct-empty">The validator has not reported its accounts database yet.</p>;
  }

  const devices = accountsDevices(host);
  return (
    <div className="grid acct">
      <Card title="Caches" aside="last minute" className="acct-body">
        <CacheBody cache={cache ?? null} db={db ?? null} />
      </Card>
      <Card title="Disk" aside={devices.paths.join(", ")} className="acct-body">
        <DiskBody cache={cache ?? null} disk={cache?.disk ?? null} devices={devices} />
      </Card>
      <Card title="Clean and shrink" aside="last minute" className="acct-body">
        {db ? <UpkeepBody db={db} /> : <Waiting />}
      </Card>
      <Card title="Index" className="acct-body">
        {db ? <IndexBody db={db} /> : <Waiting />}
      </Card>
    </div>
  );
}

function Waiting() {
  return <p className="cache-footnote">Not reported yet.</p>;
}

const rate = (total: number, seconds: number) => `${count(Math.round(perSecond(total, seconds)))}/s`;

/** A size or a time on one line, what it is measured against beside it, and a bar where that is
 *  a share. */
function Level({
  label,
  explain,
  value,
  beside,
  fraction,
}: {
  label: string;
  explain: string;
  value: ReactNode;
  beside: string;
  fraction?: number;
}) {
  return (
    <div className="cache-storage">
      <div className="cache-storage-head">
        <Explain text={explain}>
          <span className="cache-storage-label">{label}</span>
        </Explain>
        <span className="cache-storage-value">
          {value}
          <span className="cache-storage-limit"> {beside}</span>
        </span>
      </div>
      {fraction !== undefined && <Meter fraction={fraction} />}
    </div>
  );
}

function CacheBody({ cache, db }: { cache: AccountsCache | null; db: AccountsDb | null }) {
  const served = cache ? servedFromMemory(cache) : null;
  const read = cache ? cacheHits(cache) : null;
  return (
    <>
      {cache && served && read && (
        <div className="cache-figures">
          <Stat
            label="Read cache hit rate"
            explain="Share of the reads that got past the write cache and found their account in the read cache."
            value={percent(cache.hit_rate, 2)}
            tone={rateTone(cache.hit_rate)}
            sub={`${count(read.hits)} hits, ${count(read.misses)} misses`}
          />
          <Stat
            label="Reads"
            explain="Accounts loaded, and the share answered from either cache without touching a storage file."
            value={rate(served.loads, cache.window_seconds)}
            sub={`${served.rate === null ? "—" : percent(served.rate, 2)} from memory`}
          />
          <Stat
            label="Writes"
            explain="Accounts flushed from the write cache to storage files."
            value={rate(cache.stored_accounts, cache.window_seconds)}
            sub={`${bytes(Math.round(perSecond(cache.stored_bytes, cache.window_seconds)))}/s`}
          />
          <Stat
            label="Evicted"
            explain="Accounts dropped from the read cache to keep it under its size limit."
            value={count(cache.evictions)}
          />
        </div>
      )}
      {cache && (
        <Level
          label="Read cache"
          explain="Accounts kept in memory after being read from a storage file."
          value={bytes(cache.cache_bytes)}
          beside={`${count(cache.cache_entries)} accounts`}
        />
      )}
      {db && (
        <Level
          label="Write cache"
          explain="Accounts written by recent slots and not yet flushed to a storage file."
          value={bytes(db.write_cache_bytes)}
          beside={`${count(db.write_cache_accounts)} accounts in ${count(db.write_cache_slots)} slots`}
        />
      )}
    </>
  );
}

function DiskBody({
  cache,
  disk,
  devices,
}: {
  cache: AccountsCache | null;
  disk: AccountsDisk | null;
  devices: ReturnType<typeof accountsDevices>;
}) {
  if (!disk) return <Waiting />;
  const share = (part: number) => (disk.allocated > 0 ? part / disk.allocated : 0);
  return (
    <>
      <div className="cache-figures">
        <Stat
          label="Used"
          explain="Bytes of the storage files that live accounts still reference."
          value={bytes(disk.used)}
        />
        <Stat
          label="Fragmentation"
          explain="Allocated bytes no live account references, which shrink reclaims."
          value={bytes(disk.fragmented)}
          sub={percent(share(disk.fragmented), 1)}
        />
        <Stat
          label="Allocated"
          explain="Space the storage files take, across every accounts path."
          value={bytes(disk.allocated)}
          sub={`${count(disk.storages)} files`}
        />
      </div>
      <Meter fraction={share(disk.used)} />
      <div className="cache-figures">
        {devices.read !== null && (
          <Stat
            label="Read"
            explain="Bytes read from the devices under the accounts paths, by any process."
            value={`${bytes(devices.read)}/s`}
          />
        )}
        {devices.write !== null && (
          <Stat
            label="Write"
            explain="Bytes written to the devices under the accounts paths, by any process."
            value={`${bytes(devices.write)}/s`}
            sub={
              cache
                ? `${bytes(Math.round(perSecond(cache.stored_bytes, cache.window_seconds)))}/s of accounts`
                : undefined
            }
          />
        )}
        {devices.operations !== null && (
          <Stat
            label="Operations"
            explain="Reads and writes the devices completed."
            value={`${count(devices.operations)}/s`}
          />
        )}
        {cache && (
          <Stat
            label="Reads from storage"
            explain="Account reads that missed both caches and went to a storage file."
            value={rate(cache.from_storage, cache.window_seconds)}
          />
        )}
      </div>
    </>
  );
}

function UpkeepBody({ db }: { db: AccountsDb }) {
  return (
    <>
      <div className="cache-figures">
        <Stat
          label="Reclaimed"
          explain="Dead bytes shrink dropped from storage files."
          value={bytes(db.shrunk_bytes)}
          sub={`${count(db.shrunk_accounts)} accounts, ${count(db.shrunk_slots)} slots`}
        />
        <Stat
          label="Relocated"
          explain="Live bytes shrink wrote again to drop the dead ones beside them."
          value={bytes(db.rewritten_bytes)}
          sub={`${bytes(Math.round(perSecond(db.rewritten_bytes, db.window_seconds)))}/s`}
        />
        <Stat
          label="Cleaned"
          explain="Index entries clean removed: old versions of accounts and accounts closed."
          value={count(db.cleaned_accounts)}
          sub={`${count(db.clean_runs)} ${db.clean_runs === 1 ? "run" : "runs"}, ${count(db.cleaned_slots)} slots`}
        />
        <Stat
          label="Cleaned through"
          explain="The newest root the last clean covered."
          value={db.cleaned_to > 0 ? count(db.cleaned_to) : "—"}
        />
      </div>
      {upkeep(db).map((job) => (
        <Level
          key={job.key}
          label={job.label}
          explain={EXPLAIN[job.key]}
          value={micros(job.micros)}
          beside={`${percent(job.share, 1)} of the time`}
          fraction={job.share}
        />
      ))}
    </>
  );
}

const EXPLAIN = {
  clean: "Time spent finding account versions no root needs any more. Counted as a run ends.",
  shrink: "Time spent rewriting storage files without their dead accounts, old slots' files packed together.",
  flush: "Time spent writing rooted slots from the write cache to storage files.",
};

function IndexBody({ db }: { db: AccountsDb }) {
  const found = indexFound(db);
  return (
    <>
      <div className="cache-figures">
        <Stat
          label="Accounts"
          explain="Entries in the index, one per account."
          value={count(db.index_entries)}
        />
        <Stat
          label="Memory"
          explain="The validator's own estimate of what the index holds in memory."
          value={bytes(db.index_bytes)}
        />
        <Stat
          label="Lookups"
          explain="Index lookups, and the share that found an account. The index reports every ten seconds."
          value={rate(db.index_found + db.index_missing, db.window_seconds)}
          sub={found === null ? undefined : `${percent(found, 2)} found`}
        />
        <Stat
          label="Inserted"
          explain="Entries added to the index and removed from it over the last minute."
          value={count(db.index_inserts)}
          sub={`${count(db.index_deletes)} deleted`}
        />
      </div>
      <Level
        label="Used"
        explain="Entries held against the slots of the index's hash maps."
        value={count(db.index_entries)}
        beside={`of ${count(db.index_capacity)}`}
        fraction={db.index_capacity > 0 ? db.index_entries / db.index_capacity : 0}
      />
    </>
  );
}
