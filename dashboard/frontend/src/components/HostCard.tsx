import { Fragment, type ReactElement } from "react";
import { bytes, count, decimal, percent } from "../format";
import {
  availableTone,
  busyTone,
  cpuTone,
  deviceLabel,
  diskRows,
  diskSplit,
  fullness,
  fullnessTone,
  loadTrend,
  memoryUse,
  PERFORMANCE_GOVERNOR,
  residentTrend,
  slowGovernor,
  swapTone,
  validatorSplit,
  waitTone,
  type DiskSplit,
  type ValidatorSplit,
} from "../host";
import {
  barClass,
  busiest,
  onCpuTone,
  pinnedLabel,
  threadRows,
  THREADS_WINDOW,
  type ThreadRow,
} from "../threads";
import { snapshotWriting, snapshotWritten } from "../snapshot";
import type { DeviceLoad, FilesystemUsage, ThreadsSample } from "../types";
import { useStore } from "../useStore";
import { Explain, Fold, type FoldRule } from "./primitives";

/** A bar means a container that can fill; load and device saturation get none. */
export function HostCard(): ReactElement | null {
  const store = useStore();
  const host = store.get("summary", "host");
  const samples = store.getThreads();
  if (!host) return null;

  const memory = memoryUse(host);
  const split = validatorSplit(host);
  // The validator's file and shared pages sit in the page cache, so only its heap is in use.
  const validatorHeap = host.process_kinds
    ? Math.min(Math.max(0, host.process_kinds.anonymous), memory.inUse)
    : 0;
  const trend = loadTrend(host);
  const resident = residentTrend(host);
  const snapshots = store.get("summary", "snapshots");
  const serverTime = store.get("summary", "server_time_nanos");
  const writing = snapshotWriting(snapshots, serverTime === undefined ? undefined : serverTime / 1e6);
  const top = busiest(threadRows(samples));
  const ledger =
    host.filesystems.find((filesystem) => filesystem.name === "ledger") ??
    [...host.filesystems].sort((a, b) => fullness(b) - fullness(a))[0];

  const rule: FoldRule = {
    kind: "meters",
    segs: [
      ...(host.cores > 0 ? [{ pct: (host.load_one / host.cores) * 100 }] : []),
      ...(host.cpu ? [{ pct: host.cpu.busy * 100 }] : []),
      ...(ledger ? [{ pct: fullness(ledger) * 100 }] : []),
    ],
  };

  const summary = (
    <>
      load <b>{decimal(host.load_one, 2)}</b> of {count(host.cores)} cores
      {host.cpu && (
        <>
          , cpu <b>{percent(host.cpu.busy, 0)}</b> busy
        </>
      )}
      , <b>{bytes(memory.available)}</b> free
      {ledger && (
        <>
          , {ledger.name} <b>{percent(fullness(ledger), 0)}</b> full
        </>
      )}
      {top && (
        <>
          , busiest thread {top.label} at <b>{percent(top.now, 0)}</b>
        </>
      )}
      {writing && <>, writing a snapshot</>}
    </>
  );

  const rows = diskRows(host.filesystems, host.devices);

  return (
    <Fold id="machine" title="Machine" summary={summary} rule={rule}>
      {slowGovernor(host.cpu_governor) && (
        <p className="host-governor">
          <span className="tone-warn">CPU governor {host.cpu_governor}.</span> Voting nodes run{" "}
          {PERFORMANCE_GOVERNOR}: slower clocks delay each vote.
        </p>
      )}
      <div className="host-body">
      <div className="host-top">
        <div className="host-figure">
          <div className="host-label">
            <Explain text="Threads wanting a processor, averaged over one, five and fifteen minutes, against the core count.">
              Load average
            </Explain>
          </div>
          <div className="host-value">
            {decimal(host.load_one, 2)} <small>of {count(host.cores)} cores</small>
          </div>
          <div className="host-memory" aria-hidden="true">
            <i
              className={host.cores > 0 && host.load_one > host.cores ? "is-load is-over" : "is-load"}
              style={{ width: share(host.load_one, host.cores) }}
            />
          </div>
          <div className="host-sub">
            5m {decimal(host.load_five, 1)}, 15m {decimal(host.load_fifteen, 1)},{" "}
            <span className={`host-trend is-${trend}`}>{trend}</span>
            <br />
            <span className="host-faint">
              {count(host.threads)} threads, {count(host.running)} running
            </span>
          </div>
        </div>

        {/* Absent where /proc/stat could not be read; the rest of the row
            does not depend on it. */}
        {host.cpu && (
          <div className="host-figure">
            <div className="host-label">
              <Explain text="Share of the last second all cores spent working, iowait in orange.">
                CPU busy
              </Explain>
            </div>
            <div className={`host-value tone-${cpuTone(host.cpu.busy)}`}>
              {percent(host.cpu.busy, 0)} <small>of {count(host.cores)} cores</small>
            </div>
            <div className="host-memory" aria-hidden="true">
              <i className="is-busy" style={{ width: share(host.cpu.busy, 1) }} />
              <i className="is-iowait" style={{ width: share(host.cpu.iowait, 1) }} />
            </div>
            <div className="host-sub">
              user {percent(host.cpu.user, 0)}, system {percent(host.cpu.system, 0)},{" "}
              iowait {percent(host.cpu.iowait, 0)}
              {host.cpu.steal >= 0.001 && (
                <>
                  {" "}
                  , <span className="tone-warn">steal {percent(host.cpu.steal, 1)}</span>
                </>
              )}
            </div>
          </div>
        )}

        <div className="host-figure">
          <div className="host-label">
            <Explain
              text={
                validatorHeap > 0
                  ? "Memory in use less what the kernel can reclaim; the bar shows validator heap, other processes, reclaimable cache."
                  : "Memory in use less what the kernel can reclaim, which is the lighter part of the bar."
              }
            >
              Memory in use
            </Explain>
          </div>
          <div className="host-value">
            {bytes(memory.inUse)} <small>of {bytes(memory.total)}</small>
          </div>
          <div className="host-memory" aria-hidden="true">
            {validatorHeap > 0 && (
              <i className="is-validator" style={{ width: share(validatorHeap, memory.total) }} />
            )}
            <i
              className={validatorHeap > 0 ? "is-others" : "is-used"}
              style={{ width: share(memory.inUse - validatorHeap, memory.total) }}
            />
            <i className="is-cache" style={{ width: share(memory.reclaimable, memory.total) }} />
          </div>
          <div className="host-sub">
            <span className={`tone-${availableTone(memory.available, memory.total)}`}>
              {bytes(memory.available)} available
            </span>
            , {bytes(memory.reclaimable)} reclaimable
            {host.process_resident !== null && (
              <>
                <br />
                {split ? (
                  <ValidatorMemory split={split} />
                ) : (
                  <>validator {bytes(host.process_resident)}</>
                )}
                {/* A line of its own, kept even when empty, so the figure does not change height
                    as the trend comes and goes. */}
                <br />
                <span className={`host-trend is-${resident?.direction ?? "steady"}`}>
                  {resident?.label ?? "\u00a0"}
                </span>
              </>
            )}
          </div>
        </div>

        {host.swap && (
          <div className="host-figure">
            <div className="host-label">
              <Explain text="Swap in use. There is no healthy amount.">
                Swap used
              </Explain>
            </div>
            <div className={`host-value tone-${swapTone(host.swap.used)}`}>
              {bytes(host.swap.used)} <small>of {bytes(host.swap.total)}</small>
            </div>
            <div className="host-sub">
              {host.swap.used > 0 ? "in use, which it should not be" : "none, as it should be"}
            </div>
          </div>
        )}
      </div>

      <div className="host-lower">
      <div
        className={
          host.filesystems.length > 0 && host.devices.length > 0
            ? "host-disks is-paired"
            : "host-disks"
        }
      >
        {host.filesystems.length > 0 && (
          <>
            <div className="host-group">
              <Explain text="How much of each filesystem is used. A full ledger partition halts the validator.">
                <span>How full</span>
              </Explain>
              <em>statvfs</em>
            </div>
            <div className="host-capacity is-head">
              <span>Filesystem</span>
              <span>What fills it</span>
              <span className="host-n">Used</span>
              <span />
            </div>
            {rows.filesystems.map(({ filesystem, gridRow, hasDevice }) => (
              <Fragment key={filesystem.path}>
                <Capacity filesystem={filesystem} gridRow={gridRow} />
                {!hasDevice && (
                  <div className="host-no-device" style={{ gridRow }}>
                    —
                  </div>
                )}
              </Fragment>
            ))}
          </>
        )}

        {host.devices.length > 0 && (
          <>
            <div className="host-group is-worked">
              <Explain text="Time busy is the share of the second with a request in flight. Wait is the mean time a request spent queued and serviced.">
                <span>How hard worked</span>
              </Explain>
              <em>diskstats</em>
            </div>
            <div className="host-device is-head">
              <span>Device</span>
              <span className="host-n">Time busy</span>
              <span className="host-n is-wait">Wait</span>
              <span className="host-n is-io">IOPS</span>
              <span className="host-n is-tp">Read and write</span>
            </div>
            {rows.devices.map(({ device, gridRow, shared }) => (
              <Device
                key={device.device}
                device={device}
                gridRow={gridRow}
                shared={shared}
                writing={writing !== null && device.device === host.snapshot_device}
              />
            ))}
            {writing && (
              <div className="host-sub host-snapshot tone-warn" style={{ gridRow: rows.noteRow }}>
                {writing}
              </div>
            )}
            {!writing && snapshots?.last_written && (
              <div className="host-sub host-snapshot host-faint" style={{ gridRow: rows.noteRow }}>
                {snapshotWritten(snapshots.last_written)}
              </div>
            )}
          </>
        )}
      </div>

      <Threads samples={samples} />
      </div>
      </div>
    </Fold>
  );
}

/** The validator line, with a bar of its parts that opens them as a list. */
function ValidatorMemory({ split }: { split: ValidatorSplit }): ReactElement {
  const whole = split.segments.reduce((sum, segment) => sum + segment.bytes, 0);
  const rest = split.segments.find((segment) => segment.key === "rest")?.bytes ?? 0;
  const list = (
    <>
      <span className="figure-panel-label">Validator memory, {bytes(split.resident)}</span>
      {split.rows.map((row) => (
        <span key={row.key} className={`memory-row is-depth-${row.depth}`}>
          <i className={`memory-key is-${row.key}`} />
          <span className="memory-name">{row.label}</span>
          <span className="memory-n">
            {row.key === "index" ? `~${bytes(row.bytes)} est.` : bytes(row.bytes)}
          </span>
        </span>
      ))}
    </>
  );
  return (
    <Explain className="memory-split" text={list}>
      validator {bytes(split.resident)}
      <span className="memory-split-bar" aria-hidden="true">
        {split.segments.map((segment) => (
          <i key={segment.key} className={`is-${segment.key}`} style={{ width: share(segment.bytes, whole) }}>
            {segment.key === "rest" && split.indexEstimate !== null && (
              <b style={{ width: share(split.indexEstimate, rest) }} />
            )}
          </i>
        ))}
      </span>
    </Explain>
  );
}

/** Parts of the whole disk, so the bar ends where the percentage says. */
function DiskBar({ name, total, split }: { name: string; total: number; split: DiskSplit }): ReactElement {
  const list = (
    <>
      <span className="figure-panel-label">
        {name}, {bytes(split.used)} used
      </span>
      {split.segments.map((segment) => (
        <Fragment key={segment.key}>
          <span className="memory-row">
            <i className={`memory-key is-${segment.key}`} />
            <span className="memory-name">{segment.label}</span>
            <span className="memory-n">{bytes(segment.bytes)}</span>
          </span>
          {segment.key === "leftover" && (
            <span className="disk-leftover">includes {split.leftovers.join(", ")}</span>
          )}
        </Fragment>
      ))}
    </>
  );
  return (
    <Explain className="disk-split" text={list}>
      <span className="host-track is-split" role="img" aria-label={`What fills ${name}`}>
        {split.segments.map((segment) => (
          <i key={segment.key} className={`is-${segment.key}`} style={{ width: share(segment.bytes, total) }} />
        ))}
      </span>
    </Explain>
  );
}

function Capacity({ filesystem, gridRow }: { filesystem: FilesystemUsage; gridRow: string }) {
  const full = fullness(filesystem);
  const tone = fullnessTone(full);
  const split = diskSplit(filesystem);
  return (
    <div className="host-capacity" style={{ gridRow }}>
      {/* The name never truncates; the path does, and is carried whole as a title. */}
      <span className="host-mount" title={filesystem.path}>
        <b>{filesystem.name}</b>
        <s>{filesystem.path}</s>
      </span>
      {split ? (
        <DiskBar name={filesystem.name} total={filesystem.total} split={split} />
      ) : (
        <span className="host-track">
          <i className={`tone-fill-${tone}`} style={{ width: `${full * 100}%` }} />
        </span>
      )}
      <span className={`host-n tone-${tone}`}>{percent(full, 0)}</span>
      <span className="host-free">{bytes(filesystem.available)} free</span>
    </div>
  );
}

/** Its name is toned while a snapshot is being written to it. */
function Device({
  device,
  gridRow,
  shared,
  writing,
}: {
  device: DeviceLoad;
  gridRow: string;
  shared: boolean;
  writing: boolean;
}) {
  return (
    <div className={shared ? "host-device is-shared" : "host-device"} style={{ gridRow }}>
      <span className="host-dev">
        <b className={writing ? "tone-warn" : undefined}>{device.device}</b>
        <s>{deviceLabel(device)}</s>
      </span>
      <span className={`host-n tone-${busyTone(device.busy)}`}>{percent(device.busy, 0)}</span>
      <span className={`host-n is-wait tone-${waitTone(device.wait_ms)}`}>
        {device.wait_ms === null ? "—" : `${decimal(device.wait_ms, 2)} ms`}
      </span>
      <span className="host-n is-io host-faint">{count(device.operations_per_second)}</span>
      <span className="host-n is-tp host-faint">
        {bytes(device.read_per_second)}, {bytes(device.write_per_second)}
      </span>
    </div>
  );
}

function share(part: number, whole: number): string {
  if (whole <= 0) return "0%";
  return `${Math.min(100, (part / whole) * 100)}%`;
}

function Threads({ samples }: { samples: ThreadsSample[] }) {
  const rows = threadRows(samples);
  const last = samples[samples.length - 1];
  if (!last || rows.length === 0) return null;

  return (
    <section className="host-threads">
      <div className="host-group">
        <Explain text="Each row is a thread's minute on a core, a pool row the mean of its threads.">
          <span>Validator threads</span>
        </Explain>
        <em>{count(last.threads)} threads, last 60s</em>
      </div>
      <div className="host-thread is-head">
        <span>Thread</span>
        <span className="host-n is-count">Count</span>
        <span
          className="host-n is-cores"
          title="Cores the kernel may schedule the thread on, where fewer than the machine has."
        >
          Pinned
        </span>
        <span className="is-spark">On CPU, each second</span>
        <span className="host-n is-now">Now</span>
        <span className="host-n is-wait" title="Runnable but waiting for a core: the worst second of the minute.">
          Waiting
        </span>
      </div>
      {rows.map((row) => (
        <ThreadLine key={row.other ? "other" : row.name} row={row} />
      ))}
      <div className="card-footnote">Fewer cores than the machine has means the thread is pinned.</div>
    </section>
  );
}

function ThreadLine({ row }: { row: ThreadRow }) {
  const onCpu = onCpuTone(row);
  return (
    <div
      className="host-thread"
      title={
        row.poh
          ? "Meant to hold a core. Toned below 90% on cpu."
          : undefined
      }
    >
      <span className="host-dev">
        <b>{row.label}</b>
        {/* Shown where their columns are not; a lone thread's 1 is left out. */}
        {(row.count > 1 || row.other) && (
          <span className="host-countline host-faint"> {count(row.count)}</span>
        )}
        {row.cores !== null && (
          <span className="host-pinline host-pin">, {pinnedLabel(row.cores)}</span>
        )}
        {row.poh && <s>holds a core</s>}
      </span>
      <span className="host-n is-count host-faint">{count(row.count)}</span>
      <span className={`host-n is-cores ${row.cores === null ? "host-faint" : "host-pin"}`}>
        {pinnedLabel(row.cores)}
      </span>
      <Spark row={row} />
      <span className={`host-n is-now${onCpu ? ` tone-${onCpu}` : ""}`}>
        {percent(row.now, row.now < 0.01 ? 1 : 0)}
      </span>
      <span className="host-n is-wait">{percent(row.waiting, 1)}</span>
    </div>
  );
}

const BAR_STEP = 6;
const BAR_WIDTH = 5;
const SPARK_HEIGHT = 16;

function Spark({ row }: { row: ThreadRow }) {
  const offset = THREADS_WINDOW - row.series.length;
  return (
    <svg
      className="host-spark"
      viewBox={`0 0 ${THREADS_WINDOW * BAR_STEP - 1} ${SPARK_HEIGHT}`}
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      {row.series.map((share, index) => {
        if (share === null) return null;
        const height = Math.max(1, Math.round(share * SPARK_HEIGHT));
        return (
          <rect
            key={index}
            x={(offset + index) * BAR_STEP}
            y={SPARK_HEIGHT - height}
            width={BAR_WIDTH}
            height={height}
            className={barClass(row, share, height)}
          />
        );
      })}
    </svg>
  );
}
