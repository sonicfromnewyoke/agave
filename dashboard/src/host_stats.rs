//! Host load, processor time, memory, filesystem capacity and disk saturation,
//! read from `/proc` and `statvfs` so the panel works on a node logging below
//! the default.

use {
    serde::Serialize,
    std::{
        collections::{BTreeMap, HashMap},
        io,
        os::unix::fs::MetadataExt,
        path::Path,
    },
};

/// Always 512, whatever the device's own sector size.
const SECTOR_BYTES: u64 = 512;

/// Each field's growth since `$before`; `None` where any went backwards.
macro_rules! checked_since {
    ($now:expr, $before:expr; $($field:ident)+) => {
        Some(Self {
            $($field: $now.$field.checked_sub($before.$field)?,)+
        })
    };
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
    pub running: u64,
    pub threads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Memory {
    pub total: u64,
    /// Not free: it includes most of the page cache.
    pub available: u64,
    /// What the kernel could take back beyond the free memory: `available - free`, so what is
    /// spoken for, `total - free - reclaimable`, is `total - available` as agave reports it. Page
    /// cache that holds tmpfs files, accounts on a RAM disk among them, is not reclaimable.
    pub reclaimable: u64,
    pub free: u64,
    pub swap_total: u64,
    pub swap_free: u64,
}

/// Guest time is already inside `user` and `nice`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CpuCounters {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    /// Not busy, but not free either.
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct CpuUse {
    pub busy: f64,
    pub user: f64,
    pub system: f64,
    pub iowait: f64,
    pub steal: f64,
}

impl CpuCounters {
    pub fn since(&self, previous: &Self) -> Option<Self> {
        checked_since!(self, previous; user nice system idle iowait irq softirq steal)
    }

    /// `None` where no time passed, since nought of nothing is not idle.
    pub fn shares(&self) -> Option<CpuUse> {
        let total = [
            self.user,
            self.nice,
            self.system,
            self.idle,
            self.iowait,
            self.irq,
            self.softirq,
            self.steal,
        ]
        .into_iter()
        .fold(0u64, u64::saturating_add);
        if total == 0 {
            return None;
        }
        let share = |part: u64| part as f64 / total as f64;
        Some(CpuUse {
            busy: 1.0 - share(self.idle.saturating_add(self.iowait)),
            user: share(self.user.saturating_add(self.nice)),
            system: share(
                self.system
                    .saturating_add(self.irq)
                    .saturating_add(self.softirq),
            ),
            iowait: share(self.iowait),
            steal: share(self.steal),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiskCounters {
    pub reads: u64,
    pub read_sectors: u64,
    pub read_ms: u64,
    pub writes: u64,
    pub write_sectors: u64,
    pub write_ms: u64,
    /// What `iostat` turns into `%util`: a duty cycle, not a level.
    pub busy_ms: u64,
}

impl DiskCounters {
    /// `None` if a counter went backwards, as when a device is re-added.
    pub fn since(&self, previous: &Self) -> Option<Self> {
        checked_since!(self, previous;
            reads read_sectors read_ms writes write_sectors write_ms busy_ms)
    }

    pub fn read_bytes(&self) -> u64 {
        self.read_sectors.saturating_mul(SECTOR_BYTES)
    }

    pub fn write_bytes(&self) -> u64 {
        self.write_sectors.saturating_mul(SECTOR_BYTES)
    }

    pub fn operations(&self) -> u64 {
        self.reads.saturating_add(self.writes)
    }

    /// `None` where the device did nothing, since nought would read as fast.
    pub fn wait_ms(&self) -> Option<f64> {
        let operations = self.operations();
        if operations == 0 {
            return None;
        }
        let waited = self.read_ms.saturating_add(self.write_ms);
        Some(waited as f64 / operations as f64)
    }

    /// Clamped because the kernel accumulates `busy_ms` on its own clock.
    pub fn busy(&self, interval_ms: f64) -> Option<f64> {
        if interval_ms <= 0.0 {
            return None;
        }
        Some((self.busy_ms as f64 / interval_ms).clamp(0.0, 1.0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Filesystem {
    pub total: u64,
    /// Root's reserve is deliberately not counted.
    pub available: u64,
    /// Free to root alone.
    pub reserved: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HostSnapshot {
    pub load: LoadAverage,
    pub cpu: Option<CpuCounters>,
    pub memory: Memory,
    pub disks: BTreeMap<String, DiskCounters>,
}

pub fn read() -> io::Result<HostSnapshot> {
    let load = parse_load(&std::fs::read_to_string("/proc/loadavg")?)
        .ok_or_else(|| invalid("unrecognised /proc/loadavg"))?;
    let cpu = std::fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|contents| parse_stat(&contents));
    let memory = parse_memory(&std::fs::read_to_string("/proc/meminfo")?)
        .ok_or_else(|| invalid("unrecognised /proc/meminfo"))?;
    let disks = parse_diskstats(&std::fs::read_to_string("/proc/diskstats")?);
    Ok(HostSnapshot {
        load,
        cpu,
        memory,
        disks,
    })
}

/// The first core's frequency governor; `None` where the kernel exposes no cpufreq, as on most
/// virtual machines.
pub fn cpu_governor() -> Option<String> {
    let governor =
        std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").ok()?;
    Some(governor.trim().to_string()).filter(|governor| !governor.is_empty())
}

pub fn filesystem(path: &Path) -> io::Result<Filesystem> {
    let stat = nix::sys::statvfs::statvfs(path)?;
    // `fragment_size` rather than `block_size`: the block counts are in fragments, and on
    // filesystems where the two differ `block_size` overstates the size.
    let block = stat.fragment_size() as u64;
    let available = stat.blocks_available() as u64;
    Ok(Filesystem {
        total: (stat.blocks() as u64).saturating_mul(block),
        available: available.saturating_mul(block),
        reserved: (stat.blocks_free() as u64)
            .saturating_sub(available)
            .saturating_mul(block),
    })
}

/// The process's resident memory, and how much of it is heap, mapped files and shared memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemory {
    pub resident: u64,
    /// `None` on kernels before 4.5, which do not split the figure.
    pub kinds: Option<ResidentKinds>,
}

/// The three add up to the resident figure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResidentKinds {
    pub anonymous: u64,
    pub file: u64,
    pub shared: u64,
}

/// The resident memory of the process `pid`.
pub fn process_memory_of(pid: u32) -> io::Result<ProcessMemory> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    parse_process_memory(&status).ok_or_else(|| invalid("status has no VmRSS line"))
}

/// The `Key:  1234 kB` lines of a `/proc` file, in bytes; a line with no number is left out.
fn kib_lines(contents: &str) -> HashMap<&str, u64> {
    contents
        .lines()
        .filter_map(|line| {
            let (key, rest) = line.split_once(':')?;
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            Some((key, kib.saturating_mul(1024)))
        })
        .collect()
}

fn parse_process_memory(status: &str) -> Option<ProcessMemory> {
    let lines = kib_lines(status);
    let bytes = |key: &str| lines.get(key).copied();
    let kinds = match (bytes("RssAnon"), bytes("RssFile"), bytes("RssShmem")) {
        (Some(anonymous), Some(file), Some(shared)) => Some(ResidentKinds {
            anonymous,
            file,
            shared,
        }),
        _ => None,
    };
    Some(ProcessMemory {
        resident: bytes("VmRSS")?,
        kinds,
    })
}

/// Four accounts directories under one mount are one filesystem, not four.
pub fn filesystem_id(path: &Path) -> io::Result<u64> {
    Ok(std::fs::metadata(path)?.dev())
}

/// A partition is folded up to its disk, since they share a queue.
#[cfg(target_os = "linux")]
pub fn device_for(path: &Path) -> io::Result<Option<String>> {
    let device = filesystem_id(path)?;
    let (major, minor) = (nix::sys::stat::major(device), nix::sys::stat::minor(device));
    if major == 0 {
        // Anonymous device: tmpfs, overlayfs and the like, with no block
        // device underneath.
        return Ok(None);
    }

    let Ok(resolved) = std::fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")) else {
        return Ok(None);
    };
    // A partition carries this file; a whole disk does not.
    let is_partition = resolved.join("partition").exists();
    let disk = if is_partition {
        resolved.parent().map(Path::to_path_buf)
    } else {
        Some(resolved)
    };
    Ok(disk
        .as_deref()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned()))
}

#[cfg(not(target_os = "linux"))]
pub fn device_for(_path: &Path) -> io::Result<Option<String>> {
    Err(io::ErrorKind::Unsupported.into())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn parse_load(contents: &str) -> Option<LoadAverage> {
    let mut fields = contents.split_whitespace();
    let one = fields.next()?.parse().ok()?;
    let five = fields.next()?.parse().ok()?;
    let fifteen = fields.next()?.parse().ok()?;
    let (running, threads) = match fields.next().and_then(|pair| pair.split_once('/')) {
        Some((running, threads)) => (
            running.parse().unwrap_or_default(),
            threads.parse().unwrap_or_default(),
        ),
        None => (0, 0),
    };
    Some(LoadAverage {
        one,
        five,
        fifteen,
        running,
        threads,
    })
}

/// `cpu  1234 5 678 90000 12 0 34 0 0 0`: user, nice, system, idle, then iowait, irq, softirq and
/// steal, which older kernels leave off.
fn parse_stat(contents: &str) -> Option<CpuCounters> {
    let line = contents.lines().find(|line| line.starts_with("cpu "))?;
    let fields: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(|value| value.parse().unwrap_or_default())
        .collect();
    if fields.len() < 4 {
        return None;
    }
    let at = |index: usize| fields.get(index).copied().unwrap_or_default();
    Some(CpuCounters {
        user: at(0),
        nice: at(1),
        system: at(2),
        idle: at(3),
        iowait: at(4),
        irq: at(5),
        softirq: at(6),
        steal: at(7),
    })
}

fn parse_memory(contents: &str) -> Option<Memory> {
    let lines = kib_lines(contents);
    let bytes = |key: &str| lines.get(key).copied().unwrap_or(0);
    let free = bytes("MemFree");
    Some(Memory {
        total: *lines.get("MemTotal")?,
        available: bytes("MemAvailable"),
        // Kernels before 3.14 give no estimate: the page cache stands for it.
        reclaimable: match lines.get("MemAvailable") {
            Some(available) => available.saturating_sub(free),
            None => bytes("Buffers").saturating_add(bytes("Cached")),
        },
        free,
        swap_total: bytes("SwapTotal"),
        swap_free: bytes("SwapFree"),
    })
}

/// `259 0 nvme0n1 12345 0 987654 3210 ...`: reads, merged, sectors, milliseconds, the same four for
/// writes, requests in flight, then milliseconds doing any I/O.
fn parse_diskstats(contents: &str) -> BTreeMap<String, DiskCounters> {
    let mut disks = BTreeMap::new();
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(name) = fields.get(2) else {
            continue;
        };
        let at = |index: usize| -> u64 {
            fields
                .get(index)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default()
        };
        // A line too short to carry the busy figure is from a kernel this
        // cannot read, and half a row is worse than none.
        if fields.len() < 13 {
            continue;
        }
        disks.insert(
            (*name).to_owned(),
            DiskCounters {
                reads: at(3),
                read_sectors: at(5),
                read_ms: at(6),
                writes: at(7),
                write_sectors: at(9),
                write_ms: at(10),
                busy_ms: at(12),
            },
        );
    }
    disks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reads_the_three_averages_and_the_thread_counts() {
        let load = parse_load("12.40 11.80 10.90 14/1847 3320145\n").unwrap();
        assert_eq!(load.one, 12.40);
        assert_eq!(load.five, 11.80);
        assert_eq!(load.fifteen, 10.90);
        assert_eq!(load.running, 14);
        assert_eq!(load.threads, 1847);
    }

    #[test]
    fn test_survives_a_loadavg_without_the_thread_pair() {
        let load = parse_load("0.10 0.20 0.30").unwrap();
        assert_eq!(load.running, 0);
        assert_eq!(load.threads, 0);
    }

    #[test]
    fn test_refuses_a_loadavg_it_cannot_read() {
        assert!(parse_load("").is_none());
        assert!(parse_load("not a number 1 2").is_none());
    }

    const MEMINFO: &str = "\
MemTotal:       402653184 kB
MemFree:         25165824 kB
MemAvailable:    92274688 kB
Buffers:          2097152 kB
Cached:          65011712 kB
SwapCached:             0 kB
SwapTotal:        8388608 kB
SwapFree:         8388608 kB
";

    const STAT: &str = "\
cpu  2400 10 600 20000 100 20 80 0 0 0
cpu0 1200 5 300 10000 50 10 40 0 0 0
cpu1 1200 5 300 10000 50 10 40 0 0 0
intr 12345 0 0
ctxt 6789
";

    #[test]
    fn test_reads_the_line_summing_every_core() {
        let cpu = parse_stat(STAT).unwrap();
        assert_eq!(cpu.user, 2400);
        assert_eq!(cpu.nice, 10);
        assert_eq!(cpu.system, 600);
        assert_eq!(cpu.idle, 20000);
        assert_eq!(cpu.iowait, 100);
        assert_eq!(cpu.irq, 20);
        assert_eq!(cpu.softirq, 80);
        assert_eq!(cpu.steal, 0);
    }

    #[test]
    fn test_survives_a_stat_from_before_steal_was_counted() {
        let cpu = parse_stat("cpu  10 0 20 70\n").unwrap();
        assert_eq!(cpu.idle, 70);
        assert_eq!(cpu.iowait, 0);
        assert_eq!(cpu.steal, 0);
    }

    #[test]
    fn test_refuses_a_stat_without_the_sum_line() {
        assert!(parse_stat("cpu0 10 0 20 70\n").is_none());
        assert!(parse_stat("cpu  10 0\n").is_none());
    }

    #[test]
    fn test_busy_leaves_out_idle_and_iowait_alone() {
        let delta = CpuCounters {
            user: 20,
            nice: 4,
            system: 5,
            idle: 60,
            iowait: 5,
            irq: 2,
            softirq: 3,
            steal: 1,
        };
        let shares = delta.shares().unwrap();
        assert!((shares.busy - 0.35).abs() < 1e-9);
        assert!((shares.user - 0.24).abs() < 1e-9);
        assert!((shares.system - 0.10).abs() < 1e-9);
        assert!((shares.iowait - 0.05).abs() < 1e-9);
        assert!((shares.steal - 0.01).abs() < 1e-9);
    }

    #[test]
    fn test_no_ticks_is_no_reading() {
        assert_eq!(CpuCounters::default().shares(), None);
    }

    #[test]
    fn test_discards_cpu_counters_that_went_backwards() {
        let previous = CpuCounters {
            user: 100,
            ..CpuCounters::default()
        };
        let current = CpuCounters {
            user: 5,
            ..CpuCounters::default()
        };
        assert!(current.since(&previous).is_none());
        assert_eq!(
            previous.since(&current),
            Some(CpuCounters {
                user: 95,
                ..CpuCounters::default()
            })
        );
    }

    #[test]
    fn test_reads_memory_in_bytes_and_takes_the_kernels_reclaimable_estimate() {
        let memory = parse_memory(MEMINFO).unwrap();
        assert_eq!(memory.total, 402_653_184 * 1024);
        assert_eq!(memory.available, 92_274_688 * 1024);
        assert_eq!(memory.free, 25_165_824 * 1024);
        assert_eq!(memory.reclaimable, (92_274_688 - 25_165_824) * 1024);
        assert_eq!(memory.swap_total, 8_388_608 * 1024);
        assert_eq!(memory.swap_free, 8_388_608 * 1024);
    }

    #[test]
    fn test_does_not_mistake_swap_cached_for_the_page_cache() {
        // `SwapCached` sits under `Cached`; a prefix match would add it to the reclaimable total.
        let memory =
            parse_memory("MemTotal: 1024 kB\nCached: 512 kB\nSwapCached: 256 kB\nBuffers: 0 kB\n")
                .unwrap();
        assert_eq!(memory.reclaimable, 512 * 1024);
    }

    #[test]
    fn test_reports_no_swap_where_none_is_configured() {
        let memory = parse_memory("MemTotal: 1024 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n").unwrap();
        assert_eq!(memory.swap_total, 0);
    }

    #[test]
    fn test_refuses_meminfo_without_a_total() {
        assert!(parse_memory("MemFree: 100 kB\n").is_none());
    }

    const DISKSTATS: &str = "\
 259       0 nvme0n1 1000 0 20000 300 4000 0 80000 900 0 1500 1200
 259       1 nvme0n1p1 900 0 18000 250 3800 0 76000 850 0 1400 1100
   8       0 sda 10 0 20 30 40 0 50 60 0 70 80
   7       0 loop0
";

    #[test]
    fn test_resident_memory_is_the_rss_lines_in_bytes() {
        let status = "\
Name:\tagave-validator
VmPeak:\t  900000 kB
VmRSS:\t  524288 kB
RssAnon:\t  500000 kB
RssFile:\t   24000 kB
RssShmem:\t     288 kB
Threads:\t48
";
        assert_eq!(
            parse_process_memory(status),
            Some(ProcessMemory {
                resident: 524_288 * 1024,
                kinds: Some(ResidentKinds {
                    anonymous: 500_000 * 1024,
                    file: 24_000 * 1024,
                    shared: 288 * 1024,
                }),
            })
        );
        assert_eq!(
            parse_process_memory("VmRSS:\t  1 kB\n"),
            Some(ProcessMemory {
                resident: 1024,
                kinds: None,
            })
        );
        assert_eq!(parse_process_memory("Name:\tx\n"), None);
    }

    #[test]
    fn test_reads_the_counters_a_device_line_carries() {
        let disks = parse_diskstats(DISKSTATS);
        let disk = disks.get("nvme0n1").unwrap();
        assert_eq!(disk.reads, 1000);
        assert_eq!(disk.read_sectors, 20000);
        assert_eq!(disk.read_ms, 300);
        assert_eq!(disk.writes, 4000);
        assert_eq!(disk.write_sectors, 80000);
        assert_eq!(disk.write_ms, 900);
        assert_eq!(disk.busy_ms, 1500);
    }

    #[test]
    fn test_keeps_partitions_separate_here_and_folds_them_later() {
        let disks = parse_diskstats(DISKSTATS);
        assert!(disks.contains_key("nvme0n1"));
        assert!(disks.contains_key("nvme0n1p1"));
    }

    #[test]
    fn test_skips_a_line_too_short_to_carry_the_busy_figure() {
        assert!(!parse_diskstats(DISKSTATS).contains_key("loop0"));
    }

    #[test]
    fn test_turns_sectors_into_bytes_at_five_hundred_and_twelve() {
        let disk = DiskCounters {
            read_sectors: 2,
            write_sectors: 4,
            ..DiskCounters::default()
        };
        assert_eq!(disk.read_bytes(), 1024);
        assert_eq!(disk.write_bytes(), 2048);
    }

    #[test]
    fn test_subtracts_the_previous_sample() {
        let previous = DiskCounters {
            reads: 10,
            busy_ms: 100,
            ..DiskCounters::default()
        };
        let current = DiskCounters {
            reads: 25,
            busy_ms: 340,
            ..DiskCounters::default()
        };
        let delta = current.since(&previous).unwrap();
        assert_eq!(delta.reads, 15);
        assert_eq!(delta.busy_ms, 240);
        // A device removed and re-added restarts at nought, and an unsigned wrap would read as an
        // enormous burst of work.
        assert!(previous.since(&current).is_none());
    }

    #[test]
    fn test_works_out_the_mean_wait_across_reads_and_writes() {
        let delta = DiskCounters {
            reads: 30,
            writes: 70,
            read_ms: 6,
            write_ms: 14,
            ..DiskCounters::default()
        };
        assert_eq!(delta.wait_ms(), Some(0.2));
    }

    #[test]
    fn test_has_no_wait_to_report_where_the_device_did_nothing() {
        assert_eq!(DiskCounters::default().wait_ms(), None);
    }

    #[test]
    fn test_reads_busy_as_a_share_of_the_interval() {
        let delta = DiskCounters {
            busy_ms: 340,
            ..DiskCounters::default()
        };
        assert_eq!(delta.busy(1000.0), Some(0.34));
    }

    #[test]
    fn test_busy_is_clamped_to_the_interval() {
        let delta = DiskCounters {
            busy_ms: 1004,
            ..DiskCounters::default()
        };
        assert_eq!(delta.busy(1000.0), Some(1.0));
    }
}
