//! Where each of the validator's threads spent the last second, from its
//! process's `task` directory under `/proc`.

use {
    serde::Serialize,
    std::{
        cmp::Ordering,
        collections::{BTreeSet, HashMap},
        io::{self, Read as _},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadReading {
    pub tid: u64,
    /// Fifteen bytes, as the kernel keeps it.
    pub name: String,
    pub on_cpu_nanos: u64,
    pub waiting_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadGroup {
    pub name: String,
    pub count: usize,
    /// The union where every thread is held to fewer cores than the machine has; `None` where any
    /// may roam.
    pub cores: Option<String>,
    pub on_cpu: f64,
    pub waiting: f64,
    pub other: bool,
}

/// The threads of the process `pid`. A thread in `known` keeps the name it has there; only the
/// others have `comm` read.
pub fn read_of(
    pid: u32,
    known: Option<&HashMap<u64, ThreadReading>>,
) -> io::Result<Vec<ThreadReading>> {
    let mut threads = Vec::new();
    let mut buf = [0u8; 128];
    for entry in std::fs::read_dir(format!("/proc/{pid}/task"))?.flatten() {
        let Some(tid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
        else {
            continue;
        };
        let dir = entry.path();
        // A thread can exit between the listing and the read.
        let Some((on_cpu_nanos, waiting_nanos)) =
            read_small(&dir.join("schedstat"), &mut buf).and_then(parse_schedstat)
        else {
            continue;
        };
        let Some(name) = known
            .and_then(|known| known.get(&tid))
            .map(|before| before.name.clone())
            .or_else(|| {
                read_small(&dir.join("comm"), &mut buf).map(|name| name.trim().to_string())
            })
        else {
            continue;
        };
        threads.push(ThreadReading {
            tid,
            name,
            on_cpu_nanos,
            waiting_nanos,
        });
    }
    Ok(threads)
}

/// One open and one read: both files are a few dozen bytes.
fn read_small<'a>(path: &std::path::Path, buf: &'a mut [u8]) -> Option<&'a str> {
    let len = std::fs::File::open(path).ok()?.read(buf).ok()?;
    std::str::from_utf8(buf.get(..len)?).ok()
}

/// The cores the thread `tid` may run on, of whichever process; a process's own are its main
/// thread's, whose id is the process's.
#[cfg(target_os = "linux")]
pub fn cores_allowed(tid: u64) -> Option<BTreeSet<usize>> {
    let cpus = agave_cpu_utils::cpu_affinity(Some(i32::try_from(tid).ok()?)).ok()?;
    Some(cpus.iter().map(|cpu| **cpu).collect())
}

#[cfg(not(target_os = "linux"))]
pub fn cores_allowed(_tid: u64) -> Option<BTreeSet<usize>> {
    None
}

/// `12345678 2345 67`: nanoseconds on a cpu, nanoseconds runnable and waiting
/// for one, and timeslices.
fn parse_schedstat(contents: &str) -> Option<(u64, u64)> {
    let mut fields = contents.split_whitespace();
    let on_cpu = fields.next()?.parse().ok()?;
    let waiting = fields.next()?.parse().ok()?;
    Some((on_cpu, waiting))
}

/// Cores back into the kernel's form, runs folded: `{2, 4, 5, 6, 7}` is `2,4-7`.
fn format_cores(cores: &BTreeSet<usize>) -> String {
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for &core in cores {
        match runs.last_mut() {
            Some((_, end)) if end.saturating_add(1) == core => *end = core,
            _ => runs.push((core, core)),
        }
    }
    runs.iter()
        .map(|(from, to)| {
            if from == to {
                from.to_string()
            } else {
                format!("{from}-{to}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub fn group_key(name: &str) -> &str {
    name.trim_end_matches(|c: char| c.is_ascii_digit())
}

#[derive(Default)]
struct GroupSum {
    count: usize,
    on_cpu_nanos: u64,
    waiting_nanos: u64,
    /// `None` until the first thread; then the union of every thread's pinning
    /// so far, or `Some(None)` once one of them is not pinned.
    cores: Option<Option<BTreeSet<usize>>>,
}

/// A thread with no earlier reading, or whose id was reused under another name, is left out.
pub fn group_shares(
    previous: &HashMap<u64, ThreadReading>,
    current: &[ThreadReading],
    pinning: &HashMap<u64, Option<BTreeSet<usize>>>,
    interval_nanos: u64,
) -> Vec<ThreadGroup> {
    let mut groups: HashMap<&str, GroupSum> = HashMap::new();
    for thread in current {
        let Some(before) = previous.get(&thread.tid) else {
            continue;
        };
        if before.name != thread.name {
            continue;
        }
        let sum = groups.entry(group_key(&thread.name)).or_default();
        sum.count = sum.count.saturating_add(1);
        sum.on_cpu_nanos = sum
            .on_cpu_nanos
            .saturating_add(thread.on_cpu_nanos.saturating_sub(before.on_cpu_nanos));
        sum.waiting_nanos = sum
            .waiting_nanos
            .saturating_add(thread.waiting_nanos.saturating_sub(before.waiting_nanos));
        let allowed = pinning.get(&thread.tid).cloned().flatten();
        sum.cores = match (sum.cores.take(), allowed) {
            (None, allowed) => Some(allowed),
            (Some(Some(mut held)), Some(allowed)) => {
                held.extend(allowed);
                Some(Some(held))
            }
            _ => Some(None),
        };
    }

    let span = interval_nanos as f64;
    groups
        .into_iter()
        .filter(|(_, sum)| sum.count > 0)
        .map(|(name, sum)| ThreadGroup {
            name: name.to_string(),
            count: sum.count,
            cores: sum.cores.flatten().as_ref().map(format_cores),
            on_cpu: share(sum.on_cpu_nanos, sum.count, span),
            waiting: share(sum.waiting_nanos, sum.count, span),
            other: false,
        })
        .collect()
}

/// Clamped: the two clocks are read apart, so a busy thread can read a hair over.
fn share(nanos: u64, count: usize, span: f64) -> f64 {
    if count == 0 || span <= 0.0 {
        return 0.0;
    }
    (nanos as f64 / (count as f64 * span)).min(1.0)
}

/// Ranked on the window so rows do not reorder every tick.
pub fn select_rows(
    mut groups: Vec<ThreadGroup>,
    means: &HashMap<&str, f64>,
    top: usize,
) -> Vec<ThreadGroup> {
    let rank = |group: &ThreadGroup| {
        means
            .get(group.name.as_str())
            .copied()
            .unwrap_or(group.on_cpu)
    };
    groups.sort_by(|a, b| {
        rank(b)
            .partial_cmp(&rank(a))
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    let rest = groups.split_off(top.min(groups.len()));
    if !rest.is_empty() {
        let count = rest
            .iter()
            .map(|group| group.count)
            .fold(0, usize::saturating_add);
        groups.push(ThreadGroup {
            name: String::new(),
            count,
            cores: None,
            on_cpu: mean_of(&rest, count, |group| group.on_cpu),
            waiting: mean_of(&rest, count, |group| group.waiting),
            other: true,
        });
    }
    groups
}

/// Weighted by thread count, so the folded row is still a mean per thread.
fn mean_of(groups: &[ThreadGroup], count: usize, value: impl Fn(&ThreadGroup) -> f64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    let weighted: f64 = groups
        .iter()
        .map(|group| value(group) * group.count as f64)
        .sum();
    weighted / count as f64
}

pub fn window_means<'a>(
    recent: impl IntoIterator<Item = &'a Vec<(String, f64)>>,
) -> HashMap<&'a str, f64> {
    let mut sums: HashMap<&str, (f64, usize)> = HashMap::new();
    for tick in recent {
        for (name, share) in tick {
            let entry = sums.entry(name.as_str()).or_insert((0.0, 0));
            entry.0 += share;
            entry.1 = entry.1.saturating_add(1);
        }
    }
    sums.into_iter()
        .map(|(name, (sum, n))| (name, if n == 0 { 0.0 } else { sum / n as f64 }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(tid: u64, name: &str, on_cpu: u64, waiting: u64) -> ThreadReading {
        ThreadReading {
            tid,
            name: name.to_string(),
            on_cpu_nanos: on_cpu,
            waiting_nanos: waiting,
        }
    }

    fn by_tid(threads: &[ThreadReading]) -> HashMap<u64, ThreadReading> {
        threads
            .iter()
            .cloned()
            .map(|thread| (thread.tid, thread))
            .collect()
    }

    const SECOND: u64 = 1_000_000_000;

    #[test]
    fn test_schedstat_is_two_clocks_and_a_count() {
        assert_eq!(
            parse_schedstat("2894374 20482 143\n"),
            Some((2_894_374, 20_482))
        );
        assert_eq!(parse_schedstat("garbage"), None);
        assert_eq!(parse_schedstat(""), None);
    }

    fn cores(list: &[usize]) -> BTreeSet<usize> {
        list.iter().copied().collect()
    }

    #[test]
    fn test_a_core_list_is_folded_into_runs() {
        assert_eq!(format_cores(&cores(&[2, 4, 5, 6, 7])), "2,4-7");
        assert_eq!(format_cores(&cores(&[3])), "3");
        assert_eq!(format_cores(&(0..=23).collect()), "0-23");
        assert_eq!(format_cores(&cores(&[7, 5, 4, 6])), "4-7");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_this_process_may_run_on_some_core() {
        let main = u64::from(std::process::id());
        assert!(cores_allowed(main).is_some_and(|cores| !cores.is_empty()));
    }

    #[test]
    fn test_a_pool_is_named_without_its_trailing_number() {
        assert_eq!(group_key("solScHandleV07"), "solScHandleV");
        assert_eq!(group_key("solPohTickProd"), "solPohTickProd");
        assert_eq!(group_key("solSigVerify3"), "solSigVerify");
    }

    #[test]
    fn test_a_group_is_the_mean_of_its_threads_over_the_second() {
        // Two handlers, one on a core for half the second and one for a
        // quarter: the pool reads three eighths.
        let before = by_tid(&[
            reading(10, "solScHandleV00", 0, 0),
            reading(11, "solScHandleV01", SECOND, 0),
        ]);
        let now = [
            reading(10, "solScHandleV00", SECOND / 2, 10_000_000),
            reading(11, "solScHandleV01", SECOND + SECOND / 4, 0),
        ];
        let groups = group_shares(&before, &now, &HashMap::new(), SECOND);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "solScHandleV");
        assert_eq!(groups[0].count, 2);
        assert!((groups[0].on_cpu - 0.375).abs() < 1e-9);
        assert!((groups[0].waiting - 0.005).abs() < 1e-9);
    }

    #[test]
    fn test_a_thread_with_no_earlier_reading_is_left_out() {
        let before = by_tid(&[reading(10, "solGossip", 0, 0)]);
        let now = [
            reading(10, "solGossip", SECOND / 10, 0),
            reading(99, "solNewThread", SECOND, 0),
        ];
        let groups = group_shares(&before, &now, &HashMap::new(), SECOND);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "solGossip");
    }

    #[test]
    fn test_reused_thread_id_is_not_differenced() {
        let before = by_tid(&[reading(10, "solOldThread", SECOND, 0)]);
        let now = [reading(10, "solNewThread", SECOND / 10, 0)];
        assert!(group_shares(&before, &now, &HashMap::new(), SECOND).is_empty());
    }

    #[test]
    fn test_a_share_cannot_exceed_the_second() {
        let before = by_tid(&[reading(1, "solPohTickProd", 0, 0)]);
        let now = [reading(1, "solPohTickProd", SECOND + 1_000, 0)];
        let groups = group_shares(&before, &now, &HashMap::new(), SECOND);
        assert_eq!(groups[0].on_cpu, 1.0);
    }

    #[test]
    fn test_pinning_needs_the_whole_group() {
        let at = |on_cpu| {
            [
                reading(1, "solPohTickProd", on_cpu, 0),
                reading(10, "solScHandleV00", on_cpu, 0),
                reading(11, "solScHandleV01", on_cpu, 0),
            ]
        };
        let pinning = HashMap::from([
            (1, Some(cores(&[3]))),
            (10, Some((12..=22).collect())),
            (11, None),
        ]);
        let mut groups = group_shares(&by_tid(&at(0)), &at(1), &pinning, SECOND);
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(groups[0].name, "solPohTickProd");
        assert_eq!(groups[0].cores.as_deref(), Some("3"));
        assert_eq!(groups[1].name, "solScHandleV");
        assert_eq!(
            groups[1].cores, None,
            "one pinned and one not is not a pinned pool"
        );
    }

    #[test]
    fn test_a_pool_pinned_one_thread_to_a_core_reports_the_union() {
        let at = |on_cpu| -> Vec<ThreadReading> {
            (10..14)
                .map(|tid| reading(tid, &format!("solTransmIO{tid}"), on_cpu, 0))
                .collect()
        };
        let pinning: HashMap<u64, _> = (10..)
            .zip(4..8)
            .map(|(tid, core)| (tid, Some(cores(&[core]))))
            .collect();
        let groups = group_shares(&by_tid(&at(0)), &at(1), &pinning, SECOND);
        assert_eq!(groups[0].cores.as_deref(), Some("4-7"));
    }

    fn group(name: &str, count: usize, on_cpu: f64) -> ThreadGroup {
        ThreadGroup {
            name: name.to_string(),
            count,
            cores: None,
            on_cpu,
            waiting: 0.0,
            other: false,
        }
    }

    #[test]
    fn test_no_folded_row_where_every_group_is_shown() {
        let rows = select_rows(
            vec![group("a", 1, 0.5), group("b", 1, 0.2)],
            &HashMap::new(),
            8,
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| !row.other));
    }

    #[test]
    fn test_a_window_mean_covers_only_the_ticks_a_group_appeared_in() {
        let recent = vec![
            vec![("a".to_string(), 0.5), ("b".to_string(), 0.1)],
            vec![("a".to_string(), 0.7)],
        ];
        let means = window_means(&recent);
        assert!((means["a"] - 0.6).abs() < 1e-9);
        assert!((means["b"] - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_ranked_rows_serialise_as_published() {
        let recent = vec![
            vec![
                ("solPohTickProd".to_string(), 1.0),
                ("solGossip".to_string(), 0.5),
            ],
            vec![
                ("solGossip".to_string(), 0.25),
                ("solScHandleV".to_string(), 0.125),
            ],
        ];
        // Gossip is quiet this second but busy across the window, so it keeps its row above the
        // handler that just spiked.
        let groups = vec![
            group("solPohTickProd", 1, 0.75),
            group("solGossip", 6, 0.0625),
            group("solScHandleV", 11, 0.25),
            group("solRepairSvc", 1, 0.0625),
        ];
        let rows = select_rows(groups, &window_means(&recent), 2);
        // The rest fold into a mean per thread: (0.25 * 11 + 0.0625) / 12.
        assert_eq!(
            serde_json::to_string(&rows).unwrap(),
            r#"[{"name":"solPohTickProd","count":1,"cores":null,"on_cpu":0.75,"waiting":0.0,"other":false},{"name":"solGossip","count":6,"cores":null,"on_cpu":0.0625,"waiting":0.0,"other":false},{"name":"","count":12,"cores":null,"on_cpu":0.234375,"waiting":0.0,"other":true}]"#
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_this_process_reports_its_own_threads() {
        let threads = read_of(std::process::id(), None).unwrap();
        assert!(!threads.is_empty());
        assert!(threads.iter().all(|thread| !thread.name.is_empty()));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_only_a_thread_not_already_known_has_its_name_read() {
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let helper = std::thread::spawn(move || {
            let _ = wait.recv();
        });
        let main = u64::from(std::process::id());
        let mut known = by_tid(&read_of(std::process::id(), None).unwrap());
        for reading in known.values_mut() {
            reading.name = "carried".to_string();
        }
        known.remove(&main);
        let threads = read_of(std::process::id(), Some(&known)).unwrap();
        drop(release);
        helper.join().unwrap();

        let name = |tid| {
            threads
                .iter()
                .find(|thread| thread.tid == tid)
                .map(|thread| thread.name.as_str())
        };
        assert!(name(main).is_some_and(|name| name != "carried"));
        assert!(
            threads
                .iter()
                .any(|thread| thread.tid != main && thread.name == "carried")
        );
    }
}
