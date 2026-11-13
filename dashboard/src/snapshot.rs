//! The newest snapshot archives on disk, read as `getHighestSnapshotSlot`
//! reads them, and how often new ones are due.

use {
    crate::certs::Span,
    agave_snapshots::{
        SnapshotInterval, paths, snapshot_archive_info::SnapshotArchiveInfoGetter,
        snapshot_config::SnapshotConfig,
    },
    serde::Serialize,
    solana_clock::Slot,
    std::{
        collections::{BTreeMap, HashSet, VecDeque},
        fs,
        path::Path,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/// A staging file older than this is a leftover from a crash, not a write.
const STALE_WRITE: Duration = Duration::from_secs(60);

/// Ended writes kept. The walk reads slots near the root, and 64 default incrementals span over
/// an hour.
const SPANS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Archive {
    pub slot: Slot,
    pub written_millis: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Writing {
    pub slot: Slot,
    pub since_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Written {
    pub slot: Slot,
    pub took_millis: u64,
    pub fell_behind_slots: u64,
}

/// A `None` interval is disabled.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Snapshots {
    pub full: Option<Archive>,
    pub incremental: Option<Archive>,
    pub full_interval: Option<u64>,
    pub incremental_interval: Option<u64>,
    /// Kind unknown: both kinds stage under the same name.
    pub writing: Option<Writing>,
    pub last_written: Option<Written>,
}

/// Follows each write from its staging file to its end, and the slots each one spanned.
#[derive(Debug, Default)]
pub struct SnapshotTracker {
    /// The write in progress, the most slots the node fell behind during it, and where it began.
    writing: Option<(Writing, u64, Slot)>,
    last: Option<Written>,
    spans: VecDeque<Span>,
}

impl SnapshotTracker {
    /// Takes what the archive directories show now and returns it with the last write filled in.
    pub fn observe(
        &mut self,
        read: Option<Snapshots>,
        completed: Slot,
        now_millis: u64,
    ) -> Option<Snapshots> {
        let writing = read.as_ref().and_then(|snapshots| snapshots.writing);
        match (writing, self.writing.take()) {
            (Some(writing), Some((held, fell_behind, from))) if held.slot == writing.slot => {
                self.writing = Some((writing, fell_behind, from));
            }
            (writing, held) => {
                if let Some((ended, fell_behind_slots, from)) = held {
                    // Ended when the next write began, or by now.
                    let ended_millis =
                        writing.map_or(now_millis, |next| next.since_millis.min(now_millis));
                    self.last = Some(Written {
                        slot: ended.slot,
                        took_millis: ended_millis.saturating_sub(ended.since_millis),
                        fell_behind_slots,
                    });
                    if self.spans.len() >= SPANS {
                        self.spans.pop_front();
                    }
                    self.spans.push_back(Span {
                        from,
                        to: Some(completed),
                    });
                }
                self.writing = writing.map(|writing| (writing, 0, completed));
            }
        }
        read.map(|snapshots| Snapshots {
            last_written: self.last,
            ..snapshots
        })
    }

    pub fn note_behind(&mut self, slots: u64) {
        if let Some((_, fell_behind, _)) = &mut self.writing {
            *fell_behind = (*fell_behind).max(slots);
        }
    }

    /// Finished writes, then the one in progress with no end.
    pub fn spans(&self) -> impl Iterator<Item = Span> + '_ {
        self.spans
            .iter()
            .copied()
            .chain(self.writing.map(|(_, _, from)| Span { from, to: None }))
    }
}

pub fn read(config: &SnapshotConfig) -> Option<Snapshots> {
    if !config.should_generate_snapshots() {
        return None;
    }
    let full = paths::get_highest_full_snapshot_archive_info(&config.full_snapshot_archives_dir);
    let incremental = full.as_ref().and_then(|full| {
        paths::get_highest_incremental_snapshot_archive_info(
            &config.incremental_snapshot_archives_dir,
            full.slot(),
        )
    });
    Some(Snapshots {
        full: full.as_ref().map(archive),
        incremental: incremental.as_ref().map(archive),
        full_interval: slots_of(config.full_snapshot_archive_interval),
        incremental_interval: slots_of(config.incremental_snapshot_archive_interval),
        writing: writing_in(&[
            &config.full_snapshot_archives_dir,
            &config.incremental_snapshot_archives_dir,
        ]),
        last_written: None,
    })
}

/// Size of the archives on disk; staging files are not archives yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveBytes {
    pub full: u64,
    pub incremental: u64,
}

pub fn archive_bytes(config: &SnapshotConfig) -> Option<ArchiveBytes> {
    if !config.should_generate_snapshots() {
        return None;
    }
    Some(ArchiveBytes {
        full: sizes(paths::full_snapshot_archives_iter(
            &config.full_snapshot_archives_dir,
        )),
        incremental: sizes(paths::incremental_snapshot_archives_iter(
            &config.incremental_snapshot_archives_dir,
        )),
    })
}

fn sizes<T: SnapshotArchiveInfoGetter>(archives: impl Iterator<Item = T>) -> u64 {
    archives
        .filter_map(|info| fs::metadata(info.path()).ok())
        .map(|meta| meta.len())
        .fold(0, u64::saturating_add)
}

fn archive(info: &impl SnapshotArchiveInfoGetter) -> Archive {
    Archive {
        slot: info.slot(),
        written_millis: fs::metadata(info.path())
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(millis),
    }
}

fn millis(time: SystemTime) -> Option<u64> {
    u64::try_from(time.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
}

/// Its directory dates the start, and a file untouched for a minute is a leftover.
fn writing_in(dirs: &[&Path]) -> Option<Writing> {
    let now = SystemTime::now();
    let mut seen: HashSet<&Path> = HashSet::new();
    let mut staged: BTreeMap<Slot, (bool, SystemTime)> = BTreeMap::new();
    for &dir in dirs {
        if !seen.insert(dir) {
            continue;
        }
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(rest) = name
                .to_str()
                .and_then(|name| name.strip_prefix(paths::TMP_SNAPSHOT_ARCHIVE_PREFIX))
            else {
                continue;
            };
            let Some(slot) = rest
                .split(['-', '.'])
                .next()
                .and_then(|slot| slot.parse().ok())
            else {
                continue;
            };
            let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
                continue;
            };
            let fresh = entry.file_type().is_ok_and(|kind| kind.is_file())
                && now.duration_since(modified).unwrap_or_default() <= STALE_WRITE;
            let (writing, since) = staged.entry(slot).or_insert((false, modified));
            *writing |= fresh;
            *since = (*since).min(modified);
        }
    }
    staged
        .into_iter()
        .filter(|(_, (writing, _))| *writing)
        .filter_map(|(slot, (_, since))| {
            Some(Writing {
                slot,
                since_millis: millis(since)?,
            })
        })
        .next_back()
}

fn slots_of(interval: SnapshotInterval) -> Option<u64> {
    match interval {
        SnapshotInterval::Slots(slots) => Some(slots.get()),
        SnapshotInterval::Disabled => None,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_snapshots::snapshot_config::SnapshotUsage,
        std::{fs::File, num::NonZeroU64},
        tempfile::TempDir,
    };

    const HASH: &str = "11111111111111111111111111111111";

    fn staged(writing: Option<Writing>) -> Option<Snapshots> {
        Some(Snapshots {
            writing,
            ..Snapshots::default()
        })
    }

    fn written(slot: Slot, took_millis: u64, fell_behind_slots: u64) -> Option<Written> {
        Some(Written {
            slot,
            took_millis,
            fell_behind_slots,
        })
    }

    const WRITE: Writing = Writing {
        slot: 300,
        since_millis: 1_000,
    };

    #[test]
    fn test_a_write_is_timed_from_its_staging_to_its_end() {
        let mut tracker = SnapshotTracker::default();
        let shown = tracker.observe(staged(Some(WRITE)), 10, 1_500).unwrap();
        assert_eq!(shown.last_written, None);
        assert_eq!(
            tracker.spans().collect::<Vec<_>>(),
            [Span { from: 10, to: None }]
        );

        tracker.note_behind(7);
        tracker.note_behind(3);
        tracker.observe(staged(Some(WRITE)), 40, 2_000);
        let shown = tracker.observe(staged(None), 90, 91_000).unwrap();
        assert_eq!(shown.last_written, written(300, 90_000, 7));
        assert_eq!(
            tracker.spans().collect::<Vec<_>>(),
            [Span {
                from: 10,
                to: Some(90),
            }]
        );
    }

    #[test]
    fn test_lag_outside_a_write_is_not_counted() {
        let mut tracker = SnapshotTracker::default();
        tracker.note_behind(50);
        tracker.observe(staged(Some(WRITE)), 10, 1_500);
        let shown = tracker.observe(staged(None), 20, 2_000).unwrap();
        assert_eq!(shown.last_written.unwrap().fell_behind_slots, 0);
    }

    #[test]
    fn test_a_new_staging_slot_ends_the_write_before_it() {
        let mut tracker = SnapshotTracker::default();
        tracker.observe(staged(Some(WRITE)), 10, 1_500);
        tracker.note_behind(5);
        let next = Writing {
            slot: 500,
            since_millis: 9_000,
        };
        let shown = tracker.observe(staged(Some(next)), 60, 10_000).unwrap();
        assert_eq!(shown.last_written, written(300, 8_000, 5));

        let shown = tracker.observe(staged(None), 70, 12_000).unwrap();
        assert_eq!(shown.last_written, written(500, 3_000, 0));
        assert_eq!(
            tracker.spans().collect::<Vec<_>>(),
            [
                Span {
                    from: 10,
                    to: Some(60),
                },
                Span {
                    from: 60,
                    to: Some(70),
                },
            ]
        );
    }

    #[test]
    fn test_only_the_newest_spans_are_kept() {
        let mut tracker = SnapshotTracker::default();
        for from in (0..).step_by(2).take(SPANS.saturating_add(3)) {
            tracker.observe(staged(Some(WRITE)), from, 0);
            tracker.observe(staged(None), from.saturating_add(1), 0);
        }
        let spans = tracker.spans().collect::<Vec<_>>();
        assert_eq!(spans.len(), SPANS);
        assert_eq!(
            spans.first(),
            Some(&Span {
                from: 6,
                to: Some(7),
            })
        );
    }

    #[test]
    fn test_no_snapshot_config_publishes_nothing() {
        let mut tracker = SnapshotTracker::default();
        assert_eq!(tracker.observe(None, 10, 0), None);
    }

    fn config(dir: &TempDir, usage: SnapshotUsage) -> SnapshotConfig {
        SnapshotConfig {
            usage,
            full_snapshot_archives_dir: dir.path().to_path_buf(),
            incremental_snapshot_archives_dir: dir.path().to_path_buf(),
            full_snapshot_archive_interval: SnapshotInterval::Slots(
                NonZeroU64::new(25_000).unwrap(),
            ),
            incremental_snapshot_archive_interval: SnapshotInterval::Disabled,
            ..SnapshotConfig::default()
        }
    }

    fn touch(dir: &TempDir, name: &str) {
        File::create(dir.path().join(name)).unwrap();
    }

    #[test]
    fn test_archive_bytes_add_up_each_kind_and_skip_what_is_not_an_archive() {
        let dir = TempDir::new().unwrap();
        for (name, size) in [
            (format!("snapshot-100-{HASH}.tar.zst"), 1_000),
            (format!("snapshot-200-{HASH}.tar.zst"), 2_000),
            (format!("incremental-snapshot-200-250-{HASH}.tar.zst"), 30),
            ("tmp-snapshot-archive-300".to_owned(), 7),
        ] {
            File::create(dir.path().join(name))
                .unwrap()
                .set_len(size)
                .unwrap();
        }
        assert_eq!(
            archive_bytes(&config(&dir, SnapshotUsage::LoadAndGenerate)),
            Some(ArchiveBytes {
                full: 3_000,
                incremental: 30,
            })
        );
    }

    #[test]
    fn test_the_newest_full_and_the_incremental_on_it_are_read() {
        let dir = TempDir::new().unwrap();
        touch(&dir, &format!("snapshot-100-{HASH}.tar.zst"));
        touch(&dir, &format!("snapshot-200-{HASH}.tar.zst"));
        touch(
            &dir,
            &format!("incremental-snapshot-100-180-{HASH}.tar.zst"),
        );
        touch(
            &dir,
            &format!("incremental-snapshot-200-260-{HASH}.tar.zst"),
        );

        let got = read(&config(&dir, SnapshotUsage::LoadAndGenerate)).unwrap();
        assert_eq!(got.full.as_ref().map(|archive| archive.slot), Some(200));
        assert_eq!(
            got.incremental.as_ref().map(|archive| archive.slot),
            Some(260)
        );
        assert!(got.full.unwrap().written_millis.is_some());
        assert_eq!(got.full_interval, Some(25_000));
        assert_eq!(got.incremental_interval, None);
    }

    #[test]
    fn test_an_empty_directory_reads_as_no_archives_rather_than_nothing() {
        let dir = TempDir::new().unwrap();
        let got = read(&config(&dir, SnapshotUsage::LoadAndGenerate)).unwrap();
        assert_eq!(got.full, None);
        assert_eq!(got.incremental, None);
    }

    #[test]
    fn test_an_archive_being_staged_is_reported_with_its_start() {
        let dir = TempDir::new().unwrap();
        touch(&dir, &format!("snapshot-100-{HASH}.tar.zst"));
        fs::create_dir(dir.path().join("tmp-snapshot-archive-300-abcd")).unwrap();
        touch(&dir, "tmp-snapshot-archive-300.tar.zst");

        let got = read(&config(&dir, SnapshotUsage::LoadAndGenerate)).unwrap();
        let writing = got.writing.expect("a fresh staging file is a write");
        assert_eq!(writing.slot, 300);
        assert!(writing.since_millis > 0);
        assert_eq!(got.last_written, None);
    }

    #[test]
    fn test_a_stale_staging_file_is_a_leftover_not_a_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("tmp-snapshot-archive-300.tar.zst");
        let file = File::create(&path).unwrap();
        let stale = SystemTime::now()
            .checked_sub(Duration::from_secs(600))
            .unwrap();
        file.set_modified(stale).unwrap();

        let got = read(&config(&dir, SnapshotUsage::LoadAndGenerate)).unwrap();
        assert_eq!(got.writing, None);
    }

    #[test]
    fn test_a_staging_directory_alone_is_not_a_write() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join("tmp-snapshot-archive-300-abcd")).unwrap();
        let got = read(&config(&dir, SnapshotUsage::LoadAndGenerate)).unwrap();
        assert_eq!(got.writing, None);
    }

    #[test]
    fn test_a_node_that_generates_none_reads_as_nothing() {
        let dir = TempDir::new().unwrap();
        touch(&dir, &format!("snapshot-100-{HASH}.tar.zst"));
        let load_only = config(&dir, SnapshotUsage::LoadOnly);
        assert_eq!(read(&load_only), None);
        assert_eq!(archive_bytes(&load_only), None);
    }
}
