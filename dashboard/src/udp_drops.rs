//! Per-socket UDP receive counters from `/proc/net/udp`, cumulative and keyed
//! by port. `sk_drops` is what the kernel discarded before the validator could
//! read it; [`crate::metrics_tap`] supplies the delivered half.

use std::{
    collections::{HashMap, VecDeque},
    io,
    time::{Duration, Instant},
};

/// Summed over every socket on the port: turbine binds several with `SO_REUSEPORT`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PortCounters {
    pub drops: u64,
    /// A gauge and the leading indicator: a queue that stays deep is a reader falling behind.
    pub queued: u64,
}

pub type PortMap = HashMap<u16, PortCounters>;

#[derive(Debug)]
pub struct PortWindow {
    span: Duration,
    samples: VecDeque<(Instant, HashMap<u16, u64>)>,
}

impl PortWindow {
    pub fn new(span: Duration) -> Self {
        Self {
            span,
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, now: Instant, totals: HashMap<u16, u64>) {
        // A gap of a whole span, as while nobody watched, leaves no sample that belongs in it.
        if self
            .samples
            .back()
            .is_some_and(|(last, _)| now.duration_since(*last) >= self.span)
        {
            self.samples.clear();
        }
        self.samples.push_back((now, totals));
        while let Some((next, _)) = self.samples.get(1) {
            if now.duration_since(*next) < self.span {
                break;
            }
            self.samples.pop_front();
        }
    }

    pub fn covers(&self, now: Instant) -> Duration {
        self.samples
            .front()
            .map(|(at, _)| now.duration_since(*at))
            .unwrap_or_default()
    }

    pub fn since(&self, port: u16, current: u64) -> u64 {
        self.samples
            .front()
            .and_then(|(_, totals)| totals.get(&port))
            // A socket closed and reopened restarts at zero, so a total below
            // the remembered one is read as no movement rather than as a wrap.
            .and_then(|earlier| current.checked_sub(*earlier))
            .unwrap_or(0)
    }
}

/// `/proc/net/udp6` is absent on a kernel without IPv6.
pub fn read() -> io::Result<PortMap> {
    let mut ports = PortMap::new();
    let mut rows: usize = 0;
    let mut last_err = None;

    for path in ["/proc/net/udp", "/proc/net/udp6"] {
        match std::fs::read_to_string(path) {
            Ok(contents) => rows = rows.saturating_add(parse_into(&contents, &mut ports)),
            Err(err) => last_err = Some(err),
        }
    }

    if rows == 0 {
        return Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "unrecognised /proc/net/udp")
        }));
    }
    Ok(ports)
}

/// `drops` is the thirteenth column, so an appended column is ignored.
fn parse_into(contents: &str, ports: &mut PortMap) -> usize {
    let mut rows: usize = 0;

    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let (Some(local), Some(queues), Some(drops)) =
            (fields.get(1), fields.get(4), fields.get(12))
        else {
            continue;
        };
        // Also how the heading row is rejected: its `local_address` column is
        // the literal word, which has no port to parse.
        let Some(port) = local
            .rsplit_once(':')
            .and_then(|(_, port)| u16::from_str_radix(port, 16).ok())
        else {
            continue;
        };
        let Ok(drops) = drops.parse::<u64>() else {
            continue;
        };
        let queued = queues
            .rsplit_once(':')
            .and_then(|(_, rx)| u64::from_str_radix(rx, 16).ok())
            .unwrap_or(0);

        let entry = ports.entry(port).or_default();
        entry.drops = entry.drops.saturating_add(drops);
        entry.queued = entry.queued.saturating_add(queued);
        rows = rows.saturating_add(1);
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two sockets on port 8001 (0x1F41) as `SO_REUSEPORT` gives, and one on 8899 (0x22C3): a
    /// verbatim transcript, so left unformatted.
    #[rustfmt::skip]
    const V4: &str = "\
   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  308: 00000000:1F41 00000000:0000 07 00000000:00000100 00:00000000 00000000     0        0 22359 2 0000000000000000 12
  309: 00000000:1F41 00000000:0000 07 00000000:00000200 00:00000000 00000000     0        0 22360 2 0000000000000000 30
  310: 0100007F:22C3 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 22361 2 0000000000000000 0
";

    /// The v6 table, whose wider address column must not shift the fields read
    /// from it. Unformatted for the same reason as [`V4`].
    #[rustfmt::skip]
    const V6: &str = "\
   sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  511: 00000000000000000000000000000000:1F41 00000000000000000000000000000000:0000 07 00000000:00000040 00:00000000 00000000     0        0 22362 2 0000000000000000 5
";

    fn parse(contents: &str) -> PortMap {
        let mut ports = PortMap::new();
        parse_into(contents, &mut ports);
        ports
    }

    #[test]
    fn test_sums_every_socket_sharing_a_port() {
        let ports = parse(V4);
        let reused = ports[&8001];
        assert_eq!(reused.drops, 42);
        // 0x100 and 0x200: the queue columns are hex where drops is decimal.
        assert_eq!(reused.queued, 768);
    }

    #[test]
    fn test_idle_socket_reports_zeroes_rather_than_being_absent() {
        let ports = parse(V4);
        assert_eq!(ports[&8899], PortCounters::default());
    }

    #[test]
    fn test_two_address_families_merge_into_one_port() {
        let mut ports = PortMap::new();
        assert_eq!(parse_into(V4, &mut ports), 3);
        assert_eq!(parse_into(V6, &mut ports), 1);
        assert_eq!(ports[&8001].drops, 47);
        assert_eq!(ports[&8001].queued, 832);
    }

    #[test]
    fn test_headings_alone_yield_nothing() {
        assert_eq!(parse(V4.lines().next().unwrap()).len(), 0);
        assert_eq!(parse("").len(), 0);
    }

    #[test]
    fn test_malformed_row_does_not_poison_the_total() {
        let text = format!("{V4}  311: garbage\n  312: 00000000:1F41 nonsense here\n");
        let ports = parse(&text);
        assert_eq!(ports[&8001].drops, 42);
        assert_eq!(ports.len(), 2);
    }

    fn totals(port: u16, drops: u64) -> HashMap<u16, u64> {
        HashMap::from([(port, drops)])
    }

    #[test]
    fn test_burst_ages_out_of_the_window() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));

        window.push(base, totals(8001, 0));
        window.push(base + Duration::from_secs(1), totals(8001, 100));
        assert_eq!(window.since(8001, 100), 100);

        for second in 2..=120 {
            window.push(base + Duration::from_secs(second), totals(8001, 100));
        }
        assert_eq!(window.since(8001, 100), 0);
    }

    #[test]
    fn test_window_covers_at_least_its_span_once_filled() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));
        for second in 0..=120 {
            window.push(base + Duration::from_secs(second), totals(8001, 0));
        }
        let covered = window.covers(base + Duration::from_secs(120));
        assert!(covered >= Duration::from_secs(60), "covered {covered:?}");
        assert!(covered <= Duration::from_secs(61), "covered {covered:?}");
    }

    #[test]
    fn test_short_window_reports_the_span_it_has_actually_watched() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));
        window.push(base, totals(8001, 0));
        window.push(base + Duration::from_secs(5), totals(8001, 3));
        assert_eq!(
            window.covers(base + Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        assert_eq!(window.since(8001, 3), 3);
    }

    #[test]
    fn test_gap_longer_than_the_span_starts_the_window_again() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));
        window.push(base, totals(8001, 0));
        window.push(base + Duration::from_secs(1), totals(8001, 10));

        let back = base + Duration::from_secs(3_600);
        window.push(back, totals(8001, 5_000));
        assert_eq!(window.covers(back), Duration::ZERO);
        assert_eq!(window.since(8001, 5_000), 0);

        window.push(back + Duration::from_secs(5), totals(8001, 5_007));
        assert_eq!(window.since(8001, 5_007), 7);
    }

    #[test]
    fn test_port_with_no_baseline_in_the_window_reports_no_drops() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));
        window.push(base, totals(8001, 0));
        assert_eq!(window.since(8899, 4_000), 0);
    }

    #[test]
    fn test_counter_reset_reads_as_no_drops_rather_than_a_wrap() {
        let base = Instant::now();
        let mut window = PortWindow::new(Duration::from_secs(60));
        window.push(base, totals(8001, 900));
        assert_eq!(window.since(8001, 12), 0);
    }

    #[test]
    fn test_row_missing_its_trailing_columns_is_skipped() {
        // As a kernel predating the drops column prints it; reading the last field would report the
        // inode.
        let text = "  308: 00000000:1F41 00000000:0000 07 00000000:00000100 00:00000000 00000000 \
                    0 0 22359\n";
        assert_eq!(parse(text).len(), 0);
    }
}
