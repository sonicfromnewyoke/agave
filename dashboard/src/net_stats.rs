//! Host-wide totals from `/proc/net/dev`, each byte counted once at the network cards, not the
//! validator's own traffic. Reports nothing rather than zeros where the file is unreadable.

use std::{collections::HashMap, io, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetCounters {
    pub received: u64,
    pub sent: u64,
}

/// Which interfaces a reading summed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counted {
    /// Those backed by a device, less any whose master is too.
    Cards,
    /// Every interface but loopback, where none is backed by a device.
    Every,
}

/// Each interface's received and sent bytes, by name.
pub type InterfaceCounters = HashMap<String, NetCounters>;

/// The host total and each interface's counters, from one read of the file, so rates taken from
/// them describe the same interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetReading {
    pub total: NetCounters,
    pub counted: Counted,
    pub interfaces: InterfaceCounters,
}

pub fn read() -> io::Result<NetReading> {
    let contents = std::fs::read_to_string("/proc/net/dev")?;
    reading_from(&contents, Path::new("/sys/class/net"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unrecognised /proc/net/dev"))
}

/// The total over the cards found under `sys`, or over every interface where there are none.
fn reading_from(contents: &str, sys: &Path) -> Option<NetReading> {
    let interfaces: InterfaceCounters = crate::net_ifaces::parse_counters(contents)
        .into_iter()
        .map(|(name, received, sent)| (name, NetCounters { received, sent }))
        .collect();
    let on_a_card = |name: &str| crate::net_ifaces::counts_toward_host_total(&sys.join(name));
    let (total, counted) = sum(&interfaces, on_a_card)
        .map(|total| (total, Counted::Cards))
        // A container sees only virtual interfaces.
        .or_else(|| sum(&interfaces, |_| true).map(|total| (total, Counted::Every)))?;
    Some(NetReading {
        total,
        counted,
        interfaces,
    })
}

/// The interfaces `include` keeps, summed; none where it keeps none.
fn sum(interfaces: &InterfaceCounters, include: impl Fn(&str) -> bool) -> Option<NetCounters> {
    interfaces
        .iter()
        .filter(|(name, _)| include(name))
        .map(|(_, counters)| *counters)
        .reduce(|total, counters| NetCounters {
            received: total.received.saturating_add(counters.received),
            sent: total.sent.saturating_add(counters.sent),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No cards here, so every interface is summed.
    fn every(contents: &str) -> Option<NetReading> {
        reading_from(contents, Path::new("/nonexistent"))
    }

    const BONDED: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000       10    0    0    0     0          0         0     2000      20    0    0    0     0       0          0
 bond0: 9000       90    0    0    0     0          0         0     6000      60    0    0    0     0       0          0
  eno1: 6000       60    0    0    0     0          0         0     4000      40    0    0    0     0       0          0
  eno2: 3000       30    0    0    0     0          0         0     2000      20    0    0    0     0       0          0
doublezero0:  500   5    0    0    0     0          0         0      300       3    0    0    0     0       0          0
 veth0:   70        1    0    0    0     0          0         0       80       1    0    0    0     0       0          0
";

    #[test]
    fn test_sums_interfaces_and_excludes_loopback() {
        let reading = every(BONDED).unwrap();
        assert_eq!(reading.counted, Counted::Every);
        assert_eq!(reading.total.received, 18_570);
        assert_eq!(reading.total.sent, 12_380);
    }

    #[test]
    fn test_headings_alone_yield_nothing() {
        let headings = BONDED.lines().take(2).collect::<Vec<_>>().join("\n");
        assert!(every(&headings).is_none());
        assert!(every("").is_none());
    }

    #[test]
    fn test_malformed_row_does_not_poison_the_total() {
        let text = format!("{BONDED}  eth2: garbage\n");
        assert_eq!(every(&text).unwrap().total.received, 18_570);
    }

    #[test]
    fn test_counts_only_the_interfaces_asked_for() {
        let interfaces = every(BONDED).unwrap().interfaces;
        let counters = sum(&interfaces, |name| name == "eno1").unwrap();
        assert_eq!((counters.received, counters.sent), (6000, 4000));
        assert!(sum(&interfaces, |_| false).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_reads_the_cards_once() {
        use std::{fs, os::unix::fs::symlink};
        let cards = tempfile::tempdir().unwrap();
        for name in ["lo", "bond0", "eno1", "eno2", "doublezero0", "veth0"] {
            fs::create_dir(cards.path().join(name)).unwrap();
        }
        for port in ["eno1", "eno2"] {
            fs::create_dir(cards.path().join(port).join("device")).unwrap();
            symlink("../bond0", cards.path().join(port).join("master")).unwrap();
        }
        let reading = reading_from(BONDED, cards.path()).unwrap();
        assert_eq!(
            (reading.total.received, reading.total.sent, reading.counted),
            (9000, 6000, Counted::Cards)
        );
        // Each interface comes from the same read as the total, the bond and its ports alike.
        let counters = |received, sent| NetCounters { received, sent };
        assert_eq!(reading.interfaces["bond0"], counters(9000, 6000));
        assert_eq!(reading.interfaces["eno2"], counters(3000, 2000));
        assert!(!reading.interfaces.contains_key("lo"));
        assert_eq!(reading_from("", cards.path()), None);
    }
}
