//! The host's network interfaces: each one's kind, state, routes and master, from `/proc/net/dev`,
//! `/proc/net/route` and `/sys/class/net`. Linux only. Their traffic is read with the host total.

use {
    serde::Serialize,
    std::{collections::HashMap, io, path::Path},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceKind {
    Physical,
    Bond,
    Bridge,
    Vlan,
    Tunnel,
    Virtual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterfaceReading {
    pub name: String,
    pub kind: InterfaceKind,
    pub up: bool,
    pub mtu: Option<u32>,
    pub routes: u32,
    /// The interface it is a port of (its master), such as a bond, bridge or team.
    pub member_of: Option<String>,
    /// On a tunnel, the interface holding the default route, which carries its packets.
    pub inside: Option<String>,
}

/// Link types the kernel gives tunnels: IPIP, IPv6-in-IPv6, SIT, GRE, IPv6 GRE, and none, which
/// TUN devices and WireGuard report.
const TUNNEL_TYPES: [u32; 6] = [768, 769, 776, 778, 823, 65534];

const IFF_UP: u32 = 0x1;

pub fn read() -> io::Result<Vec<InterfaceReading>> {
    let counters = parse_counters(&std::fs::read_to_string("/proc/net/dev")?);
    // A host without IPv4 routes has no file; every interface then carries none.
    let route_table = std::fs::read_to_string("/proc/net/route").unwrap_or_default();
    let routes = parse_routes(&route_table);
    let mut readings: Vec<InterfaceReading> = counters
        .into_iter()
        .map(|(name, _, _)| {
            let sys = Path::new("/sys/class/net").join(&name);
            InterfaceReading {
                kind: kind_of(&sys),
                up: is_up(&sys),
                mtu: read_trimmed(&sys.join("mtu")).and_then(|mtu| mtu.parse().ok()),
                routes: routes.get(&name).copied().unwrap_or(0),
                member_of: master_of(&sys),
                inside: None,
                name,
            }
        })
        .collect();
    place_tunnels(&mut readings, &route_table);
    Ok(readings)
}

/// Each interface but loopback, with its received and sent bytes.
pub(crate) fn parse_counters(contents: &str) -> Vec<(String, u64, u64)> {
    contents
        .lines()
        .filter_map(|line| {
            let (name, counters) = line.split_once(':')?;
            let name = name.trim();
            if name == "lo" {
                return None;
            }
            let mut fields = counters.split_whitespace();
            let received = fields.next()?.parse().ok()?;
            let sent = fields.nth(7)?.parse().ok()?;
            Some((name.to_string(), received, sent))
        })
        .collect()
}

/// Routes per interface in the main IPv4 table, after the heading line.
fn parse_routes(contents: &str) -> HashMap<String, u32> {
    let mut routes = HashMap::new();
    for line in contents.lines().skip(1) {
        if let Some(name) = line.split_whitespace().next() {
            let count: &mut u32 = routes.entry(name.to_string()).or_default();
            *count = count.saturating_add(1);
        }
    }
    routes
}

fn place_tunnels(readings: &mut [InterfaceReading], route_table: &str) {
    let carrier = default_route(route_table, |name| {
        readings
            .iter()
            .any(|reading| reading.name == name && reading.kind != InterfaceKind::Tunnel)
    });
    for reading in readings
        .iter_mut()
        .filter(|reading| reading.kind == InterfaceKind::Tunnel)
    {
        reading.inside.clone_from(&carrier);
    }
}

/// The interface of the lowest-metric IPv4 default route that `eligible` accepts.
fn default_route(route_table: &str, eligible: impl Fn(&str) -> bool) -> Option<String> {
    route_table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let name = fields.next()?;
            let destination = fields.next()?;
            // Gateway, flags, reference count and use come before the metric.
            let metric = fields.nth(4)?.parse::<u32>().ok()?;
            let mask = fields.next()?;
            (destination == "00000000" && mask == "00000000" && eligible(name))
                .then_some((metric, name))
        })
        .min_by_key(|(metric, _)| *metric)
        .map(|(_, name)| name.to_string())
}

fn master_of(sys: &Path) -> Option<String> {
    let target = std::fs::read_link(sys.join("master")).ok()?;
    target.file_name()?.to_str().map(str::to_string)
}

/// Backed by a device, and not a port of an interface that is, whose counters already hold its own.
pub(crate) fn counts_toward_host_total(sys: &Path) -> bool {
    sys.join("device").exists() && !sys.join("master").join("device").exists()
}

fn kind_of(sys: &Path) -> InterfaceKind {
    let devtype = read_trimmed(&sys.join("uevent")).and_then(|uevent| {
        uevent
            .lines()
            .find_map(|line| line.strip_prefix("DEVTYPE=").map(str::to_string))
    });
    let link_type = read_trimmed(&sys.join("type")).and_then(|kind| kind.parse::<u32>().ok());
    if sys.join("bonding").is_dir() {
        InterfaceKind::Bond
    } else if sys.join("bridge").is_dir() {
        InterfaceKind::Bridge
    } else if devtype.as_deref() == Some("vlan") {
        InterfaceKind::Vlan
    } else if sys.join("tun_flags").exists()
        || link_type.is_some_and(|kind| TUNNEL_TYPES.contains(&kind))
    {
        InterfaceKind::Tunnel
    } else if sys.join("device").exists() {
        InterfaceKind::Physical
    } else {
        InterfaceKind::Virtual
    }
}

/// Administratively up and not reported down. Tunnels report their state as unknown while working.
fn is_up(sys: &Path) -> bool {
    let flagged = read_trimmed(&sys.join("flags"))
        .and_then(|flags| u32::from_str_radix(flags.trim_start_matches("0x"), 16).ok())
        .is_some_and(|flags| flags & IFF_UP != 0);
    let operstate = read_trimmed(&sys.join("operstate")).unwrap_or_default();
    flagged && !matches!(operstate.as_str(), "down" | "lowerlayerdown" | "notpresent")
}

fn read_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|contents| contents.trim().to_string())
}

#[cfg(test)]
mod tests {
    use {super::*, std::fs};

    const DEV: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000       10    0    0    0     0          0         0     2000      20    0    0    0     0       0          0
  eno1: 5000       50    0    0    0     0          0         0     7000      70    0    0    0     0       0          0
doublezero0:  500   5    0    0    0     0          0         0      300       3    0    0    0     0       0          0
";

    const ROUTE: &str = "\
Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT
eno1\t00000000\t0101A8C0\t0003\t0\t0\t0\t00000000\t0\t0\t0
eno1\t0001A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0
doublezero0\t0000400A\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0
";

    #[test]
    fn test_counters_per_interface_without_loopback() {
        assert_eq!(
            parse_counters(DEV),
            vec![
                ("eno1".to_string(), 5000, 7000),
                ("doublezero0".to_string(), 500, 300),
            ]
        );
    }

    const DEFAULTS: &str = "\
Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT
doublezero0\t00000000\t00000000\t0001\t0\t0\t0\t00000000\t0\t0\t0
*\t00000000\t00000000\t0201\t0\t0\t0\t00000000\t0\t0\t0
eno1\t00000000\t0101A8C0\t0003\t0\t0\t200\t00000000\t0\t0\t0
bond0\t00000000\t0101A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0
bond0\t0001A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0
";

    #[test]
    fn test_routes_counted_per_interface_after_the_heading() {
        let routes = parse_routes(ROUTE);
        assert_eq!(routes.get("eno1"), Some(&2));
        assert_eq!(routes.get("doublezero0"), Some(&1));
        assert_eq!(routes.get("Iface"), None);
    }

    #[test]
    fn test_default_route_is_the_lowest_metric_one_eligible() {
        let named = |names: &'static [&'static str]| move |name: &str| names.contains(&name);
        assert_eq!(
            default_route(DEFAULTS, named(&["eno1", "bond0"])),
            Some("bond0".to_string())
        );
        assert_eq!(
            default_route(DEFAULTS, named(&["eno1"])),
            Some("eno1".to_string())
        );
        assert_eq!(default_route(DEFAULTS, |_| false), None);
        assert_eq!(default_route(ROUTE, |_| true), Some("eno1".to_string()));
        assert_eq!(default_route("", |_| true), None);
    }

    fn tunnel_test_reading(name: &str, kind: InterfaceKind) -> InterfaceReading {
        InterfaceReading {
            name: name.to_string(),
            kind,
            up: true,
            mtu: None,
            routes: 0,
            member_of: None,
            inside: None,
        }
    }

    #[test]
    fn test_a_tunnel_is_inside_the_default_route_holder_never_a_tunnel() {
        let mut readings = [
            tunnel_test_reading("eno1", InterfaceKind::Physical),
            tunnel_test_reading("bond0", InterfaceKind::Bond),
            tunnel_test_reading("doublezero0", InterfaceKind::Tunnel),
        ];
        place_tunnels(&mut readings, DEFAULTS);
        let inside: Vec<_> = readings
            .iter()
            .map(|reading| reading.inside.as_deref())
            .collect();
        assert_eq!(inside, [None, None, Some("bond0")]);
        let mut alone = [tunnel_test_reading("doublezero0", InterfaceKind::Tunnel)];
        place_tunnels(&mut alone, DEFAULTS);
        assert_eq!(alone[0].inside, None);
    }

    #[cfg(unix)]
    #[test]
    fn test_a_port_names_its_master_and_counts_only_where_the_master_has_no_device() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let sys = |name: &str| root.path().join(name);
        for dir in [
            "bond0",
            "eno1/device",
            "eth0/device",
            "enP1s1/device",
            "veth0",
        ] {
            fs::create_dir_all(sys(dir)).unwrap();
        }
        symlink("../bond0", sys("eno1/master")).unwrap();
        symlink("../eth0", sys("enP1s1/master")).unwrap();
        assert_eq!(master_of(&sys("eno1")), Some("bond0".to_string()));
        assert_eq!(master_of(&sys("bond0")), None);
        assert!(
            counts_toward_host_total(&sys("eno1")),
            "the bond has no device"
        );
        assert!(!counts_toward_host_total(&sys("bond0")));
        assert!(counts_toward_host_total(&sys("eth0")));
        assert!(
            !counts_toward_host_total(&sys("enP1s1")),
            "a VF its synthetic NIC already counts"
        );
        assert!(!counts_toward_host_total(&sys("veth0")));
    }

    fn interface(files: &[(&str, &str)], dirs: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for (name, contents) in files {
            fs::write(root.path().join(name), contents).unwrap();
        }
        for dir in dirs {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        root
    }

    #[test]
    fn test_kind_from_sysfs() {
        let kind = |files: &[(&str, &str)], dirs: &[&str]| kind_of(interface(files, dirs).path());
        let ethernet = [("type", "1\n")];
        assert_eq!(kind(&ethernet, &["device"]), InterfaceKind::Physical);
        assert_eq!(kind(&ethernet, &["bonding"]), InterfaceKind::Bond);
        assert_eq!(kind(&ethernet, &["bridge"]), InterfaceKind::Bridge);
        assert_eq!(
            kind(
                &[
                    ("type", "1\n"),
                    ("uevent", "DEVTYPE=vlan\nINTERFACE=eno1.10\n")
                ],
                &[]
            ),
            InterfaceKind::Vlan
        );
        assert_eq!(kind(&[("type", "778\n")], &[]), InterfaceKind::Tunnel);
        assert_eq!(
            kind(&[("type", "65534\n"), ("tun_flags", "0x1001\n")], &[]),
            InterfaceKind::Tunnel
        );
        assert_eq!(kind(&ethernet, &[]), InterfaceKind::Virtual);
    }

    #[test]
    fn test_a_tunnel_reporting_unknown_is_up_and_a_link_without_carrier_is_not() {
        let tunnel = interface(&[("flags", "0x1091\n"), ("operstate", "unknown\n")], &[]);
        assert!(is_up(tunnel.path()));
        let unplugged = interface(&[("flags", "0x1003\n"), ("operstate", "down\n")], &[]);
        assert!(!is_up(unplugged.path()));
        let disabled = interface(&[("flags", "0x1002\n"), ("operstate", "unknown\n")], &[]);
        assert!(!is_up(disabled.path()));
    }
}
