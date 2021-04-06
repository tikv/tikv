// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::string::ToString;

use crate::server::service::diagnostics::ioload;
use kvproto::diagnosticspb::{ServerInfoItem, ServerInfoPair};
use tikv_util::config::KB;
use tikv_util::sys::{cpu_time::LiunxStyleCpuTime, sys_quota::SysQuota, *};
use walkdir::WalkDir;

type CpuTimeSnapshot = Option<LiunxStyleCpuTime>;

#[derive(Clone, Debug)]
pub struct NicSnapshot {
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
    rx_errors: u64,
    tx_errors: u64,
}

impl NicSnapshot {
    pub fn from_network_data(data: &impl NetworkExt) -> NicSnapshot {
        NicSnapshot {
            rx_bytes: data.get_total_received(),
            tx_bytes: data.get_total_transmitted(),
            rx_packets: data.get_total_packets_received(),
            tx_packets: data.get_total_packets_transmitted(),
            rx_errors: data.get_total_errors_on_received(),
            tx_errors: data.get_total_errors_on_transmitted(),
        }
    }

    fn into_pairs(self, prev: &NicSnapshot) -> Vec<ServerInfoPair> {
        macro_rules! pair {
            ($label: literal, $value: expr, $old_value: expr) => {{
                let mut pair = ServerInfoPair::default();
                pair.set_key($label.to_owned());
                pair.set_value(format!("{:.2}", ($value - $old_value) as f64));
                pair
            }};
        }

        vec![
            pair!("rx-bytes/s", self.rx_bytes, prev.rx_bytes),
            pair!("tx-bytes/s", self.tx_bytes, prev.tx_bytes),
            pair!("rx-packets/s", self.rx_packets, prev.rx_packets),
            pair!("tx-packets/s", self.tx_packets, prev.tx_packets),
            pair!("rx-errors/s", self.rx_errors, prev.rx_errors),
            pair!("tx-errors/s", self.tx_errors, prev.tx_errors),
        ]
    }
}

fn cpu_load_info(prev_cpu: CpuTimeSnapshot, collector: &mut Vec<ServerInfoItem>) {
    // CPU load
    {
        let infos = {
            let mut system = SYS_INFO.lock().unwrap();
            system.refresh_system();
            let load = system.get_load_average();
            vec![
                ("load1", load.one),
                ("load5", load.five),
                ("load15", load.fifteen),
            ]
        };
        let mut pairs = vec![];
        for info in infos.iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(info.0.to_string());
            pair.set_value(info.1.to_string());
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("cpu".to_string());
        item.set_name("cpu".to_string());
        item.set_pairs(pairs.into());
        collector.push(item);
    }

    if prev_cpu.is_none() {
        return;
    }

    let t2 = LiunxStyleCpuTime::current();
    if t2.is_err() {
        return;
    }
    let t2 = t2.unwrap();
    if t2.total() == 0 {
        return;
    }

    let delta = t2 - prev_cpu.unwrap();
    let delta_total = delta.total() as f64;
    let data: Vec<(&'static str, f64)> = vec![
        ("user", delta.user as f64 / delta_total),
        ("system", delta.system as f64 / delta_total),
        ("idle", delta.idle as f64 / delta_total),
        ("nice", delta.nice as f64 / delta_total),
        ("iowait", delta.iowait as f64 / delta_total),
        ("irq", delta.irq as f64 / delta_total),
        ("softirq", delta.softirq as f64 / delta_total),
        ("steal", delta.steal as f64 / delta_total),
        ("guest", delta.guest as f64 / delta_total),
        ("guest_nice", delta.guest_nice as f64 / delta_total),
    ];

    let mut pairs = vec![];
    for (key, value) in data {
        let mut pair = ServerInfoPair::default();
        pair.set_key(key.to_string());
        pair.set_value(format!("{:.2}", value));
        pairs.push(pair);
    }

    let mut item = ServerInfoItem::default();
    item.set_tp("cpu".to_string());
    item.set_name("usage".to_string());
    item.set_pairs(pairs.into());
    collector.push(item);
}

fn mem_load_info(collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_memory();
    let total_memory = system.get_total_memory() * KB;
    let used_memory = system.get_used_memory() * KB;
    let free_memory = system.get_free_memory() * KB;
    let total_swap = system.get_total_swap() * KB;
    let used_swap = system.get_used_swap() * KB;
    let free_swap = system.get_free_swap() * KB;
    drop(system);
    let used_memory_pct = (used_memory as f64) / (total_memory as f64);
    let free_memory_pct = (free_memory as f64) / (total_memory as f64);
    let used_swap_pct = (used_swap as f64) / (total_swap as f64);
    let free_swap_pct = (free_swap as f64) / (total_swap as f64);
    let infos = vec![
        (
            "virtual",
            vec![
                ("total", total_memory.to_string()),
                ("used", used_memory.to_string()),
                ("free", free_memory.to_string()),
                ("used-percent", format!("{:.2}", used_memory_pct)),
                ("free-percent", format!("{:.2}", free_memory_pct)),
            ],
        ),
        (
            "swap",
            vec![
                ("total", total_swap.to_string()),
                ("used", used_swap.to_string()),
                ("free", free_swap.to_string()),
                ("used-percent", format!("{:.2}", used_swap_pct)),
                ("free-percent", format!("{:.2}", free_swap_pct)),
            ],
        ),
    ];
    for info in infos.into_iter() {
        let mut pairs = vec![];
        for item in info.1.into_iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(item.0.to_string());
            pair.set_value(item.1);
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("memory".to_string());
        item.set_name(info.0.to_string());
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

fn nic_load_info(prev_nic: HashMap<String, NicSnapshot>, collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_networks_list();
    system.refresh_networks();
    let current = system.get_networks();

    for (name, cur) in current {
        let prev = match prev_nic.get(name) {
            Some(p) => p,
            None => continue,
        };
        let cur = NicSnapshot::from_network_data(cur);
        let pairs = cur.into_pairs(prev);
        let mut item = ServerInfoItem::default();
        item.set_tp("net".to_string());
        item.set_name(name.clone());
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

fn io_load_info(prev_io: HashMap<String, ioload::IoLoad>, collector: &mut Vec<ServerInfoItem>) {
    let current = ioload::IoLoad::snapshot();
    let rate = |cur, prev| (cur - prev) as f64;
    for (name, cur) in current.into_iter() {
        let prev = match prev_io.get(&name) {
            Some(p) => p,
            None => continue,
        };
        let mut infos = vec![
            ("read_io/s", rate(cur.read_io, prev.read_io)),
            ("read_merges/s", rate(cur.read_merges, prev.read_merges)),
            (
                "read_sectors/s",
                rate(cur.read_sectors, prev.read_sectors) * 512f64,
            ),
            ("read_ticks/s", rate(cur.read_ticks, prev.read_ticks)),
            ("write_io/s", rate(cur.write_io, prev.write_io)),
            ("write_merges/s", rate(cur.write_merges, prev.write_merges)),
            (
                "write_sectors/s",
                rate(cur.write_sectors, prev.write_sectors) * 512f64,
            ),
            ("write_ticks/s", rate(cur.write_ticks, prev.write_ticks)),
            ("in_flight/s", rate(cur.in_flight, prev.in_flight)),
            ("io_ticks/s", rate(cur.io_ticks, prev.io_ticks)),
            (
                "time_in_queue/s",
                rate(cur.time_in_queue, prev.time_in_queue),
            ),
        ];
        infos.extend(
            [
                ("discard_io/s", cur.discard_io, prev.discard_io),
                ("discard_merged/s", cur.discard_merged, prev.discard_merged),
                (
                    "discard_sectors/s",
                    cur.discard_sectors,
                    prev.discard_sectors,
                ),
                ("discard_ticks/s", cur.discard_ticks, prev.discard_ticks),
            ]
            .iter()
            .filter_map(|(name, cur_stat, prev_stat)| {
                if let (Some(cur_stat), Some(prev_stat)) = (cur_stat, prev_stat) {
                    Some((*name, rate(*cur_stat, *prev_stat)))
                } else {
                    None
                }
            }),
        );
        let mut pairs = vec![];
        for info in infos.into_iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(info.0.to_string());
            pair.set_value(format!("{:.2}", info.1));
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("io".to_string());
        item.set_name(name);
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

pub fn cpu_time_snapshot() -> CpuTimeSnapshot {
    let t1 = LiunxStyleCpuTime::current();
    if t1.is_err() {
        return None;
    }
    let t1 = t1.unwrap();
    if t1.total() == 0 {
        return None;
    }
    Some(t1)
}

/// load_info collects CPU/Memory/IO/Network load information
pub fn load_info(
    (prev_cpu, prev_nic, prev_io): (
        CpuTimeSnapshot,
        HashMap<String, NicSnapshot>,
        HashMap<String, ioload::IoLoad>,
    ),
    collector: &mut Vec<ServerInfoItem>,
) {
    cpu_load_info(prev_cpu, collector);
    mem_load_info(collector);
    nic_load_info(prev_nic, collector);
    io_load_info(prev_io, collector);
}

fn cpu_hardware_info(collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_cpu();
    let processor = match system.get_processors().iter().next() {
        Some(p) => p,
        None => return,
    };
    let mut infos = vec![
        (
            "cpu-logical-cores",
            SysQuota::new().cpu_cores_quota().to_string(),
        ),
        ("cpu-physical-cores", num_cpus::get_physical().to_string()),
        ("cpu-frequency", format!("{}MHz", processor.get_frequency())),
        ("cpu-vendor-id", processor.get_vendor_id().to_string()),
    ];
    // Depend on Rust lib return CPU arch not matching
    // Golang lib so need this match loop to conversion
    // Golang Doc:https://go.googlesource.com/go/+/go1.15.8/src/runtime/internal/sys/zgoarch_amd64.go#7
    // Rust Doc:http://web.mit.edu/rust-lang_v1.26.0/arch/amd64_ubuntu1404/share/doc/rust/html/std/env/consts/constant.ARCH.html
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "x86" => "386",
        "aarch64" => "arm64",
        "powerpc" => "ppc",
        "powerpc64" => "ppc64",
        _ => std::env::consts::ARCH,
    };
    infos.push(("cpu-arch", arch.to_string()));
    // cache
    let caches = vec![
        ("l1-cache-size", cache_size(1)),
        ("l1-cache-line-size", cache_line_size(1)),
        ("l2-cache-size", cache_size(2)),
        ("l2-cache-line-size", cache_line_size(2)),
        ("l3-cache-size", cache_size(3)),
        ("l3-cache-line-size", cache_line_size(3)),
    ];
    for cache in caches.into_iter() {
        if let Some(v) = cache.1 {
            infos.push((cache.0, v.to_string()));
        }
    }
    let mut pairs = vec![];
    for info in infos.into_iter() {
        let mut pair = ServerInfoPair::default();
        pair.set_key(info.0.to_string());
        pair.set_value(info.1);
        pairs.push(pair);
    }
    let mut item = ServerInfoItem::default();
    item.set_tp("cpu".to_string());
    item.set_name("cpu".to_string());
    item.set_pairs(pairs.into());
    collector.push(item);
}

fn mem_hardware_info(collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_memory();
    let mut pair = ServerInfoPair::default();
    pair.set_key("capacity".to_string());
    pair.set_value((system.get_total_memory() * KB).to_string());
    let mut item = ServerInfoItem::default();
    item.set_tp("memory".to_string());
    item.set_name("memory".to_string());
    item.set_pairs(vec![pair].into());
    collector.push(item);
}

fn disk_hardware_info(collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_disks_list();
    system.refresh_disks();
    let disks = system.get_disks();
    for disk in disks {
        let total = disk.get_total_space();
        let free = disk.get_available_space();
        let used = total - free;
        let free_pct = (free as f64) / (total as f64);
        let used_pct = (used as f64) / (total as f64);
        let infos = vec![
            ("type", format!("{:?}", disk.get_type())),
            (
                "fstype",
                std::str::from_utf8(disk.get_file_system())
                    .unwrap_or("unkonwn")
                    .to_string(),
            ),
            (
                "path",
                disk.get_mount_point()
                    .to_str()
                    .unwrap_or("unknown")
                    .to_string(),
            ),
            ("total", total.to_string()),
            ("free", free.to_string()),
            ("used", used.to_string()),
            ("free-percent", format!("{:.2}", free_pct)),
            ("used-percent", format!("{:.2}", used_pct)),
        ];
        let mut pairs = vec![];
        for info in infos.into_iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(info.0.to_string());
            pair.set_value(info.1);
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("disk".to_string());
        item.set_name(disk.get_name().to_str().unwrap_or("disk").to_string());
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

fn mac_address(mac: Option<pnet_datalink::MacAddr>) -> String {
    match mac {
        Some(mac) => mac.to_string(),
        None => "none".to_string(),
    }
}

fn nic_hardware_info(collector: &mut Vec<ServerInfoItem>) {
    let nics = pnet_datalink::interfaces();
    for nic in nics.into_iter() {
        let mut infos = vec![
            ("mac", mac_address(nic.mac)),
            ("flag", nic.flags.to_string()),
            ("index", nic.index.to_string()),
            ("is-up", nic.is_up().to_string()),
            ("is-broadcast", nic.is_broadcast().to_string()),
            ("is-multicast", nic.is_multicast().to_string()),
            ("is-loopback", nic.is_loopback().to_string()),
            ("is-point-to-point", nic.is_point_to_point().to_string()),
        ];
        for ip in nic.ips.into_iter() {
            infos.push(("ip", ip.to_string()));
        }
        let mut pairs = vec![];
        for info in infos.into_iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(info.0.to_string());
            pair.set_value(info.1);
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("net".to_string());
        item.set_name(nic.name);
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

/// hardware_info collects CPU/Memory/Network/Disk hardware information
pub fn hardware_info(collector: &mut Vec<ServerInfoItem>) {
    cpu_hardware_info(collector);
    mem_hardware_info(collector);
    disk_hardware_info(collector);
    nic_hardware_info(collector);
}

/// system_info collects system related information, e.g: kernel
pub fn system_info(collector: &mut Vec<ServerInfoItem>) {
    // sysctl
    let sysctl = get_sysctl_list();
    let mut pairs = vec![];
    for (key, val) in sysctl.into_iter() {
        let mut pair = ServerInfoPair::default();
        pair.set_key(key);
        pair.set_value(val);
        pairs.push(pair);
    }
    // Sort pairs by key to make result stable
    pairs.sort_by(|a, b| a.get_key().cmp(b.get_key()));
    let mut item = ServerInfoItem::default();
    item.set_tp("system".to_string());
    item.set_name("sysctl".to_string());
    item.set_pairs(pairs.into());
    collector.push(item);
    if let Some(item) = get_transparent_hugepage() {
        collector.push(item);
    }
}

/// Returns system wide configuration
///
/// # Note
///
/// Current only can be used in operating system mounted `procfs`
fn get_sysctl_list() -> HashMap<String, String> {
    const DIR: &str = "/proc/sys/";
    WalkDir::new(DIR)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let content = std::fs::read_to_string(entry.path()).ok()?;
            let path = entry.path().to_str()?;

            let name = path.trim_start_matches(DIR).replace("/", ".");
            Some((name, content.trim().to_string()))
        })
        .collect()
}

fn get_transparent_hugepage() -> Option<ServerInfoItem> {
    if let Ok(content) = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
        let mut pairs = vec![];
        let mut pair = ServerInfoPair::default();
        pair.set_key("transparent_hugepage_enabled".to_string());
        pair.set_value(content.trim().to_string());
        pairs.push(pair);
        let mut item = ServerInfoItem::default();
        item.set_tp("system".to_string());
        item.set_name("kernel".to_string());
        item.set_pairs(pairs.into());
        return Some(item);
    }
    None
}

/// process_info collects all process list
/// TODO: use different `ServerInfoType` to collect process list
#[allow(dead_code)]
pub fn process_info(collector: &mut Vec<ServerInfoItem>) {
    let mut system = SYS_INFO.lock().unwrap();
    system.refresh_processes();
    let processes = system.get_processes();
    for (pid, p) in processes.iter() {
        if p.cmd().is_empty() {
            continue;
        }
        let mut pairs = vec![];
        let infos = vec![
            ("executable", format!("{:?}", p.exe())),
            ("cmd", p.cmd().join(" ")),
            ("cwd", format!("{:?}", p.cwd())),
            ("start-time", p.start_time().to_string()),
            ("memory", p.memory().to_string()),
            ("status", p.status().to_string().to_owned()),
            ("cpu-usage", p.cpu_usage().to_string()),
        ];
        for (key, val) in infos.into_iter() {
            let mut pair = ServerInfoPair::default();
            pair.set_key(key.to_string());
            pair.set_value(val);
            pairs.push(pair);
        }
        let mut item = ServerInfoItem::default();
        item.set_tp("process".to_string());
        item.set_name(format!("{}({})", p.name(), pid));
        item.set_pairs(pairs.into());
        collector.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_load_info() {
        let prev_cpu = cpu_time_snapshot();
        let prev_nic = {
            let mut system = SYS_INFO.lock().unwrap();
            system.refresh_networks_list();
            system.refresh_all();
            system
                .get_networks()
                .into_iter()
                .map(|(n, d)| (n.to_owned(), NicSnapshot::from_network_data(d)))
                .collect()
        };
        let prev_io = ioload::IoLoad::snapshot();
        let mut collector = vec![];
        load_info((prev_cpu, prev_nic, prev_io), &mut collector);
        #[cfg(target_os = "linux")]
        let tps = vec!["cpu", "memory", "net", "io"];
        #[cfg(not(target_os = "linux"))]
        let tps = vec!["cpu", "memory"];
        for tp in tps.into_iter() {
            assert!(
                collector.iter().any(|x| x.get_tp() == tp),
                "expect collect {}, but collect nothing",
                tp
            );
        }

        let mut cpu_info = collector.iter().filter(|x| x.get_tp() == "cpu");
        // load1/5/15
        let cpu_load = cpu_info.find(|x| x.get_name() == "cpu").unwrap();
        let keys = cpu_load
            .get_pairs()
            .iter()
            .map(|x| x.get_key())
            .collect::<Vec<&str>>();
        assert_eq!(keys, vec!["load1", "load5", "load15"]);
        // cpu_stat
        let cpu_stat = cpu_info.next().unwrap();
        let keys = cpu_stat
            .get_pairs()
            .iter()
            .map(|x| x.get_key())
            .collect::<Vec<&str>>();
        assert_eq!(
            keys,
            vec![
                "user",
                "system",
                "idle",
                "nice",
                "iowait",
                "irq",
                "softirq",
                "steal",
                "guest",
                "guest_nice",
            ]
        );
        // memory
        for name in vec!["virtual", "swap"].into_iter() {
            let item = collector
                .iter()
                .find(|x| x.get_tp() == "memory" && x.get_name() == name);
            let keys = item
                .unwrap()
                .get_pairs()
                .iter()
                .map(|x| x.get_key())
                .collect::<Vec<&str>>();
            assert_eq!(
                keys,
                vec!["total", "used", "free", "used-percent", "free-percent",]
            );
        }
        #[cfg(target_os = "linux")]
        {
            // io
            let item = collector.iter().find(|x| x.get_tp() == "io");
            let keys = item
                .unwrap()
                .get_pairs()
                .iter()
                .map(|x| x.get_key())
                .collect::<Vec<&str>>();
            assert_eq!(
                keys,
                vec![
                    "read_io/s",
                    "read_merges/s",
                    "read_sectors/s",
                    "read_ticks/s",
                    "write_io/s",
                    "write_merges/s",
                    "write_sectors/s",
                    "write_ticks/s",
                    "in_flight/s",
                    "io_ticks/s",
                    "time_in_queue/s",
                ]
            );
        }
    }

    #[test]
    fn test_system_info() {
        let mut collector = vec![];
        system_info(&mut collector);
        assert!(
            collector.iter().any(|x| x.get_tp() == "system"),
            "expect collect system, but collect nothing",
        );
        #[cfg(target_os = "linux")]
        {
            let item = collector
                .iter()
                .filter(|x| x.get_tp() == "system" && x.get_name() == "sysctl");
            assert_ne!(item.count(), 0);
            let item = collector
                .iter()
                .filter(|x| x.get_tp() == "system" && x.get_name() == "kernel");
            assert_ne!(item.count(), 0);
        }
    }

    #[test]
    fn test_process_info() {
        let mut collector = vec![];
        process_info(&mut collector);
        assert!(
            collector.iter().any(|x| x.get_tp() == "process"),
            "expect collect process, but collect nothing",
        );
        // at least contains the unit test process
        let processes = collector.iter().find(|x| x.get_tp() == "process").unwrap();
        assert_ne!(processes.get_pairs().len(), 0);
    }

    #[test]
    fn test_hardware_info() {
        let mut collector = vec![];
        hardware_info(&mut collector);
        let tps = vec!["cpu", "memory", "net", "disk"];
        for tp in tps.into_iter() {
            assert!(
                collector.iter().any(|x| x.get_tp() == tp),
                "expect collect {}, but collect nothing",
                tp
            );
        }
        // cpu
        let cpu_info = collector.iter().find(|x| x.get_tp() == "cpu").unwrap();
        let vendor_id = cpu_info
            .get_pairs()
            .iter()
            .find(|x| x.get_key() == "cpu-vendor-id")
            .unwrap()
            .get_value();
        if vendor_id != "AuthenticAMD" {
            assert_eq!(
                cpu_info
                    .get_pairs()
                    .iter()
                    .map(|x| x.get_key())
                    .collect::<Vec<&str>>(),
                vec![
                    "cpu-logical-cores",
                    "cpu-physical-cores",
                    "cpu-frequency",
                    "cpu-vendor-id",
                    "cpu-arch",
                    "l1-cache-size",
                    "l1-cache-line-size",
                    "l2-cache-size",
                    "l2-cache-line-size",
                    "l3-cache-size",
                    "l3-cache-line-size",
                ]
            );
        }
        let mem_info = collector.iter().find(|x| x.get_tp() == "memory").unwrap();
        assert_eq!(
            mem_info
                .get_pairs()
                .iter()
                .map(|x| x.get_key())
                .collect::<Vec<&str>>(),
            vec!["capacity",]
        );
        // disk
        let disk_info = collector.iter().find(|x| x.get_tp() == "disk").unwrap();
        assert_eq!(
            disk_info
                .get_pairs()
                .iter()
                .map(|x| x.get_key())
                .collect::<Vec<&str>>(),
            vec![
                "type",
                "fstype",
                "path",
                "total",
                "free",
                "used",
                "free-percent",
                "used-percent",
            ]
        );
        // nic
        let nic_info = collector.iter().find(|x| x.get_tp() == "net").unwrap();
        assert_eq!(
            nic_info
                .get_pairs()
                .iter()
                .map(|x| x.get_key())
                .filter(|x| *x != "ip") // different environment contains different IPs
                .collect::<Vec<&str>>(),
            vec![
                "mac",
                "flag",
                "index",
                "is-up",
                "is-broadcast",
                "is-multicast",
                "is-loopback",
                "is-point-to-point",
            ]
        );
    }
}
