// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Future, Sink, Stream};
use futures_cpupool::CpuPool;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags};
use kvproto::diagnosticspb::{
    Diagnostics, SearchLogRequest, SearchLogResponse, ServerInfoRequest, ServerInfoResponse,
    ServerInfoType,
};

#[cfg(feature = "prost-codec")]
use kvproto::diagnosticspb::search_log_request::Target as SearchLogRequestTarget;
#[cfg(not(feature = "prost-codec"))]
use kvproto::diagnosticspb::SearchLogRequestTarget;

use tikv_util::security::{check_common_name, SecurityManager};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use crate::server::{Error, Result};

/// Service handles the RPC messages for the `Diagnostics` service.
#[derive(Clone)]
pub struct Service {
    pool: CpuPool,
    log_file: String,
    slow_log_file: String,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    pub fn new(
        pool: CpuPool,
        log_file: String,
        slow_log_file: String,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Service {
            pool,
            log_file,
            slow_log_file,
            security_mgr,
        }
    }
}

impl Diagnostics for Service {
    fn search_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: SearchLogRequest,
        sink: ServerStreamingSink<SearchLogResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let log_file = if req.get_target() == SearchLogRequestTarget::Normal {
            self.log_file.to_owned()
        } else {
            self.slow_log_file.to_owned()
        };
        let stream = self
            .pool
            .spawn_fn(move || log::search(log_file, req))
            .map(|stream| {
                stream
                    .map(|resp| (resp, WriteFlags::default().buffer_hint(true)))
                    .map_err(|e| {
                        grpcio::Error::RpcFailure(RpcStatus::new(
                            RpcStatusCode::UNKNOWN,
                            Some(format!("{:?}", e)),
                        ))
                    })
            })
            .map_err(|e| {
                grpcio::Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::UNKNOWN,
                    Some(format!("{:?}", e)),
                ))
            });
        let future = self.pool.spawn(
            stream
                .and_then(|stream| sink.send_all(stream))
                .map(|_| ())
                .map_err(|e| {
                    error!("search log RPC error"; "error" => ?e);
                }),
        );
        ctx.spawn(future);
    }

    fn server_info(
        &mut self,
        ctx: RpcContext<'_>,
        req: ServerInfoRequest,
        sink: UnarySink<ServerInfoResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let tp = req.get_tp();
        let collect = self
            .pool
            .spawn_fn(move || {
                let s = match tp {
                    ServerInfoType::LoadInfo | ServerInfoType::All => {
                        let load = (
                            sys::cpu_time_snapshot(),
                            sysinfo::NICLoad::snapshot(),
                            sysinfo::IOLoad::snapshot(),
                        );
                        let when = Instant::now() + Duration::from_millis(1000);
                        (Some(load), when)
                    }
                    _ => (None, Instant::now()),
                };
                Ok(s)
            })
            .and_then(|(load, when)| {
                let timer = GLOBAL_TIMER_HANDLE.clone();
                timer.delay(when).then(|_| Ok(load))
            })
            .and_then(move |load| -> Result<ServerInfoResponse> {
                let mut server_infos = Vec::new();
                match req.get_tp() {
                    ServerInfoType::HardwareInfo => sys::hardware_info(&mut server_infos),
                    ServerInfoType::LoadInfo => sys::load_info(load.unwrap(), &mut server_infos),
                    ServerInfoType::SystemInfo => sys::system_info(&mut server_infos),
                    ServerInfoType::All => {
                        sys::hardware_info(&mut server_infos);
                        sys::load_info(load.unwrap(), &mut server_infos);
                        sys::system_info(&mut server_infos);
                    }
                };
                // Sort pairs by key to make result stable
                server_infos
                    .sort_by(|a, b| (a.get_tp(), a.get_name()).cmp(&(b.get_tp(), b.get_name())));
                let mut resp = ServerInfoResponse::default();
                resp.set_items(server_infos.into());
                Ok(resp)
            });
        let f = self
            .pool
            .spawn(collect)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map_err(|e| debug!("Diagnostics rpc failed"; "err" => ?e));
        ctx.spawn(f);
    }
}

mod sys {
    use std::collections::HashMap;
    use std::string::ToString;

    use kvproto::diagnosticspb::{ServerInfoItem, ServerInfoPair};
    use sysinfo::{DiskExt, ProcessExt, SystemExt};
    use tikv_util::config::KB;
    use tikv_util::sys::cpu_time::LiunxStyleCpuTime;
    use tikv_util::sys::sys_quota::SysQuota;
    use walkdir::WalkDir;

    type CpuTimeSnapshot = Option<LiunxStyleCpuTime>;

    fn cpu_load_info(prev_cpu: CpuTimeSnapshot, collector: &mut Vec<ServerInfoItem>) {
        // CPU load
        {
            let load = sysinfo::get_avg_load();
            let infos = vec![
                ("load1", load.one),
                ("load5", load.five),
                ("load15", load.fifteen),
            ];
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
        let mut system = sysinfo::System::new();
        system.refresh_all();
        let total_memory = SysQuota::new().memory_limit_in_bytes();
        let used_memory = system.get_used_memory() * KB;
        let free_memory = system.get_free_memory() * KB;
        let total_swap = system.get_total_swap() * KB;
        let used_swap = system.get_used_swap() * KB;
        let free_swap = system.get_free_swap() * KB;
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

    fn nic_load_info(
        prev_nic: HashMap<String, sysinfo::NICLoad>,
        collector: &mut Vec<ServerInfoItem>,
    ) {
        let current = sysinfo::NICLoad::snapshot();
        let rate = |cur, prev| (cur - prev) as f64;
        for (name, cur) in current.into_iter() {
            let prev = match prev_nic.get(&name) {
                Some(p) => p,
                None => continue,
            };
            let infos = vec![
                ("rx-bytes/s", rate(cur.rx_bytes, prev.rx_bytes)),
                ("tx-bytes/s", rate(cur.tx_bytes, prev.tx_bytes)),
                ("rx-packets/s", rate(cur.rx_packets, prev.rx_packets)),
                ("tx-packets/s", rate(cur.tx_packets, prev.tx_packets)),
                ("rx-errors/s", rate(cur.rx_errors, prev.rx_errors)),
                ("tx-errors/s", rate(cur.tx_errors, prev.tx_errors)),
                ("rx-comp/s", rate(cur.rx_compressed, prev.rx_compressed)),
                ("tx-comp/s", rate(cur.tx_compressed, prev.tx_compressed)),
            ];
            let mut pairs = vec![];
            for info in infos.into_iter() {
                let mut pair = ServerInfoPair::default();
                pair.set_key(info.0.to_string());
                pair.set_value(format!("{:.2}", info.1));
                pairs.push(pair);
            }
            let mut item = ServerInfoItem::default();
            item.set_tp("net".to_string());
            item.set_name(name);
            item.set_pairs(pairs.into());
            collector.push(item);
        }
    }

    fn io_load_info(
        prev_io: HashMap<String, sysinfo::IOLoad>,
        collector: &mut Vec<ServerInfoItem>,
    ) {
        let current = sysinfo::IOLoad::snapshot();
        let rate = |cur, prev| (cur - prev) as f64;
        for (name, cur) in current.into_iter() {
            let prev = match prev_io.get(&name) {
                Some(p) => p,
                None => continue,
            };
            let infos = vec![
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
            HashMap<String, sysinfo::NICLoad>,
            HashMap<String, sysinfo::IOLoad>,
        ),
        collector: &mut Vec<ServerInfoItem>,
    ) {
        cpu_load_info(prev_cpu, collector);
        mem_load_info(collector);
        nic_load_info(prev_nic, collector);
        io_load_info(prev_io, collector);
    }

    fn cpu_hardware_info(collector: &mut Vec<ServerInfoItem>) {
        let mut system = sysinfo::System::new();
        system.refresh_all();
        let mut infos = vec![
            (
                "cpu-logical-cores",
                SysQuota::new().cpu_cores_quota().to_string(),
            ),
            ("cpu-physical-cores", num_cpus::get_physical().to_string()),
            (
                "cpu-frequency",
                format!("{}MHz", sysinfo::get_cpu_frequency()),
            ),
        ];
        // cache
        let caches = vec![
            ("l1-cache-size", cache_size::l1_cache_size()),
            ("l1-cache-line-size", cache_size::l1_cache_line_size()),
            ("l2-cache-size", cache_size::l2_cache_size()),
            ("l2-cache-line-size", cache_size::l2_cache_line_size()),
            ("l3-cache-size", cache_size::l3_cache_size()),
            ("l3-cache-line-size", cache_size::l3_cache_line_size()),
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
        let mut system = sysinfo::System::new();
        system.refresh_all();
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
        let mut system = sysinfo::System::new();
        system.refresh_all();
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
                        .unwrap_or_else(|_| "unkonwn")
                        .to_string(),
                ),
                (
                    "path",
                    disk.get_mount_point()
                        .to_str()
                        .unwrap_or_else(|| "unknown")
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
            item.set_name(
                disk.get_name()
                    .to_str()
                    .unwrap_or_else(|| "disk")
                    .to_string(),
            );
            item.set_pairs(pairs.into());
            collector.push(item);
        }
    }

    fn nic_hardware_info(collector: &mut Vec<ServerInfoItem>) {
        let nics = pnet_datalink::interfaces();
        for nic in nics.into_iter() {
            let mut infos = vec![
                ("mac", nic.mac_address().to_string()),
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

    /// process_info collects all process list
    /// TODO: use different `ServerInfoType` to collect process list
    #[allow(dead_code)]
    pub fn process_info(collector: &mut Vec<ServerInfoItem>) {
        let mut system = sysinfo::System::new();
        system.refresh_all();
        let processes = system.get_process_list();
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
            let prev_nic = sysinfo::NICLoad::snapshot();
            let prev_io = sysinfo::IOLoad::snapshot();
            let mut collector = vec![];
            load_info((prev_cpu, prev_nic, prev_io), &mut collector);
            #[cfg(linux)]
            let tps = vec!["cpu", "memory", "net", "io"];
            #[cfg(not(linux))]
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
            #[cfg(linux)]
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
            #[cfg(linux)]
            {
                let item = collector
                    .filter(|x| x.get_tp() == "system" && x.get_name() == "sysctl")
                    .unwrap();
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
                    "l1-cache-size",
                    "l1-cache-line-size",
                    "l2-cache-size",
                    "l2-cache-line-size",
                    "l3-cache-size",
                    "l3-cache-line-size",
                ]
            );
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
}

mod log {
    use std::convert::From;
    use std::fs::{read_dir, File};
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    use std::path::Path;

    use chrono::DateTime;
    use futures::stream::{iter_ok, Stream};
    use itertools::Itertools;
    use kvproto::diagnosticspb::{LogLevel, LogMessage, SearchLogRequest, SearchLogResponse};
    use lazy_static::lazy_static;
    use nom::bytes::complete::{tag, take};
    use nom::character::complete::{alpha1, space0, space1};
    use nom::sequence::tuple;
    use nom::*;
    use regex::Regex;
    use rev_lines;

    const INVALID_TIMESTAMP: i64 = -1;
    const TIMESTAMP_LENGTH: usize = 30;

    #[derive(Default)]
    struct LogIterator {
        search_files: Vec<(i64, File)>,
        currrent_lines: Option<std::io::Lines<BufReader<File>>>,

        // filter conditions
        begin_time: i64,
        end_time: i64,
        level_flag: usize,
        patterns: Vec<regex::Regex>,
    }

    #[derive(Debug)]
    pub enum Error {
        InvalidRequest(String),
        ParseError(String),
        SearchError(String),
        IOError(std::io::Error),
    }

    impl From<std::io::Error> for Error {
        fn from(err: std::io::Error) -> Self {
            Error::IOError(err)
        }
    }

    impl LogIterator {
        fn new<P: AsRef<Path>>(
            log_file: P,
            begin_time: i64,
            end_time: i64,
            level_flag: usize,
            patterns: Vec<regex::Regex>,
        ) -> Result<Self, Error> {
            let end_time = if end_time > 0 {
                end_time
            } else {
                std::i64::MAX
            };
            let log_path = log_file.as_ref();
            let log_name = match log_path.file_name() {
                Some(file_name) => match file_name.to_str() {
                    Some(file_name) => file_name,
                    None => {
                        return Err(Error::SearchError(format!("Invalid utf8: {:?}", file_name)))
                    }
                },
                None => {
                    return Err(Error::SearchError(format!(
                        "Illegal file name: {:?}",
                        log_path
                    )))
                }
            };
            if log_name.is_empty() {
                return Err(Error::InvalidRequest("Empty `log_file` path".to_owned()));
            }
            let log_dir = match log_path.parent() {
                Some(dir) => dir,
                None => {
                    return Err(Error::SearchError(format!(
                        "Illegal parent dir: {:?}",
                        log_path
                    )))
                }
            };

            let mut search_files = vec![];
            for entry in read_dir(log_dir)? {
                let entry = entry?;
                if !entry.path().is_file() {
                    continue;
                }
                let file_name = entry.file_name();
                let file_name = match file_name.to_str() {
                    Some(file_name) => file_name,
                    None => continue,
                };
                // Rotated file name have the same prefix with the original
                if !is_log_file(file_name, log_name) {
                    continue;
                }
                // Open the file
                let mut file = match File::open(entry.path()) {
                    Ok(file) => file,
                    Err(_) => continue,
                };

                let (file_start_time, file_end_time) = match parse_time_range(&file) {
                    Ok((file_start_time, file_end_time)) => (file_start_time, file_end_time),
                    Err(_) => continue,
                };
                if begin_time > file_end_time || end_time < file_start_time {
                    continue;
                }
                if let Err(err) = file.seek(SeekFrom::Start(0)) {
                    warn!("seek file failed: {}, err: {}", file_name, err);
                    continue;
                }
                search_files.push((file_start_time, file));
            }
            // Sort by start time descending
            search_files.sort_by(|a, b| b.0.cmp(&a.0));
            let current_reader = search_files.pop().map(|file| BufReader::new(file.1));
            Ok(Self {
                search_files,
                currrent_lines: current_reader.map(|reader| reader.lines()),
                begin_time,
                end_time,
                level_flag,
                patterns,
            })
        }
    }

    impl Iterator for LogIterator {
        type Item = LogMessage;
        fn next(&mut self) -> Option<Self::Item> {
            while let Some(lines) = &mut self.currrent_lines {
                loop {
                    let line = match lines.next() {
                        Some(line) => line,
                        None => {
                            self.currrent_lines = self
                                .search_files
                                .pop()
                                .map(|file| BufReader::new(file.1))
                                .map(|reader| reader.lines());
                            break;
                        }
                    };
                    let input = match line {
                        Ok(input) => input,
                        Err(err) => {
                            warn!("read line failed: {:?}", err);
                            continue;
                        }
                    };
                    if input.len() < TIMESTAMP_LENGTH {
                        continue;
                    }
                    match parse(&input) {
                        Ok((content, (time, level))) => {
                            // The remain content timestamp more the end time or this file contains inrecognation formation
                            if time == INVALID_TIMESTAMP || time > self.end_time {
                                break;
                            }
                            if time < self.begin_time {
                                continue;
                            }
                            if self.level_flag != 0
                                && self.level_flag & (1 << (level as usize)) == 0
                            {
                                continue;
                            }
                            if !self.patterns.is_empty() {
                                let mut not_match = false;
                                for pattern in self.patterns.iter() {
                                    if !pattern.is_match(content) {
                                        not_match = true;
                                        break;
                                    }
                                }
                                if not_match {
                                    continue;
                                }
                            }
                            let mut item = LogMessage::default();
                            item.set_time(time);
                            item.set_level(level);
                            item.set_message(content.to_owned());
                            return Some(item);
                        }
                        Err(err) => {
                            warn!("parse line failed: {:?}", err);
                            continue;
                        }
                    }
                }
            }
            None
        }
    }

    lazy_static! {
        static ref NUM_REGEX: Regex = Regex::new(r"^\d{4}").unwrap();
    }

    // Returns true if target 'filename' is part of given 'log_file'
    fn is_log_file(filename: &str, log_file: &str) -> bool {
        // for not rotated nomral file
        if filename == log_file {
            return true;
        }

        // for rotated *.<rotated-datetime> file
        if let Some(res) = filename.strip_prefix((log_file.to_owned() + ".").as_str()) {
            if NUM_REGEX.is_match(res) {
                return true;
            }
        }
        false
    }

    fn parse_time(input: &str) -> IResult<&str, &str> {
        let (input, (_, _, time, _)) =
            tuple((space0, tag("["), take(TIMESTAMP_LENGTH), tag("]")))(input)?;
        Ok((input, time))
    }

    fn parse_level(input: &str) -> IResult<&str, &str> {
        let (input, (_, _, level, _)) = tuple((space0, tag("["), alpha1, tag("]")))(input)?;
        Ok((input, level))
    }

    /// Parses the single log line and retrieve the log meta and log body.
    fn parse(input: &str) -> IResult<&str, (i64, LogLevel)> {
        let (content, (time, level, _)) = tuple((parse_time, parse_level, space1))(input)?;
        let timestamp = match DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z") {
            Ok(t) => t.timestamp_millis(),
            Err(_) => INVALID_TIMESTAMP,
        };
        let level = match level {
            "trace" | "TRACE" => LogLevel::Trace,
            "debug" | "DEBUG" => LogLevel::Debug,
            "info" | "INFO" => LogLevel::Info,
            "warn" | "WARN" | "warning" | "WARNING" => LogLevel::Warn,
            "error" | "ERROR" => LogLevel::Error,
            "critical" | "CRITICAL" => LogLevel::Critical,
            _ => LogLevel::Unknown,
        };
        Ok((content, (timestamp, level)))
    }

    /// Parses the start time and end time of a log file and return the maximal and minimal
    /// timestamp in unix milliseconds.
    fn parse_time_range(file: &std::fs::File) -> Result<(i64, i64), Error> {
        let buffer = BufReader::new(file);
        let file_start_time = match buffer.lines().nth(0) {
            Some(Ok(line)) => {
                let (_, (time, _)) = parse(&line)
                    .map_err(|err| Error::ParseError(format!("Parse error: {:?}", err)))?;
                time
            }
            Some(Err(err)) => {
                return Err(err.into());
            }
            None => INVALID_TIMESTAMP,
        };

        let buffer = BufReader::new(file);
        let mut rev_lines = rev_lines::RevLines::with_capacity(512, buffer)?;
        let file_end_time = match rev_lines.nth(0) {
            Some(line) => {
                let (_, (time, _)) = parse(&line)
                    .map_err(|err| Error::ParseError(format!("Parse error: {:?}", err)))?;
                time
            }
            None => INVALID_TIMESTAMP,
        };
        Ok((file_start_time, file_end_time))
    }

    // Batch size of the log streaming
    const LOG_ITEM_BATCH_SIZE: usize = 256;

    fn bacth_log_item(item: LogIterator) -> impl Stream<Item = SearchLogResponse, Error = ()> {
        iter_ok(item.batching(|iter| {
            let batch = iter.take(LOG_ITEM_BATCH_SIZE).collect_vec();
            if batch.is_empty() {
                None
            } else {
                let mut resp = SearchLogResponse::default();
                resp.set_messages(batch.into());
                Some(resp)
            }
        }))
    }

    pub fn search<P: AsRef<Path>>(
        log_file: P,
        mut req: SearchLogRequest,
    ) -> Result<impl Stream<Item = SearchLogResponse, Error = ()>, Error> {
        if !log_file.as_ref().exists() {
            return Ok(bacth_log_item(LogIterator::default()));
        }
        let begin_time = req.get_start_time();
        let end_time = req.get_end_time();
        let levels = req.take_levels();
        let mut patterns = vec![];
        for pattern in req.take_patterns().iter() {
            let pattern = regex::Regex::new(pattern).map_err(|e| {
                Error::InvalidRequest(format!("illegal regular expression: {:?}", e))
            })?;
            patterns.push(pattern);
        }
        let level_flag = levels
            .into_iter()
            .fold(0, |acc, x| acc | (1 << (x as usize)));
        let item = LogIterator::new(log_file, begin_time, end_time, level_flag, patterns)?;
        Ok(bacth_log_item(item))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;
        use tempfile::tempdir;

        #[test]
        fn test_parse_time() {
            // (input, remain, time)
            let cs = vec![
                (
                    "[2019/08/23 18:09:52.387 +08:00]",
                    "",
                    "2019/08/23 18:09:52.387 +08:00",
                ),
                (
                    " [2019/08/23 18:09:52.387 +08:00] [",
                    " [",
                    "2019/08/23 18:09:52.387 +08:00",
                ),
            ];
            for (input, remain, time) in cs {
                let result = parse_time(input);
                assert_eq!(result.unwrap(), (remain, time));
            }
        }

        #[test]
        fn test_parse_level() {
            // (input, remain, level_str)
            let cs = vec![("[INFO]", "", "INFO"), (" [WARN] [", " [", "WARN")];
            for (input, remain, level_str) in cs {
                let result = parse_level(input);
                assert_eq!(result.unwrap(), (remain, level_str));
            }
        }

        fn timestamp(time: &str) -> i64 {
            match DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z") {
                Ok(t) => t.timestamp_millis(),
                Err(_) => INVALID_TIMESTAMP,
            }
        }

        #[test]
        fn test_parse() {
            // (input, time, level, message)
            let cs: Vec<(&str, &str, LogLevel, &str)> = vec![
                (
                    "[2019/08/23 18:09:52.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Info,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00]           [info] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Info,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Warn,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [WARNING] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Warn,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [warn] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Warn,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Warn,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Debug,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00]    [debug] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Debug,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00]    [ERROR] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Error,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [error] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Error,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Critical,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [critical] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Critical,
                    "[foo.rs:100] [some message] [key=val]",
                ),

                (
                    "[2019/08/23 18:09:52.387 +08:00] [TRACE] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Trace,
                    "[foo.rs:100] [some message] [key=val]",
                ),
                (
                    "[2019/08/23 18:09:52.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]",
                    "2019/08/23 18:09:52.387 +08:00",
                    LogLevel::Trace,
                    "[foo.rs:100] [some message] [key=val]",
                ),
            ];
            for (input, time, level, content) in cs.into_iter() {
                let result = parse(input);
                assert!(result.is_ok(), "expected OK, but got: {:?}", result);
                let timestamp = timestamp(time);
                let log = result.unwrap();
                assert_eq!(log.0, content);
                assert_eq!((log.1).0, timestamp);
                assert_eq!((log.1).1, level);
            }
        }

        #[test]
        fn test_parse_time_range() {
            let dir = tempdir().unwrap();
            let log_file = dir.path().join("tikv.log");
            let mut file = File::create(&log_file).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:09:52.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]"#
            )
            .unwrap();

            let file = File::open(&log_file).unwrap();
            let start_time = timestamp("2019/08/23 18:09:52.387 +08:00");
            let end_time = timestamp("2019/08/23 18:09:58.387 +08:00");
            assert_eq!(parse_time_range(&file).unwrap(), (start_time, end_time));
        }

        #[test]
        fn test_log_iterator() {
            let dir = tempdir().unwrap();
            let log_file = dir.path().join("tikv.log");
            let mut file = File::create(&log_file).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
            )
            .unwrap();

            let log_file2 = dir.path().join("tikv.log.2019-08-23-18:10:00.387000");
            let mut file = File::create(&log_file2).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val]"#
            )
            .unwrap();

            // We use the timestamp as the identity of log item in following test cases
            // all content
            let log_iter = LogIterator::new(&log_file, 0, std::i64::MAX, 0, vec![]).unwrap();
            let expected = vec![
                "2019/08/23 18:09:53.387 +08:00",
                "2019/08/23 18:09:54.387 +08:00",
                "2019/08/23 18:09:55.387 +08:00",
                "2019/08/23 18:09:56.387 +08:00",
                "2019/08/23 18:09:57.387 +08:00",
                "2019/08/23 18:09:58.387 +08:00",
                "2019/08/23 18:09:59.387 +08:00",
                "2019/08/23 18:10:01.387 +08:00",
                "2019/08/23 18:10:02.387 +08:00",
                "2019/08/23 18:10:03.387 +08:00",
                "2019/08/23 18:10:04.387 +08:00",
                "2019/08/23 18:10:05.387 +08:00",
                "2019/08/23 18:10:06.387 +08:00",
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );

            // filter by time range
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:56.387 +08:00"),
                timestamp("2019/08/23 18:10:03.387 +08:00"),
                0,
                vec![],
            )
            .unwrap();
            let expected = vec![
                "2019/08/23 18:09:56.387 +08:00",
                "2019/08/23 18:09:57.387 +08:00",
                "2019/08/23 18:09:58.387 +08:00",
                "2019/08/23 18:09:59.387 +08:00",
                "2019/08/23 18:10:01.387 +08:00",
                "2019/08/23 18:10:02.387 +08:00",
                "2019/08/23 18:10:03.387 +08:00",
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:56.387 +08:00"),
                timestamp("2019/08/23 18:09:58.387 +08:00"),
                0,
                vec![],
            )
            .unwrap();
            let expected = vec![
                "2019/08/23 18:09:56.387 +08:00",
                "2019/08/23 18:09:57.387 +08:00",
                "2019/08/23 18:09:58.387 +08:00",
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );

            // filter by log level
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:53.387 +08:00"),
                timestamp("2019/08/23 18:09:58.387 +08:00"),
                1 << (LogLevel::Info as usize),
                vec![],
            )
            .unwrap();

            let expected = vec!["2019/08/23 18:09:53.387 +08:00"]
                .iter()
                .map(|s| timestamp(s))
                .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );

            for time in vec![0, std::i64::MAX].into_iter() {
                let log_iter = LogIterator::new(
                    &log_file,
                    timestamp("2019/08/23 18:09:53.387 +08:00"),
                    time,
                    1 << (LogLevel::Warn as usize),
                    vec![],
                )
                .unwrap();
                let expected = vec![
                    "2019/08/23 18:09:58.387 +08:00",
                    "2019/08/23 18:09:59.387 +08:00",
                    "2019/08/23 18:10:06.387 +08:00",
                ]
                .iter()
                .map(|s| timestamp(s))
                .collect::<Vec<i64>>();
                assert_eq!(
                    log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                    expected
                );
            }

            // filter by pattern
            let log_iter = LogIterator::new(
                &log_file,
                timestamp("2019/08/23 18:09:54.387 +08:00"),
                std::i64::MAX,
                1 << (LogLevel::Warn as usize),
                vec![regex::Regex::new(".*test-filter.*").unwrap()],
            )
            .unwrap();
            let expected = vec!["2019/08/23 18:09:58.387 +08:00"]
                .iter()
                .map(|s| timestamp(s))
                .collect::<Vec<i64>>();
            assert_eq!(
                log_iter.map(|m| m.get_time()).collect::<Vec<i64>>(),
                expected
            );
        }

        #[test]
        fn test_search() {
            let dir = tempdir().unwrap();
            let log_file = dir.path().join("tikv.log");
            let mut file = File::create(&log_file).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:09:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:09:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:09:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
            )
            .unwrap();

            let log_file2 = dir.path().join("tikv.log.2");
            let mut file = File::create(&log_file2).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:10:01.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:02.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:03.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:04.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:05.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:10:06.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter"#
            )
            .unwrap();

            let log_file3 = dir.path().join("tikv.log.2019-08-23-18:11:02.123456789");
            let mut file = File::create(&log_file3).unwrap();
            write!(
                file,
                r#"[2019/08/23 18:11:53.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:54.387 +08:00] [trace] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:55.387 +08:00] [DEBUG] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:56.387 +08:00] [ERROR] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:57.387 +08:00] [CRITICAL] [foo.rs:100] [some message] [key=val]
[2019/08/23 18:11:58.387 +08:00] [WARN] [foo.rs:100] [some message] [key=val] - test-filter
[2019/08/23 18:11:59.387 +08:00] [warning] [foo.rs:100] [some message] [key=val]"#
            )
            .unwrap();

            let mut req = SearchLogRequest::default();
            req.set_start_time(timestamp("2019/08/23 18:09:54.387 +08:00"));
            req.set_end_time(std::i64::MAX);
            req.set_levels(vec![LogLevel::Warn.into()].into());
            req.set_patterns(vec![".*test-filter.*".to_string()].into());
            let expected = vec![
                "2019/08/23 18:09:58.387 +08:00",
                "2019/08/23 18:11:58.387 +08:00",
            ]
            .iter()
            .map(|s| timestamp(s))
            .collect::<Vec<i64>>();
            assert_eq!(
                expected,
                search(log_file, req)
                    .unwrap()
                    .wait()
                    .map(|x| x.unwrap())
                    .collect::<Vec<SearchLogResponse>>()
                    .into_iter()
                    .map(|mut resp| resp.take_messages().into_iter())
                    .into_iter()
                    .flatten()
                    .map(|msg| msg.get_time())
                    .collect::<Vec<i64>>()
            );
        }
    }
}
