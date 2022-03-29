// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use clap::{App, Arg};
use hyper::service::{make_service_fn, service_fn};
use slog_global::{error, info};
use std::path::PathBuf;
use std::sync::Arc;

fn main() {
    init_logger();
    let matches = App::new("tikv-compactor")
        .about("tikv remote compactor")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true)
                .required(true),
        )
        .get_matches();
    let config_path = matches.value_of_os("config").unwrap();
    let result = std::fs::read(PathBuf::from(config_path));
    if result.is_err() {
        error!("failed to read config file {:?}", result.unwrap_err());
        return;
    }
    let data = result.unwrap();
    let mut config: Config = toml::from_slice(&data).unwrap();
    if config.port == 0 {
        config.port = 19000;
    }
    if config.tenant_id == 0 {
        config.tenant_id = 1;
    }
    if config.local_dir == "" {
        config.local_dir = "/tmp".to_string();
    }
    info!("config is {:?}", &config);
    let dfs = Arc::new(kvengine::dfs::S3FS::new(
        config.tenant_id,
        PathBuf::from(config.local_dir),
        config.s3_endpoint,
        config.s3_key_id,
        config.s3_secret_key,
        config.s3_region,
        config.s3_bucket,
    ));

    let thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .thread_name("compaction-server")
        .build()
        .unwrap();
    let addr = ([0, 0, 0, 0], config.port).into();

    let incoming = {
        let _enter = thread_pool.enter();
        hyper::server::conn::AddrIncoming::bind(&addr)
    }
    .unwrap();
    let server_builder = hyper::Server::builder(incoming);
    let server = server_builder.serve(make_service_fn(move |_| {
        let dfs = dfs.clone();
        async move {
            // Create a status service.
            Ok::<_, hyper::Error>(service_fn(move |req: hyper::Request<hyper::Body>| {
                let dfs = dfs.clone();
                async move { kvengine::handle_remote_compaction(dfs, req).await }
            }))
        }
    }));
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    thread_pool.spawn(async move {
        let res = server.await;
        tx.send(res).unwrap();
    });
    rx.recv().unwrap().unwrap();
}

fn init_logger() {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    slog_global::set_global(logger);
}

#[macro_use]
extern crate serde_derive;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub port: u16,
    pub tenant_id: u32,
    pub s3_endpoint: String,
    pub s3_key_id: String,
    pub s3_secret_key: String,
    pub s3_bucket: String,
    pub s3_region: String,
    pub local_dir: String,
}
