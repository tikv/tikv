use std::{sync::Arc, time::Duration};

use backup_stream::metadata::{
    keys::{KeyValue, MetaKey},
    store::MetaStore,
    ConnectionConfig, LazyEtcdClient,
};
use clap::Parser;
use rand::Rng;
use security::{SecurityConfig, SecurityManager};

#[derive(Clone, Debug, PartialEq, Default, Parser)]
struct Config {
    #[arg(short, long)]
    endpoints: Vec<String>,
    #[arg(long)]
    ca_path: String,
    #[arg(long)]
    cert_path: String,
    #[arg(long)]
    key_path: String,
}

impl From<Config> for SecurityConfig {
    fn from(c: Config) -> Self {
        Self {
            ca_path: c.ca_path,
            cert_path: c.cert_path,
            key_path: c.key_path,
            ..Self::default()
        }
    }
}

fn main() {
    let cfg = Config::parse();
    let security_mgr = Arc::new(
        SecurityManager::new(&cfg.clone().into()).expect("failed to create security manager."),
    );
    println!("using config = {:?}", cfg);
    let ccfg = ConnectionConfig {
        keep_alive_interval: Duration::from_secs(3),
        keep_alive_timeout: Duration::from_secs(10),
        tls: security_mgr.tonic_tls_config(),
    };
    println!("using connection config = {:?}", ccfg);
    let etcd_cli = LazyEtcdClient::new(cfg.endpoints.as_slice(), ccfg);
    let client_id = rand::thread_rng().gen::<u64>();
    println!("using client_id = {}", client_id);
    let t = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kv = KeyValue(
        MetaKey("/hello/world".as_bytes().to_vec()),
        client_id.to_string().into_bytes(),
    );
    println!("setting {:?} to PD", kv);
    t.block_on(etcd_cli.set(kv)).unwrap();
    println!("success!");
}
