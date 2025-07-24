// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(result_flattening)]

mod metrics;

use crate::metrics::RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM;

use std::{
    ffi::CString,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use std::hash::BuildHasherDefault;

use collections::HashMap;
use grpcio::{
    Channel,
    ChannelBuilder, CompressionAlgorithms, Environment,
};
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use security::SecurityManager;
use tokio::sync::Mutex;
use tikv_util::time::Instant;

pub struct TikvClientsMgr {
    tikv_clients: Mutex<HashMap<u64, Channel>>,
    pd_client: Arc<dyn PdClient>,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
}

static CONN_ID: AtomicI32 = AtomicI32::new(0);
const DEFAULT_GRPC_GZIP_COMPRESSION_LEVEL: usize = 2;
const DEFAULT_GRPC_MIN_MESSAGE_SIZE_TO_COMPRESS: usize = 4096;

impl TikvClientsMgr {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> TikvClientsMgr {
        TikvClientsMgr {
            tikv_clients: Mutex::default(),
            pd_client,
            env,
            security_mgr,
        }
    }

    pub async fn remove(&mut self, store_id: u64) {
        self.tikv_clients.lock().await.remove(&store_id);
    }

    async fn get_channel(
        &mut self,
        store_id: u64,
        timeout: Duration,
    ) -> pd_client::Result<Channel> {
        {
            let clients = self.tikv_clients.lock().await;
            if let Some(client) = clients.get(&store_id).cloned() {
                return Ok(client);
            }
        }
        let store = tokio::time::timeout(timeout, self.pd_client.get_store_async(store_id))
            .await
            .map_err(|e| pd_client::Error::Other(Box::new(e)))
            .flatten()?;
        let mut clients = self.tikv_clients.lock().await;
        let start = Instant::now_coarse();
        // hack: so it's different args, grpc will always create a new connection.
        // the check leader requests may be large but not frequent, compress it to
        // reduce the traffic.
        let cb = ChannelBuilder::new(self.env.clone())
            .raw_cfg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            )
            .default_compression_algorithm(CompressionAlgorithms::GRPC_COMPRESS_GZIP)
            .default_gzip_compression_level(DEFAULT_GRPC_GZIP_COMPRESSION_LEVEL)
            .default_grpc_min_message_size_to_compress(DEFAULT_GRPC_MIN_MESSAGE_SIZE_TO_COMPRESS);
    
        let channel = self.security_mgr.connect(cb, &store.peer_address);
        clients.insert(store_id, channel.clone());
        RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
        Ok(channel)
    }

    pub async fn get_tikv_client(
        &mut self,
        store_id: u64,
        timeout: Duration,
    ) -> pd_client::Result<TikvClient> {
        let channel = self.get_channel(store_id, timeout).await?;
        let cli = TikvClient::new(channel);
        Ok(cli)
    }
}

struct MockPdClient {}
impl PdClient for MockPdClient {}

impl Default for TikvClientsMgr {
    fn default() -> Self {
        TikvClientsMgr {
            tikv_clients: Mutex::new(HashMap::with_hasher(BuildHasherDefault::default())),
            pd_client: Arc::new(MockPdClient {}),
            env: Arc::new(Environment::new(1)),
            security_mgr: Arc::new(SecurityManager::new(&Default::default()).unwrap()),
        }
    }
}