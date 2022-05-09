// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use api_version::{ApiV2, KeyMode, KvFormat};
use engine_traits::KvEngine;
use kvproto::raft_cmdpb::{CmdType, Request as RaftRequest};
use raft::StateRole;
use raftstore::{
    coprocessor,
    coprocessor::{
        BoxQueryObserver, BoxRoleObserver, Coprocessor, CoprocessorHost, ObserverContext,
        QueryObserver, RoleChange, RoleObserver,
    },
};

use crate::CausalTsProvider;

/// CausalObserver appends timestamp for RawKV V2 data,
/// and invoke causal_ts_provider.flush() on specified event, e.g. leader transfer, snapshot apply.
/// Should be used ONLY when API v2 is enabled.
pub struct CausalObserver<Ts: CausalTsProvider> {
    causal_ts_provider: Arc<Ts>,
}

impl<Ts: CausalTsProvider> Clone for CausalObserver<Ts> {
    fn clone(&self) -> Self {
        Self {
            causal_ts_provider: self.causal_ts_provider.clone(),
        }
    }
}

// Causal observer's priority should be higher than all other observers, to avoid being bypassed.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;

impl<Ts: CausalTsProvider + 'static> CausalObserver<Ts> {
    pub fn new(causal_ts_provider: Arc<Ts>) -> Self {
        Self { causal_ts_provider }
    }

    pub fn register_to<E: KvEngine>(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host.registry.register_query_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host
            .registry
            .register_role_observer(CAUSAL_OBSERVER_PRIORITY, BoxRoleObserver::new(self.clone()));
    }
}

impl<Ts: CausalTsProvider> Coprocessor for CausalObserver<Ts> {}

impl<Ts: CausalTsProvider> QueryObserver for CausalObserver<Ts> {
    fn pre_propose_query(
        &self,
        _: &mut ObserverContext<'_>,
        requests: &mut Vec<RaftRequest>,
    ) -> coprocessor::Result<()> {
        let mut ts = None;

        for req in requests.iter_mut().filter(|r| {
            r.get_cmd_type() == CmdType::Put
                && ApiV2::parse_key_mode(r.get_put().get_key()) == KeyMode::Raw
        }) {
            if ts.is_none() {
                ts = Some(self.causal_ts_provider.get_ts().map_err(|err| {
                    coprocessor::Error::Other(box_err!("Get causal timestamp error: {:?}", err))
                })?);
            }

            ApiV2::append_ts_on_encoded_bytes(req.mut_put().mut_key(), ts.unwrap());
        }
        Ok(())
    }
}

impl<Ts: CausalTsProvider> RoleObserver for CausalObserver<Ts> {
    /// Observe becoming leader, to flush CausalTsProvider.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        if role_change.state == StateRole::Leader {
            let region_id = ctx.region().get_id();
            if let Err(err) = self.causal_ts_provider.flush() {
                warn!("CausalObserver::on_role_change, flush timestamp error"; "region" => region_id, "error" => ?err);
            } else {
                debug!("CausalObserver::on_role_change, flush timestamp succeed"; "region" => region_id);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{mem, sync::Arc, time::Duration};

    use api_version::{ApiV2, KvFormat};
    use futures::executor::block_on;
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{RaftCmdRequest, Request as RaftRequest},
    };
    use test_raftstore::TestPdClient;
    use txn_types::{Key, TimeStamp};

    use super::*;
    use crate::BatchTsoProvider;

    fn init() -> CausalObserver<BatchTsoProvider<TestPdClient>> {
        let pd_cli = Arc::new(TestPdClient::new(0, true));
        pd_cli.set_tso(100.into());
        let causal_ts_provider =
            Arc::new(block_on(BatchTsoProvider::new_opt(pd_cli, Duration::ZERO, 100)).unwrap());
        CausalObserver::new(causal_ts_provider)
    }

    #[test]
    fn test_causal_observer() {
        let testcases: Vec<&[&[u8]]> = vec![
            &[b"r\0a", b"r\0b"],
            &[b"r\0c"],
            &[b"r\0d", b"r\0e", b"r\0f"],
        ];

        let ob = init();
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);

        for (i, keys) in testcases.into_iter().enumerate() {
            let mut cmd_req = RaftCmdRequest::default();

            for key in keys {
                let key = ApiV2::encode_raw_key(key, None);
                let value = b"value".to_vec();
                let mut req = RaftRequest::default();
                req.set_cmd_type(CmdType::Put);
                req.mut_put().set_key(key.into_encoded());
                req.mut_put().set_value(value);

                cmd_req.mut_requests().push(req);
            }

            let query = cmd_req.mut_requests();
            let mut vec_query: Vec<RaftRequest> = mem::take(query).into();
            ob.pre_propose_query(&mut ctx, &mut vec_query).unwrap();
            *query = vec_query.into();

            for req in cmd_req.get_requests() {
                let key = Key::from_encoded_slice(req.get_put().get_key());
                let (_, ts) = ApiV2::decode_raw_key_owned(key, true).unwrap();
                assert_eq!(ts, Some(TimeStamp::from(i as u64 + 101)));
            }
        }
    }
}
