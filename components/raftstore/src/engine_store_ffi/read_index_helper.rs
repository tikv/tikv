use crate::router::RaftStoreRouter;
use crate::store::{Callback, RaftRouter, ReadResponse};
use engine_rocks::RocksEngine;
use engine_traits::RaftEngine;
use futures::executor::block_on;
use futures_util::compat::Future01CompatExt;
use kvproto::kvrpcpb::{ReadIndexRequest, ReadIndexResponse};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use std::time::{Duration, Instant};
use tikv_util::future::paired_future_callback;
use txn_types::Key;

use std::future::Future;
use tikv_util::{debug, error};

pub trait ReadIndex: Sync + Send {
    fn batch_read_index(
        &self,
        req: Vec<ReadIndexRequest>,
        timeout: Duration,
    ) -> Vec<(ReadIndexResponse, u64)>;
}

pub struct ReadIndexClient<ER: RaftEngine> {
    pub routers: Vec<std::sync::Mutex<RaftRouter<RocksEngine, ER>>>,
}

impl<ER: RaftEngine> ReadIndexClient<ER> {
    pub fn new(router: RaftRouter<RocksEngine, ER>, cnt: usize) -> Self {
        let mut routers = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            routers.push(std::sync::Mutex::new(router.clone()));
        }
        Self { routers }
    }
}

fn into_read_index_response<S: engine_traits::Snapshot>(
    res: Option<ReadResponse<S>>,
) -> kvproto::kvrpcpb::ReadIndexResponse {
    let mut resp = ReadIndexResponse::default();
    if res.is_none() {
        resp.set_region_error(Default::default());
    } else {
        let mut res = res.unwrap();
        if res.response.get_header().has_error() {
            resp.set_region_error(res.response.mut_header().take_error());
        } else {
            let mut raft_resps = res.response.take_responses();
            if raft_resps.len() != 1 {
                error!(
                    "invalid read index response";
                    "response" => ?raft_resps
                );
                resp.mut_region_error().set_message(format!(
                    "Internal Error: invalid response: {:?}",
                    raft_resps
                ));
            } else {
                let mut read_index_resp = raft_resps[0].take_read_index();
                if read_index_resp.has_locked() {
                    resp.set_locked(read_index_resp.take_locked());
                } else {
                    resp.set_read_index(read_index_resp.get_read_index());
                }
            }
        }
    }
    resp
}

impl<ER: RaftEngine> ReadIndex for ReadIndexClient<ER> {
    fn batch_read_index(
        &self,
        req_vec: Vec<ReadIndexRequest>,
        timeout: Duration,
    ) -> Vec<(ReadIndexResponse, u64)> {
        debug!("batch_read_index start"; "size"=>req_vec.len(), "request"=>?req_vec);
        let mut router_cbs = std::collections::LinkedList::new();
        for req in &req_vec {
            let region_id = req.get_context().get_region_id();
            let mut cmd = RaftCmdRequest::default();
            {
                let mut header = RaftRequestHeader::default();
                let mut inner_req = RaftRequest::default();
                inner_req.set_cmd_type(CmdType::ReadIndex);
                inner_req.mut_read_index().set_start_ts(req.get_start_ts());
                for r in req.get_ranges() {
                    let mut range = kvproto::kvrpcpb::KeyRange::default();
                    range.set_start_key(Key::from_raw(r.get_start_key()).into_encoded());
                    range.set_end_key(Key::from_raw(r.get_end_key()).into_encoded());
                    inner_req.mut_read_index().mut_key_ranges().push(range);
                }
                header.set_region_id(region_id);
                header.set_peer(req.get_context().get_peer().clone());
                header.set_region_epoch(req.get_context().get_region_epoch().clone());
                cmd.set_header(header);
                cmd.set_requests(vec![inner_req].into());
            }

            let (cb, f) = paired_future_callback();

            if let Err(_) = self.routers[region_id as usize % self.routers.len()]
                .lock()
                .unwrap()
                .send_command(cmd, Callback::Read(cb))
            {
                router_cbs.push_back((None, region_id));
            } else {
                router_cbs.push_back((Some(f), region_id));
            }
        }

        let mut read_index_res = Vec::with_capacity(req_vec.len());
        let finished = {
            let read_index_res = &mut read_index_res;
            let read_index_fut = async {
                loop {
                    if let Some((fut, region_id)) = router_cbs.front_mut() {
                        let res = match fut {
                            None => None,
                            Some(fut) => match fut.await {
                                Err(_) => None,
                                Ok(e) => Some(e),
                            },
                        };
                        read_index_res.push((into_read_index_response(res), *region_id));
                        router_cbs.pop_front();
                    } else {
                        break;
                    }
                }
            };

            futures::pin_mut!(read_index_fut);
            let deadline = Instant::now() + timeout;
            let delay = tikv_util::timer::READ_INDEX_TIMER_HANDLE
                .delay(deadline)
                .compat();
            let ret = futures::future::select(read_index_fut, delay);
            match block_on(ret) {
                futures::future::Either::Left(_) => true,
                futures::future::Either::Right(_) => false,
            }
        };
        if !finished {
            let read_index_res = &mut read_index_res;
            loop {
                if let Some((cb, region_id)) = router_cbs.front_mut() {
                    let res = match cb {
                        None => None,
                        Some(fut) => {
                            let waker = futures::task::noop_waker();
                            let cx = &mut std::task::Context::from_waker(&waker);
                            futures::pin_mut!(fut);
                            match fut.poll(cx) {
                                std::task::Poll::Pending => None,
                                std::task::Poll::Ready(e) => match e {
                                    Err(_) => None,
                                    Ok(e) => Some(e),
                                },
                            }
                        }
                    };
                    read_index_res.push((into_read_index_response(res), *region_id));
                    router_cbs.pop_front();
                } else {
                    break;
                }
            }
        }
        assert_eq!(req_vec.len(), read_index_res.len());
        debug!("batch_read_index success"; "response"=>?read_index_res);
        read_index_res
    }
}
