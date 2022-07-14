// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use engine_traits::{KvEngine, RaftEngine};
use futures::executor::block_on;
use futures_util::{compat::Future01CompatExt, future::BoxFuture};
use kvproto::{
    kvrpcpb::{ReadIndexRequest, ReadIndexResponse},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest},
};
use tikv_util::{debug, error, future::paired_future_callback};

use super::utils::ArcNotifyWaker;
use crate::{
    router::RaftStoreRouter,
    store::{Callback, RaftCmdExtraOpts, RaftRouter, ReadResponse},
};

pub trait ReadIndex: Sync + Send {
    // To remove
    fn batch_read_index(
        &self,
        req: Vec<ReadIndexRequest>,
        timeout: Duration,
    ) -> Vec<(ReadIndexResponse, u64)>;
    fn make_read_index_task(&self, req: ReadIndexRequest) -> Option<ReadIndexTask>;
    fn poll_read_index_task(
        &self,
        task: &mut ReadIndexTask,
        waker: Option<&ArcNotifyWaker>,
    ) -> Option<ReadIndexResponse>;
}

pub struct ReadIndexClient<ER: RaftEngine, EK: KvEngine> {
    pub routers: Vec<std::sync::Mutex<RaftRouter<EK, ER>>>,
}

impl<ER: RaftEngine, EK: KvEngine> ReadIndexClient<ER, EK> {
    pub fn new(router: RaftRouter<EK, ER>, cnt: usize) -> Self {
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
    if let Some(mut res) = res {
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
    } else {
        resp.set_region_error(Default::default());
    }
    resp
}

fn gen_read_index_raft_cmd_req(req: &mut ReadIndexRequest) -> RaftCmdRequest {
    let region_id = req.get_context().get_region_id();
    let mut cmd = RaftCmdRequest::default();
    {
        let mut header = RaftRequestHeader::default();
        let mut inner_req = RaftRequest::default();
        inner_req.set_cmd_type(CmdType::ReadIndex);
        inner_req.mut_read_index().set_start_ts(req.get_start_ts());
        if !req.get_ranges().is_empty() {
            let r = &mut req.mut_ranges()[0];
            let mut range = kvproto::kvrpcpb::KeyRange::default();
            range.set_start_key(r.take_start_key());
            range.set_end_key(r.take_end_key());
            inner_req.mut_read_index().mut_key_ranges().push(range);
        }
        header.set_region_id(region_id);
        header.set_peer(req.mut_context().take_peer());
        header.set_region_epoch(req.mut_context().take_region_epoch());
        cmd.set_header(header);
        cmd.set_requests(vec![inner_req].into());
    }
    cmd
}

impl<ER: RaftEngine, EK: KvEngine> ReadIndex for ReadIndexClient<ER, EK> {
    fn batch_read_index(
        &self,
        req_vec: Vec<ReadIndexRequest>,
        timeout: Duration,
    ) -> Vec<(ReadIndexResponse, u64)> {
        debug!("batch_read_index start"; "size"=>req_vec.len(), "request"=>?req_vec);
        let mut router_cbs = std::collections::LinkedList::new();
        let req_vec_len = req_vec.len();
        let mut read_index_res = Vec::with_capacity(req_vec_len);

        for mut req in req_vec {
            let region_id = req.get_context().get_region_id();
            let cmd = gen_read_index_raft_cmd_req(&mut req);

            let (cb, f) = paired_future_callback();

            if let Err(e) = self.routers[region_id as usize % self.routers.len()]
                .lock()
                .unwrap()
                .send_command(cmd, Callback::Read(cb), RaftCmdExtraOpts::default())
            {
                tikv_util::info!("make_read_index_task send command error"; "e" => ?e);
                router_cbs.push_back((None, region_id));
            } else {
                router_cbs.push_back((Some(f), region_id));
            }
        }

        let finished = {
            let read_index_res = &mut read_index_res;
            let read_index_fut = async {
                while let Some((fut, region_id)) = router_cbs.front_mut() {
                    let res = match fut {
                        None => None,
                        Some(fut) => match fut.await {
                            Err(_) => None,
                            Ok(e) => Some(e),
                        },
                    };
                    read_index_res.push((into_read_index_response(res), *region_id));
                    router_cbs.pop_front();
                }
            };

            futures::pin_mut!(read_index_fut);
            let deadline = Instant::now() + timeout;
            let delay = tikv_util::timer::PROXY_TIMER_HANDLE
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
            while let Some((cb, region_id)) = router_cbs.front_mut() {
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
            }
        }
        assert_eq!(req_vec_len, read_index_res.len());
        debug!("batch_read_index success"; "response"=>?read_index_res);
        read_index_res
    }

    fn make_read_index_task(&self, mut req: ReadIndexRequest) -> Option<ReadIndexTask> {
        let fut = {
            let region_id = req.get_context().get_region_id();
            let cmd = gen_read_index_raft_cmd_req(&mut req);

            let (cb, f) = paired_future_callback();

            if let Err(_) = self.routers[region_id as usize % self.routers.len()]
                .lock()
                .unwrap()
                .send_command(cmd, Callback::Read(cb), RaftCmdExtraOpts::default())
            {
                return None;
            } else {
                Some(f)
            }
        };

        let async_task = async {
            let res = match fut {
                None => None,
                Some(fut) => match fut.await {
                    Err(_) => None,
                    Ok(e) => Some(e),
                },
            };
            return into_read_index_response(res);
        };

        Some(ReadIndexTask {
            future: Box::pin(async_task),
        })
    }

    fn poll_read_index_task(
        &self,
        task: &mut ReadIndexTask,
        waker: Option<&ArcNotifyWaker>,
    ) -> Option<ReadIndexResponse> {
        let mut func = |cx: &mut std::task::Context| {
            let fut = &mut task.future;
            match fut.as_mut().poll(cx) {
                std::task::Poll::Pending => None,
                std::task::Poll::Ready(e) => Some(e),
            }
        };
        if let Some(waker) = waker {
            let waker = futures::task::waker_ref(waker);
            let cx = &mut std::task::Context::from_waker(&*waker);
            func(cx)
        } else {
            let waker = futures::task::noop_waker();
            let cx = &mut std::task::Context::from_waker(&waker);
            func(cx)
        }
    }
}

pub struct ReadIndexTask {
    future: BoxFuture<'static, ReadIndexResponse>,
}
