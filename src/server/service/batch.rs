use crate::server::metrics::GRPC_MSG_HISTOGRAM_STATIC;
use crate::server::service::kv::{batch_commands_response, poll_future_notify};
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use futures::Future;
use kvproto::kvrpcpb::*;
use tikv_util::mpsc::batch::Sender;
use tikv_util::time::{duration_to_sec, Instant};

#[derive(Default)]
pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    /// TODO: batch lock request
    // locks: Vec<PessimisticLockRequest>,
    get_ids: Vec<u64>,
    raw_get_ids: Vec<u64>,
    // lock_ids: Vec<u64>,
}

impl ReqBatcher {
    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn add_get_request(&mut self, req: GetRequest, id: u64) {
        self.gets.push(req);
        self.get_ids.push(id);
    }

    pub fn add_raw_get_request(&mut self, req: RawGetRequest, id: u64) {
        self.raw_gets.push(req);
        self.raw_get_ids.push(id);
    }

    pub fn maybe_commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if self.gets.len() > 10 {
            let gets = std::mem::replace(&mut self.gets, vec![]);
            let ids = std::mem::replace(&mut self.get_ids, vec![]);
            future_batch_get_command(storage, ids, gets, tx.clone());
        }
        if self.raw_gets.len() > 16 {
            let gets = std::mem::replace(&mut self.raw_gets, vec![]);
            let ids = std::mem::replace(&mut self.raw_get_ids, vec![]);
            future_batch_raw_get_command(storage, ids, gets, tx.clone());
        }
    }

    pub fn commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if !self.gets.is_empty() {
            let gets = std::mem::replace(&mut self.gets, vec![]);
            let ids = std::mem::replace(&mut self.get_ids, vec![]);
            future_batch_get_command(storage, ids, gets, tx.clone());
        }
        if !self.raw_gets.is_empty() {
            let gets = std::mem::replace(&mut self.raw_gets, vec![]);
            let ids = std::mem::replace(&mut self.raw_get_ids, vec![]);
            future_batch_raw_get_command(storage, ids, gets, tx.clone());
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
) {
    let begin_instant = Instant::now_coarse();
    let f = storage.batch_get_command(gets).then(move |ret| {
        match ret {
            Ok(ret) => {
                for (v, req) in ret.into_iter().zip(requests) {
                    let mut resp = GetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok(Some(val)) => resp.set_value(val),
                            Ok(None) => resp.set_not_found(true),
                            Err(e) => resp.set_error(extract_key_error(&e)),
                        }
                    }
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                    if tx.send_and_notify((req, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = GetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(extract_key_error(&e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .kv_batch_get_command
            .observe(begin_instant.elapsed_secs());
        Ok(())
    });
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
) {
    let begin_instant = Instant::now_coarse();
    let f = storage.raw_batch_get_command(gets).then(move |v| {
        match v {
            Ok(v) => {
                if requests.len() != v.len() {
                    error!("KvService batch response size mismatch");
                }
                for (req, v) in requests.into_iter().zip(v.into_iter()) {
                    let mut resp = RawGetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok(Some(val)) => resp.set_value(val),
                            Ok(None) => resp.set_not_found(true),
                            Err(e) => resp.set_error(format!("{}", e)),
                        }
                    }
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                    if tx.send_and_notify((req, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = RawGetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(format!("{}", e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .raw_batch_get_command
            .observe(duration_to_sec(begin_instant.elapsed()));
        Ok(())
    });
    poll_future_notify(f);
}
