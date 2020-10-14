use crate::store::{PeerMsg, StoreMsg};
use bitflags::_core::cell::RefCell;
use crossbeam::channel::TrySendError;
use engine_traits::KvEngine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tikv_util::collections::HashMap;
use tikv_util::lru::LruCache;
use tokio::sync::mpsc::error as tokio_error;
use tokio::sync::mpsc::UnboundedSender;

pub struct AsyncRouter<EK: KvEngine> {
    normals: Arc<Mutex<HashMap<u64, UnboundedSender<PeerMsg<EK>>>>>,
    caches: RefCell<LruCache<u64, UnboundedSender<PeerMsg<EK>>>>,
    control_scheduler: UnboundedSender<StoreMsg<EK>>,

    // Indicates the router is shutdown down or not.
    shutdown: Arc<AtomicBool>,
}

impl<EK: KvEngine> Clone for AsyncRouter<EK> {
    fn clone(&self) -> Self {
        AsyncRouter {
            normals: self.normals.clone(),
            caches: RefCell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_scheduler: self.control_scheduler.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

pub enum AsyncRouterError<EK: KvEngine> {
    NotExist(PeerMsg<EK>),
    Closed(PeerMsg<EK>),
}

impl<EK: KvEngine> std::fmt::Debug for AsyncRouterError<EK> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AsyncRouterError::NotExist(t) => write!(f, "NotExist({:?})", t),
            AsyncRouterError::Closed(t) => write!(f, "Closed({:?})", t),
        }
    }
}

type RouterResult<EK> = std::result::Result<(), AsyncRouterError<EK>>;

impl<EK: KvEngine> AsyncRouter<EK> {
    #[inline]
    fn check_do(&self, addr: u64, msg: PeerMsg<EK>) -> RouterResult<EK> {
        let (cnt, sender) = {
            let boxes = self.normals.lock().unwrap();
            let cnt = boxes.len();
            let b = match boxes.get(&addr) {
                Some(sender) => sender.clone(),
                None => {
                    drop(boxes);
                    return Err(AsyncRouterError::NotExist(msg));
                }
            };
            (cnt, b)
        };
        if cnt > self.caches.borrow().capacity() || cnt < self.caches.borrow().capacity() / 2 {
            self.caches.borrow_mut().resize(cnt);
        }

        match sender.send(msg) {
            Ok(()) => {
                self.caches.borrow_mut().insert(addr, sender);
                Ok(())
            }
            Err(tokio_error::SendError(t)) => Err(AsyncRouterError::Closed(t)),
        }
    }

    #[inline]
    fn send_in_cache(&self, addr: u64, msg: PeerMsg<EK>) -> RouterResult<EK> {
        let mut caches = self.caches.borrow_mut();
        if let Some(sender) = caches.get(&addr) {
            match sender.send(msg) {
                Ok(()) => return Ok(()),
                Err(tokio_error::SendError(t)) => {
                    caches.remove(&addr);
                    drop(caches);
                    return self.check_do(addr, t);
                }
            }
        }
        drop(caches);
        self.check_do(addr, msg)
    }
}

impl<EK: KvEngine> AsyncRouter<EK> {
    pub fn new(control_scheduler: UnboundedSender<StoreMsg<EK>>) -> AsyncRouter<EK> {
        AsyncRouter {
            normals: Arc::new(Mutex::new(HashMap::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            caches: RefCell::new(LruCache::with_capacity_and_sample(1024, 7)),
            control_scheduler,
        }
    }

    // Indicates the router is shutdown down or not.
    pub fn send(&self, region_id: u64, msg: PeerMsg<EK>) -> RouterResult<EK> {
        self.send_in_cache(region_id, msg)
    }

    pub fn try_send(&self, region_id: u64, msg: PeerMsg<EK>) {
        let _ = self.send_in_cache(region_id, msg);
    }

    pub fn force_send(
        &self,
        region_id: u64,
        msg: PeerMsg<EK>,
    ) -> std::result::Result<(), TrySendError<PeerMsg<EK>>> {
        match self.send_in_cache(region_id, msg) {
            Ok(()) => Ok(()),
            Err(AsyncRouterError::Closed(t)) => Err(TrySendError::Disconnected(t)),
            Err(AsyncRouterError::NotExist(t)) => Err(TrySendError::Disconnected(t)),
        }
    }

    pub fn send_control(
        &self,
        msg: StoreMsg<EK>,
    ) -> std::result::Result<(), TrySendError<StoreMsg<EK>>> {
        match self.control_scheduler.send(msg) {
            Ok(()) => Ok(()),
            Err(tokio_error::SendError(t)) => Err(TrySendError::Disconnected(t)),
        }
    }

    pub fn register(&self, addr: u64, sender: UnboundedSender<PeerMsg<EK>>) {
        let mut normals = self.normals.lock().unwrap();
        let mut caches = self.caches.borrow_mut();
        caches.remove(&addr);
        normals.insert(addr, sender);
    }

    pub fn close(&self, addr: u64) {
        let mut normals = self.normals.lock().unwrap();
        let mut caches = self.caches.borrow_mut();
        caches.remove(&addr);
        if let Some(sender) = normals.remove(&addr) {
            let _ = sender.send(PeerMsg::Stop);
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> PeerMsg<EK>) {
        let normals = self.normals.lock().unwrap();
        for sender in normals.values() {
            let _ = sender.send(msg_gen());
        }
    }

    pub fn control_mailbox(&self) -> UnboundedSender<StoreMsg<EK>> {
        self.control_scheduler.clone()
    }

    pub fn shutdown(&self) {
        self.broadcast_normal(|| PeerMsg::Stop);
        self.caches.borrow_mut().clear();
        let mut normals = self.normals.lock().unwrap();
        normals.clear();
    }

    pub fn mailbox(&self, addr: u64) -> Option<UnboundedSender<PeerMsg<EK>>> {
        let mut caches = self.caches.borrow_mut();
        if let Some(sender) = caches.get(&addr) {
            return Some(sender.clone());
        }
        let boxes = self.normals.lock().unwrap();
        boxes.get(&addr).cloned()
    }
}
