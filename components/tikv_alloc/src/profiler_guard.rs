use super::error::ProfResult;
use super::{activate_prof, deactivate_prof};

use futures::{Future, Poll};
use futures_locks::{Mutex, MutexFut, MutexGuard};

lazy_static! {
    static ref PROFILER_MUTEX: Mutex<u32> = Mutex::new(0);
}

pub struct ProfGuard(MutexGuard<u32>);

pub struct ProfLock(MutexFut<u32>);

impl ProfLock {
    pub fn new() -> ProfResult<ProfLock> {
        let guard = PROFILER_MUTEX.lock();
        match activate_prof() {
            Ok(_) => Ok(ProfLock(guard)),
            Err(e) => Err(e),
        }
    }
}

impl Drop for ProfGuard {
    fn drop(&mut self) {
        match deactivate_prof() {
            _ => {} // TODO: handle error here
        }
    }
}

impl Future for ProfLock {
    type Item = ProfGuard;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map(|item| item.map(|guard| ProfGuard(guard)))
    }
}
