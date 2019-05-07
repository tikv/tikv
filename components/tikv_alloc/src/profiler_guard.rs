use super::error::ProfResult;
use super::{activate_prof, deactivate_prof};

use futures::{Future, Poll};
use futures_locks::{Mutex, MutexFut, MutexGuard};

lazy_static! {
    static ref PROFILER_MUTEX: Mutex<u32> = Mutex::new(0);
}

pub struct ProfilerLock(MutexFut<u32>);

impl ProfilerLock {
    pub fn new() -> ProfResult<ProfilerLock> {
        match activate_prof() {
            Ok(_) => Ok(ProfilerLock(PROFILER_MUTEX.lock())),
            Err(e) => Err(e),
        }
    }
}

impl Drop for ProfilerLock {
    fn drop(&mut self) {
        match deactivate_prof() {
            _ => {} // TODO: handle error here
        }
    }
}

impl Future for ProfilerLock {
    type Item = MutexGuard<u32>;
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
