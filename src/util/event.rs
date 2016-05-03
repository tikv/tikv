use std::sync::{Mutex, Condvar};
use std::time::{Duration, Instant};

/// This is a simple mechanism to synchronize between threads with timeout support.
pub struct Event<T> {
    m: Mutex<Option<T>>,
    cond: Condvar,
}

impl<T> Default for Event<T> {
    fn default() -> Event<T> {
        Event {
            m: Mutex::new(None),
            cond: Condvar::new(),
        }
    }
}

impl<T> Event<T> {
    pub fn new() -> Event<T> {
        Default::default()
    }

    /// Set this event and wake up all other waiting-set threads.
    pub fn set(&self, t: T) {
        let mut l = self.m.lock().unwrap();
        *l = Some(t);
        self.cond.notify_all();
    }

    /// Apply a function to an event.
    ///
    /// If the event is not set yet, None is returned; otherwise
    /// applied result is returned.
    pub fn apply<U, F: Fn(&T) -> U>(&self, f: F) -> Option<U> {
        if self.m.lock().unwrap().is_none() {
            return None;
        }
        let l = self.m.lock().unwrap();
        let res = f(l.as_ref().unwrap());
        Some(res)
    }

    /// Query current status without block.
    pub fn is_set(&self) -> bool {
        self.m.lock().unwrap().is_some()
    }

    /// Take the inner value and wait up all other waiting-take threads.
    pub fn take(&self) -> Option<T> {
        let mut l = self.m.lock().unwrap();
        let t = l.take();
        self.cond.notify_all();
        t
    }

    /// Wait till this event is set.
    ///
    /// If it's set before timeout, true is returned; otherwise return false.
    pub fn wait_set(&self, timeout: Option<Duration>) -> bool {
        self.wait(true, timeout)
    }

    // Wait for inner mutex status change.
    fn wait(&self, res: bool, timeout: Option<Duration>) -> bool {
        let start_time = Instant::now();
        let has_timeout = timeout.is_some();
        let timeout = timeout.unwrap_or_else(|| Duration::from_millis(0));
        let mut l = self.m.lock().unwrap();
        while l.is_some() != res {
            if !has_timeout {
                l = self.cond.wait(l).unwrap();
                continue;
            }
            let elapsed = start_time.elapsed();
            if timeout <= elapsed {
                return false;
            }
            let (v, timeout_res) = self.cond.wait_timeout(l, timeout - elapsed).unwrap();
            if timeout_res.timed_out() {
                return false;
            }
            l = v;
        }
        true
    }

    /// Wait for set status to be clear.
    ///
    /// If it's clear before timeout, true is returned; otherwise return false.
    pub fn wait_take(&self, timeout: Option<Duration>) -> bool {
        self.wait(false, timeout)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use std::thread;
    use std::time::{Instant, Duration};
    use std::sync::Arc;

    #[test]
    fn test_event() {
        let e = Arc::new(Event::new());
        let e2 = e.clone();

        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            e2.set(4);
            let set_time = Instant::now();
            e2.wait_take(None);
            assert!(set_time.elapsed() > Duration::from_millis(100));
        });

        assert!(!e.is_set());
        let start_time = Instant::now();
        assert!(!e.wait_set(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() >= Duration::from_millis(100));
        assert!(e.wait_set(None));
        assert!(start_time.elapsed() >= Duration::from_millis(200));
        assert!(e.is_set());

        let past_time = start_time.elapsed();
        e.wait_set(None);
        assert!(start_time.elapsed() - past_time < Duration::from_millis(1));

        assert!(!e.wait_take(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() - past_time >= Duration::from_millis(100));

        let v = e.apply(|s| *s);

        assert_eq!(e.take(), v);
        assert!(!e.is_set());
        assert!(!e.wait_set(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() - past_time >= Duration::from_millis(100));

        h.join().unwrap();
    }
}
