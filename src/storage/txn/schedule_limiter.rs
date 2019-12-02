use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

//pub struct Node<T: Clone> {
//    item: Option<T>,
//    next: AtomicPtr<Node<T>>,
//}
//
//pub struct AtomicList<T: Clone> {
//    head: AtomicPtr<Node<T>>
//}
//
//impl<T: Clone> AtomicList<T> {
//    pub fn new() -> AtomicList<T> {
//        AtomicList {
//            head: AtomicPtr::default(),
//        }
//    }
//
//    pub fn add(&self, v: T) -> &T {
//
//    }
//}

pub struct ScheduleLimiter {
    low_priority_write_limit: usize,
    request_freq: Vec<(AtomicUsize, AtomicBool)>,
}

impl ScheduleLimiter {
    pub fn new(low_priority_write_limit: usize, capacity: usize) -> ScheduleLimiter {
        let mut request_freq = Vec::with_capacity(capacity);
        request_freq.resize_with(capacity, || (AtomicUsize::new(0), AtomicBool::new(false)));
        ScheduleLimiter {
            low_priority_write_limit,
            request_freq,
        }
    }

    pub fn delay(&self, region_id: usize, high_priority: bool, key_size: usize) -> bool {
        let v = &self.request_freq[region_id % self.request_freq.len()];
        if high_priority {
            v.1.store(true, Ordering::Relaxed);
            return false;
        } else {
            if v.1.load(Ordering::Acquire) {
                let prev = v.0.fetch_add(key_size, Ordering::Acquire);

                if prev >= self.low_priority_write_limit {
                    if v.1.compare_and_swap(true, false, Ordering::AcqRel) {
                        v.0.fetch_sub(self.low_priority_write_limit, Ordering::Relaxed);
                    }
                    eprintln!("==========delay write in region: {}", high_priority);
                    return true;
                }
            }
            return false;
        }
    }

    pub fn is_high_priority_region(&self, region_id: usize) -> bool {
        let v = &self.request_freq[region_id % self.request_freq.len()];
        v.1.load(Ordering::Relaxed)
    }

    pub fn change_to_high_priority(&self, region_id: usize) -> bool {
        let v = &self.request_freq[region_id % self.request_freq.len()];
        if v.1.load(Ordering::Relaxed) {
            if v.0.fetch_add(1, Ordering::Relaxed) > self.low_priority_write_limit {
                return false;
            }
            return true;
        }
        false
    }
}
