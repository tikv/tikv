use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    hash::Hash,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::{task::AtomicWaker, Stream};
use rand::{rngs::StdRng, RngCore, SeedableRng};

#[derive(Clone)]
pub struct FairQueues<K: Eq + PartialEq + Hash + Copy, I> {
    inner: Arc<Mutex<Inner<K, I>>>,
}

impl<K: Eq + PartialEq + Hash + Copy, I> FairQueues<K, I> {
    pub fn push(&self, key: K, item: I) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.closed {
            inner.push(key, item);
            return true;
        }
        false
    }

    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.close();
    }
}

pub struct Receiver<K: Eq + PartialEq + Hash + Copy, I> {
    inner: Arc<Mutex<Inner<K, I>>>,
    rng: StdRng,
}

impl<K: Eq + PartialEq + Hash + Copy, I> Stream for Receiver<K, I> {
    type Item = (K, I);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        let inner = this.inner.clone();
        let mut inner_ = inner.lock().unwrap();
        inner_.poll_next(cx, &mut this.rng)
    }
}

pub fn create<K: Eq + PartialEq + Hash + Copy, I>() -> (FairQueues<K, I>, Receiver<K, I>) {
    let inner = Arc::new(Mutex::new(Inner {
        keys: vec![],
        queues: HashMap::default(),
        waker: AtomicWaker::new(),
        closed: false,
    }));
    let queues = FairQueues { inner: inner.clone() };

    let rng = StdRng::seed_from_u64(
        SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs(),
    );
    let receiver = Receiver { inner, rng };

    (queues, receiver)
}

struct Inner<K: Eq + PartialEq + Hash + Copy, I> {
    keys: Vec<K>,
    queues: HashMap<K, VecDeque<I>>,
    waker: AtomicWaker,
    closed: bool,
}

impl<K: Eq + PartialEq + Hash + Copy, I> Inner<K, I> {
    fn push(&mut self, key: K, item: I) {
        let queue = match self.queues.entry(key) {
            Entry::Vacant(x) => {
                self.keys.push(key);
                x.insert(VecDeque::with_capacity(16))
            }
            Entry::Occupied(x) => x.into_mut(),
        };
        queue.push_back(item);
        self.waker.wake();
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.keys = vec![];
            self.queues = HashMap::default();
            self.waker.wake();
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>, rng: &mut StdRng) -> Poll<Option<(K, I)>> {
        if self.closed {
            return Poll::Ready(None);
        }

        if self.keys.is_empty() {
            self.waker.register(cx.waker());
        }
        if self.keys.is_empty() {
            return Poll::Pending;
        }

        let offset = rng.next_u64() as usize % self.keys.len();
        let key = self.keys[offset];
        if let Entry::Occupied(mut x) = self.queues.entry(key) {
            let item = x.get_mut().pop_front().unwrap();
            if x.get().is_empty() {
                x.remove();
                self.keys.swap_remove(offset);
            } else {
                let x = x.into_mut();
                let cap = x.capacity();
                let len = x.len();
                if cap > len * 2 {
                    x.shrink_to(cap / 2);
                }
            }
            return Poll::Ready(Some((key, item)));
        } else {
            unreachable!();
        }
    }
}

