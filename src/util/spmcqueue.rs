// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::Box;
use std::mem;
use std::ptr;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::usize;
use std::vec::Vec;

#[derive(Debug)]
enum Payload<T> {
    /// A node with actual data that can be popped.
    Data(T),
    /// A node representing a blocked request for data.
    Blocked(*mut thread::Thread),
}

const SPMCQUEUE_CELLS_NUM: usize = 1 << 9;
const SPMCQUEUE_CELLS_BIT: usize = SPMCQUEUE_CELLS_NUM - 1;
const SPMCQUEUE_SPINS: usize = 1 << 5;
const SPMCQUEUE_DEFAULT_THRESHOLD: usize = 1 << 3;
const SPMCQUEUE_PUT_FLAG: u32 = 1;
const SPMCQUEUE_POP_FLAG: u32 = 1 << 1;
const SPMCQUEUE_BOTH_FLAG: u32 = SPMCQUEUE_PUT_FLAG | SPMCQUEUE_POP_FLAG;

struct Node<T> {
    id: usize,
    cells: [AtomicPtr<Payload<T>>; SPMCQUEUE_CELLS_NUM],
    next: AtomicPtr<Node<T>>,
}

pub struct Queue_<T> {
    // if AtomicUsize == usize::MAX, There are other threads that have been reclaiming memory, Just give up
    next_reclaim_index: AtomicUsize,
    next_reclaim_node_ptr: AtomicPtr<Node<T>>,

    threshold: usize,

    put_index: usize,
    put_node: AtomicPtr<Node<T>>,

    pop_index: AtomicUsize,
    pop_node: AtomicPtr<Node<T>>,

    put_handles: Vec<AtomicPtr<Handle<T>>>,
    pop_handles: Vec<AtomicPtr<Handle<T>>>,

    stopped: AtomicBool,
    num_threads_put: usize,
    num_threads_pop: usize,

    barrier: Barrier,
}

pub struct Handle<T> {
    spare: *mut Node<T>,
    put_node: AtomicPtr<Node<T>>,
    pop_node: AtomicPtr<Node<T>>,
    queue: *mut Queue_<T>,
    thread: AtomicPtr<thread::Thread>,
    ptr: *mut Payload<T>,
    role_flag: u32,
}

impl<T> Handle<T> {
    pub fn push(&mut self, t: T) {
        let queue = unsafe { &mut *self.queue };
        queue.push(self, t);
    }

    pub fn pop(&mut self) -> Result<T, usize> {
        let queue = unsafe { &*self.queue };
        queue.pop(self)
    }

    pub fn wait(&mut self, pop_index: usize) -> Result<T, usize> {
        let queue = unsafe { &*self.queue };
        queue.wait(self, pop_index)
    }

    pub fn wait_timeout(&mut self, pop_index: usize, timeout: Duration) -> Result<T, usize> {
        let queue = unsafe { &*self.queue };
        queue.wait_timeout(self, pop_index, timeout)
    }

    pub fn update_pop_node(&mut self) {
        assert_eq!(self.role_flag, SPMCQUEUE_POP_FLAG);
        let queue = unsafe { &*self.queue };
        queue.update_pop_node(self);
    }

    pub fn stop(&mut self) {
        assert_eq!(self.role_flag, SPMCQUEUE_PUT_FLAG);
        let queue = unsafe { &*self.queue };
        queue.stop();
    }
}

const DEFAULT_NUM_THREADS: usize = 1 << 3;
impl<T> Default for Queue_<T> {
    fn default() -> Self {
        Self::new(1, DEFAULT_NUM_THREADS)
    }
}

impl<T> Queue_<T> {
    /// Create a new, empty queue.
    pub fn new(num_threads_put: usize, num_threads_pop: usize) -> Queue_<T> {
        let node_ptr = Box::into_raw(Box::new(Node {
            id: 0,
            cells: unsafe { mem::zeroed() },
            next: AtomicPtr::default(),
        }));
        // producers
        assert_eq!(num_threads_put, 1);
        let mut put_handles_ = Vec::with_capacity(num_threads_put);
        for _ in 0..num_threads_put {
            put_handles_.push(AtomicPtr::default());
        }
        // consumers
        let mut pop_handles_ = Vec::with_capacity(num_threads_pop);
        for _ in 0..num_threads_pop {
            pop_handles_.push(AtomicPtr::default());
        }

        Queue_ {
            threshold: SPMCQUEUE_DEFAULT_THRESHOLD,
            next_reclaim_index: AtomicUsize::new(0),
            next_reclaim_node_ptr: AtomicPtr::new(node_ptr),
            put_node: AtomicPtr::new(node_ptr),
            pop_node: AtomicPtr::new(node_ptr),
            put_index: 0,
            pop_index: AtomicUsize::new(0),
            put_handles: put_handles_,
            pop_handles: pop_handles_,
            stopped: AtomicBool::new(false),
            num_threads_put,
            num_threads_pop,
            barrier: Barrier::new(num_threads_put + num_threads_pop),
        }
    }

    pub fn register_as_producer(&self) -> *mut Handle<T> {
        self.register(SPMCQUEUE_PUT_FLAG)
    }

    pub fn register_as_consumer(&self) -> *mut Handle<T> {
        self.register(SPMCQUEUE_POP_FLAG)
    }

    fn register(&self, flag: u32) -> *mut Handle<T> {
        if flag & SPMCQUEUE_BOTH_FLAG == SPMCQUEUE_BOTH_FLAG {
            panic!("thread can't both be enqueuer and dequeuer");
        }

        let handle = Box::into_raw(Box::new(Handle::<T> {
            thread: AtomicPtr::default(),
            ptr: ptr::null_mut(),
            spare: ptr::null_mut(),
            queue: self as *const Queue_<T> as *mut Queue_<T>,
            put_node: AtomicPtr::new(self.put_node.load(Ordering::Acquire)),
            pop_node: AtomicPtr::new(self.pop_node.load(Ordering::Acquire)),
            role_flag: flag,
        }));
        let handle_obj = unsafe { &mut *handle };
        if flag & SPMCQUEUE_PUT_FLAG == SPMCQUEUE_PUT_FLAG {
            let mut i = 0;
            while i < self.num_threads_put {
                if self.put_handles[i]
                    .compare_exchange(
                        ptr::null_mut(),
                        handle,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
                i += 1;
            }
            if i == self.num_threads_put {
                panic!("producers is too much ");
            }
        }

        if flag & SPMCQUEUE_POP_FLAG == SPMCQUEUE_POP_FLAG {
            handle_obj.thread.store(
                Box::into_raw(Box::new(thread::current())),
                Ordering::Release,
            );
            let mut i = 0;
            while i < self.num_threads_pop {
                if self.pop_handles[i]
                    .compare_exchange(
                        ptr::null_mut(),
                        handle,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
                i += 1;
            }
            if i == self.num_threads_pop {
                panic!("consumers is too much");
            }
        }
        self.barrier.wait();
        handle
    }

    fn update_private_node(
        &self,
        private_handle: &mut Handle<T>,
        private_node: *mut Node<T>,
        id: usize,
    ) -> &Node<T> {
        let mut private_put_node = unsafe { &*private_node };
        loop {
            if private_put_node.id < id {
                if private_put_node.next.load(Ordering::Acquire).is_null() {
                    let mut new_shared_node_ptr = private_handle.spare;
                    if new_shared_node_ptr.is_null() {
                        new_shared_node_ptr = Box::into_raw(Box::new(Node {
                            id: private_put_node.id + 1,
                            cells: unsafe { mem::zeroed() },
                            next: AtomicPtr::default(),
                        }));
                    } else {
                        unsafe { &mut *new_shared_node_ptr }.id = private_put_node.id + 1;
                    }
                    match private_put_node.next.compare_exchange(
                        ptr::null_mut(),
                        new_shared_node_ptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            private_handle.spare = ptr::null_mut();
                        }
                        Err(_) => {
                            private_handle.spare = new_shared_node_ptr;
                        }
                    }
                }

                private_put_node = unsafe { &*private_put_node.next.load(Ordering::Acquire) };
            } else {
                return private_put_node;
            }
        }
    }

    fn push(&mut self, private_put_handle: &mut Handle<T>, t: T) {
        // put the data(t) in the heap
        let ptr = Box::into_raw(Box::new(Payload::Data(t)));
        // get the node_id and node_index.
        let mut put_index = self.put_index;
        self.put_index = put_index + 1;
        let id = put_index / SPMCQUEUE_CELLS_NUM;
        // put_index in the node
        put_index &= SPMCQUEUE_CELLS_BIT;

        // Try to fill in the nodes we need
        let node_ptr = private_put_handle.put_node.load(Ordering::Acquire);
        let private_put_node = self.update_private_node(private_put_handle, node_ptr, id);
        private_put_handle.put_node.store(
            private_put_node as *const Node<T> as *mut Node<T>,
            Ordering::Release,
        );

        // put_index in private_put_node is the need'd cell
        let ptr_order = private_put_node.cells[put_index].swap(ptr, Ordering::Relaxed);
        if !ptr_order.is_null() {
            // there is one consumer wait for the data, just wake up it...
            let pay = unsafe { Box::from_raw(ptr_order) };
            match *pay {
                Payload::Blocked(thread) => unsafe {
                    (*thread).unpark();
                },
                _ => unreachable!(),
            }
        }
    }

    /// Called while spinning (name borrowed from Linux). Can be implemented to call
    /// a platform-specific method of lightening CPU load in spinlocks.
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[inline(always)]
    pub fn cpu_relax() {
        // This instruction is meant for usage in spinlock loops
        // (see Intel x86 manual, III, 4.2)
        unsafe {
            asm!("pause" :::: "volatile");
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    #[inline(always)]
    pub fn cpu_relax() {}

    fn pop(&self, private_pop_handle: &mut Handle<T>) -> Result<T, usize> {
        let mut v;
        let mut pop_index = self.pop_index.fetch_add(1, Ordering::Relaxed);
        let id = pop_index / SPMCQUEUE_CELLS_NUM;
        // pop_index in node
        pop_index &= SPMCQUEUE_CELLS_BIT;

        // Try to fill in the nodes we need
        let node_ptr = private_pop_handle.pop_node.load(Ordering::Acquire);
        let private_pop_node = self.update_private_node(private_pop_handle, node_ptr, id);
        private_pop_handle.pop_node.store(
            private_pop_node as *const Node<T> as *mut Node<T>,
            Ordering::Release,
        );

        if self.stopped.load(Ordering::Acquire) {
            // queue is stopped, and return None,
            return self.try_clean_none(private_pop_handle, pop_index);
        }

        // pop_index in private_pop_node is the need'd cell
        // to test SPMCQUEUE_SPINS times to probes the datas
        let mut times = SPMCQUEUE_SPINS;
        loop {
            v = private_pop_node.cells[pop_index].load(Ordering::Acquire);
            if !v.is_null() {
                return self.get_data(v, private_pop_handle, pop_index);
            }
            times -= 1;
            if times == 0 {
                break;
            }
            Self::cpu_relax();
        }

        // now We've tried many times. should wait for data
        let ptr_handle = private_pop_handle.thread.load(Ordering::Acquire);
        let ptr = Box::into_raw(Box::new(Payload::Blocked(ptr_handle)));
        v = private_pop_node.cells[pop_index].swap(ptr, Ordering::Relaxed);
        if v.is_null() {
            // we should wait for the data..(woke by producers)
            private_pop_handle.ptr = ptr;
            Err(pop_index)
        } else {
            // v is not null, We got the data
            unsafe {
                // just reclaim the not needed data
                Box::from_raw(ptr);
            }
            self.get_data(v, private_pop_handle, pop_index)
        }
    }

    fn wait(&self, private_pop_handle: &mut Handle<T>, pop_index: usize) -> Result<T, usize> {
        // we should wait for the data..(woke by producers)
        let private_pop_node = unsafe { &*private_pop_handle.pop_node.load(Ordering::Acquire) };
        loop {
            let v = private_pop_node.cells[pop_index].load(Ordering::Acquire);
            if v != private_pop_handle.ptr {
                private_pop_handle.ptr = ptr::null_mut();
                return self.get_data(v, private_pop_handle, pop_index);
            }
            if self.stopped.load(Ordering::Acquire) {
                // queue is stopped, and return None,
                return self.try_clean_none(private_pop_handle, pop_index);
            }
            thread::park();
        }
    }

    fn wait_timeout(
        &self,
        private_pop_handle: &mut Handle<T>,
        pop_index: usize,
        timeout: Duration,
    ) -> Result<T, usize> {
        // we should wait for the data..(woke by producers)
        let beginning_park = Instant::now();
        let mut timeout_remaining;
        let private_pop_node = unsafe { &*private_pop_handle.pop_node.load(Ordering::Acquire) };
        loop {
            let v = private_pop_node.cells[pop_index].load(Ordering::Acquire);
            if v != private_pop_handle.ptr {
                private_pop_handle.ptr = ptr::null_mut();
                return self.get_data(v, private_pop_handle, pop_index);
            }
            if self.stopped.load(Ordering::Acquire) {
                // queue is stopped, and return None,
                return self.try_clean_none(private_pop_handle, pop_index);
            }
            // with spurious wakeup
            let elapsed = beginning_park.elapsed();
            if elapsed >= timeout {
                return Err(pop_index);
            }
            timeout_remaining = timeout - elapsed;
            thread::park_timeout(timeout_remaining);
        }
    }

    // return data and maybe reclaim the queue's nodes
    fn get_data(
        &self,
        v: *mut Payload<T>,
        private_pop_handle: &mut Handle<T>,
        pop_index: usize,
    ) -> Result<T, usize> {
        let resu = unsafe { Box::from_raw(v) };
        let result = match *resu {
            Payload::Data(t) => Ok(t),
            _ => unreachable!(),
        };
        self.try_clean(private_pop_handle, pop_index);
        result
    }

    fn update_pop_node(&self, private_pop_handle: &mut Handle<T>) {
        let id = self.pop_index.load(Ordering::Acquire) / SPMCQUEUE_CELLS_NUM;
        let node_ptr = private_pop_handle.pop_node.load(Ordering::Acquire);
        let private_pop_node = self.update_private_node(private_pop_handle, node_ptr, id);
        private_pop_handle.pop_node.store(
            private_pop_node as *const Node<T> as *mut Node<T>,
            Ordering::Release,
        );
    }

    fn try_clean_none(
        &self,
        private_pop_handle: &mut Handle<T>,
        pop_index: usize,
    ) -> Result<T, usize> {
        self.try_clean(private_pop_handle, pop_index);
        Err(usize::MAX)
    }

    fn try_clean(&self, private_pop_handle: &mut Handle<T>, pop_index: usize) {
        if pop_index & SPMCQUEUE_CELLS_BIT == SPMCQUEUE_CELLS_BIT {
            //fence(Ordering::Acquire);
            let init_index = self.next_reclaim_index.load(Ordering::Acquire);
            if init_index < usize::MAX && ((unsafe {
                &*private_pop_handle.pop_node.load(Ordering::Acquire)
            }.id) - init_index) >= self.threshold
                && self.next_reclaim_index
                    .compare_exchange(init_index, usize::MAX, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
            {
                let mut init_node = self.next_reclaim_node_ptr.load(Ordering::Acquire);

                let th = &self.pop_handles[0];
                let mut min_node = unsafe { &*th.load(Ordering::Acquire) }
                    .pop_node
                    .load(Ordering::Acquire);

                for i in 1..self.num_threads_pop {
                    let next = &self.pop_handles[i];
                    if next.load(Ordering::Acquire).is_null() {
                        break;
                    }
                    let next_min = unsafe { &*next.load(Ordering::Acquire) }
                        .pop_node
                        .load(Ordering::Acquire);
                    if (unsafe { &*next_min }).id < (unsafe { &*min_node }).id {
                        min_node = next_min;
                    }
                    if (unsafe { &*min_node }).id <= init_index {
                        break;
                    }
                }

                for i in 0..self.num_threads_put {
                    let next = &self.put_handles[i];
                    if next.load(Ordering::Acquire).is_null() {
                        break;
                    }
                    let next_min = unsafe { &*next.load(Ordering::Acquire) }
                        .put_node
                        .load(Ordering::Acquire);
                    if (unsafe { &*next_min }).id < (unsafe { &*min_node }).id {
                        min_node = next_min;
                    }
                    if (unsafe { &*min_node }).id <= init_index {
                        break;
                    }
                }

                let new_id = (unsafe { &*min_node }).id;
                if new_id <= init_index {
                    self.next_reclaim_index.store(init_index, Ordering::Release);
                } else {
                    self.next_reclaim_node_ptr
                        .store(min_node, Ordering::Release);
                    self.next_reclaim_index.store(new_id, Ordering::Release);

                    loop {
                        let tmp = &(unsafe { &*init_node }).next;
                        let init_node_next = tmp.load(Ordering::Acquire);
                        unsafe { Box::from_raw(init_node) };
                        if init_node_next == min_node {
                            break;
                        }
                        init_node = init_node_next;
                    }
                }
            }
        }
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        for i in 0..self.num_threads_pop {
            let th = &self.pop_handles[i];
            if th.load(Ordering::Acquire).is_null() {
                return;
            }
            let handle = unsafe { &*th.load(Ordering::Acquire) };
            let thread = unsafe { &*handle.thread.load(Ordering::Acquire) };
            thread.unpark();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_put_pop_data() {
        let num_threads = 64;
        const COUNT: usize = 1000;
        let per_thread_count = COUNT / num_threads;
        let queue = Arc::new(Queue_::<usize>::new(1, num_threads));

        let array: [AtomicUsize; COUNT] = unsafe { mem::zeroed() };
        let array_arc = Arc::new(array);
        let mut guards = vec![];

        for _ in 0..num_threads {
            let queue_pop = Arc::clone(&queue);
            let array_arc = Arc::clone(&array_arc);
            guards.push(thread::spawn(move || {
                let handle = queue_pop.register_as_consumer();
                let handle = unsafe { &mut *handle };
                for _ in 0..per_thread_count + 10000 {
                    match handle.pop() {
                        Ok(data) => {
                            array_arc[data].store(1, Ordering::Release);
                        }
                        Err(index) => {
                            if index == usize::MAX {
                                return;
                            }
                            match handle.wait_timeout(index, Duration::from_secs(1)) {
                                Ok(data) => {
                                    array_arc[data].store(1, Ordering::Release);
                                }
                                Err(index) => {
                                    if index == usize::MAX {
                                        return;
                                    }
                                    match handle.wait(index) {
                                        Ok(data) => {
                                            array_arc[data].store(1, Ordering::Release);
                                        }
                                        Err(_) => {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }));
        }

        let handle = queue.register_as_producer();
        let handle = unsafe { &mut *handle };
        for i in 0..COUNT {
            handle.push(i);
        }

        thread::sleep(Duration::from_secs(2));
        handle.stop();
        for guard in guards {
            guard.join().unwrap();
        }

        for item in array_arc.iter().take(COUNT) {
            assert_eq!(item.load(Ordering::Acquire), 1);
        }
    }
}
