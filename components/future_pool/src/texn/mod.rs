use crossbeam::channel::Select;
use crossbeam::deque::{Injector, Steal, Stealer, Worker as LocalQueue};
use crossbeam::{channel, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use lazy_static::lazy_static;
use num_cpus;
use parking_lot::{Condvar, Mutex};
use rand::prelude::*;
use serde::Deserialize;
use toml;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::future::Future;
use std::mem::{forget, ManuallyDrop};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::{Duration, Instant};

use dropmap::DropMap;

mod dropmap;

#[derive(Debug, Deserialize)]
struct Config {
    num_thread: usize,
    // dropmap swap interval (in secs)
    swap_interval: u64,
    queue_privilige: Vec<u64>,
    time_feedback: Vec<u64>,
    percentage: u64,
}

lazy_static! {
    static ref CONFIG: Config = {
        Config {num_thread: num_cpus::get_physical(),
        swap_interval: 20,
        queue_privilige: vec![32, 4, 1],
        time_feedback: vec![1000, 300_000, 10_000_000],
        percentage: 80,
        }
        // let content = read_to_string("/root/config.toml").unwrap();
        // let content = read_to_string("/data/waynest/code/pingcap_hackathon2019/adaptive-thread-pool/texn/src/config.toml").unwrap();
        // toml::from_str(&content).unwrap()
    };
    // take how many tasks from a queue in one term
    static ref QUEUE_PRIVILEGE:&'static[u64] = &CONFIG.queue_privilige;
    static ref FIRST_PRIVILEGE: AtomicU64 = AtomicU64::new(QUEUE_PRIVILEGE[0]);
    // the longest executed time a queue can hold (in micros)
    static ref TIME_FEEDBACK:&'static[u64] = &CONFIG.time_feedback;
    // SMALL_TASK_CNT/HUGE_TASK_CNT should equal to PERCENTAGE
    static ref PERCENTAGE: u64 = {
        if CONFIG.percentage > 90{
            panic!("percentage greater than 90%");
        }
        CONFIG.percentage
        };
    static ref SMALL_TASK_CNT: AtomicU64 = AtomicU64::new(0);
    static ref HUGE_TASK_CNT: AtomicU64 = AtomicU64::new(0);
    static ref MIN_PRI : u64 = 4;
    static ref MAX_PRI : u64 = 4096 / CONFIG.num_thread as u64;
}

// external upper level tester
use adaptive_spawn::{AdaptiveSpawn, Options};

struct Parker {
    sleep: Mutex<usize>,
    cvar: Condvar,
    notified: AtomicBool,
}

impl Parker {
    fn wait(&self) {
        let mut sleep = self.sleep.lock();
        if !self.notified.swap(false, SeqCst) {
            *sleep += 1;
            self.cvar.wait(&mut sleep);
        }
    }

    fn notify_one(&self) {
        if !self.notified.load(SeqCst) {
            let mut sleep = self.sleep.lock();

            if *sleep > 0 {
                *sleep -= 1;
                self.cvar.notify_one();
            } else {
                self.notified.store(true, SeqCst);
            }
        }
    }
}

struct Worker {
    local: LocalQueue<ArcTask>,
    stealers: Vec<Stealer<ArcTask>>,
    injectors: Arc<[Injector<ArcTask>]>,
    parker: Arc<Parker>,
    after_start: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl Worker {
    fn run(self) {
        thread::spawn(move || {
            (self.after_start)();
            let mut rng = thread_rng();
            let mut step = 0;
            loop {
                if let Some(task) = self.find_task(&mut rng) {
                    step = 0;
                    poll_task(task);
                } else {
                    match step {
                        0 | 1 => {
                            std::thread::yield_now();
                            step += 1;
                        }
                        2 => {
                            std::thread::sleep(Duration::from_micros(10));
                            step += 1;
                        }
                        _ => {
                            self.parker.wait();
                            step = 0;
                        }
                    }
                }
            }
        });
    }

    fn find_task(&self, rng: &mut ThreadRng) -> Option<ArcTask> {
        if let Some(task) = self.local.pop() {
            return Some(task);
        }
        let mut retry = true;
        while retry {
            retry = false;
            // Local is empty, steal from injector
            let i = rng.gen::<usize>() % 3;
            for j in 0..3 {
                let idx = (i + j) % 3;
                match self.injectors[idx].steal_batch_and_pop(&self.local) {
                    Steal::Success(task) => {
                        return Some(task);
                    }
                    Steal::Retry => retry = true,
                    _ => {}
                }
            }
            // Fail to steal from injectors, steal from others
            let i = rng.gen::<usize>() % self.stealers.len();
            for j in 0..self.stealers.len() {
                let idx = (i + j) % self.stealers.len();
                match self.stealers[idx].steal_batch_and_pop(&self.local) {
                    Steal::Success(task) => {
                        return Some(task);
                    }
                    Steal::Retry => retry = true,
                    _ => {}
                }
            }
        }
        None
    }
}

fn poll_task(task: ArcTask) {
    unsafe {
        task.poll();
    }
}

#[derive(Default, Clone)]
struct TaskStats {
    elapsed: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct ThreadPool {
    injectors: Arc<[Injector<ArcTask>]>,
    stats: DropMap<u64, TaskStats>,
    parker: Arc<Parker>,
}

impl ThreadPool {
    pub fn new(num_threads: usize, f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
        let mut injectors = Vec::new();
        let parker = Arc::new(Parker {
            sleep: Mutex::new(0),
            cvar: Condvar::new(),
            notified: AtomicBool::new(false),
        });
        let mut workers: Vec<Worker> = Vec::new();
        for _ in *QUEUE_PRIVILEGE {
            let injector = Injector::new();
            injectors.push(injector);
        }
        let injectors: Arc<[Injector<ArcTask>]> = Arc::from(injectors);
        let mut local_queues = Vec::new();
        for _ in 0..num_threads {
            local_queues.push(LocalQueue::new_fifo());
        }
        let all_stealers: Vec<_> = local_queues.iter().map(|q| q.stealer()).collect();
        for (i, local) in local_queues.into_iter().enumerate() {
            let mut stealers = Vec::new();
            for j in 0..num_threads {
                if i != j {
                    stealers.push(all_stealers[j].clone());
                }
            }
            let worker = Worker {
                local,
                stealers,
                injectors: injectors.clone(),
                parker: parker.clone(),
                after_start: f.clone(),
            };
            workers.push(worker);
        }
        for worker in workers {
            worker.run();
        }
        ThreadPool {
            injectors,
            stats: DropMap::new(CONFIG.swap_interval),
            parker,
        }
    }

    pub fn spawn<F>(&self, task: F, token: u64, nice: u8)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // at begin a token has top priority
        let stat = &*self.stats.get_or_insert(&token, TaskStats::default());
        let injector = match stat.elapsed.load(SeqCst) {
            0..=999 => &self.injectors[0],
            1000..=299_999 => &self.injectors[1],
            _ => &self.injectors[2],
        };
        injector.push(ArcTask::new(
            task,
            self.injectors.clone(),
            self.parker.clone(),
            stat.clone(),
            nice,
            token,
        ));
        self.parker.notify_one();
    }

    pub fn new_from_config(f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
        ThreadPool::new(CONFIG.num_thread, f)
    }
}

// #[derive(Clone)]
// pub struct ThreadPool {
//     // first priority, thread independent task queues
//     first_queues: Vec<Sender<ArcTask>>,
//     // other shared queues
//     queues: Arc<[Sender<ArcTask>]>,
//     // stats: token -> (executed_time(in micros),queue_index)
//     stats: DropMap<u64, (Arc<AtomicU64>, Arc<AtomicUsize>)>,
//     num_threads: usize,
//     first_queue_iter: Arc<AtomicUsize>,
// }

// impl ThreadPool {
//     pub fn new(num_threads: usize, f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
//         let mut queues = Vec::new();
//         let mut rxs = Vec::new();
//         let mut first_queues = Vec::new();
//         for _ in QUEUE_PRIVILEGE.into_iter() {
//             let (tx, rx) = channel::unbounded();
//             queues.push(tx);
//             rxs.push(rx);
//         }
//         // create stats
//         let stats = DropMap::new(CONFIG.swap_interval);
//         let queues: Arc<[Sender<ArcTask>]> = Arc::from(queues.into_boxed_slice());
//         // spawn worker threads
//         for _ in 0..num_threads {
//             let (tx, rx) = channel::unbounded();
//             first_queues.push(tx.clone());
//             let mut rxs = rxs.clone();
//             rxs.push(rx);
//             rxs.swap_remove(0);
//             let f = f.clone();
//             thread::spawn(move || {
//                 f();
//                 let mut sel = Select::new();
//                 let mut rx_map = HashMap::new();
//                 for rx in &rxs {
//                     let idx = sel.recv(rx);
//                     rx_map.insert(idx, rx);
//                 }
//                 loop {
//                     let mut is_empty = true;
//                     for ((rx, &limit), index) in
//                         rxs.iter().zip(QUEUE_PRIVILEGE.into_iter()).zip(0..)
//                     {
//                         if index == 0 {
//                             for task in rx.try_iter().take(FIRST_PRIVILEGE.load(SeqCst) as usize) {
//                                 is_empty = false;
//                                 unsafe { poll_with_timer(task, index) };
//                             }
//                         } else {
//                             for task in rx.try_iter().take(limit as usize) {
//                                 is_empty = false;
//                                 unsafe { poll_with_timer(task, index) };
//                             }
//                         }
//                     }
//                     if is_empty {
//                         let oper = sel.select();
//                         let rx = rx_map.get(&oper.index()).unwrap();
//                         if let Ok(task) = oper.recv(*rx) {
//                             let index = task.0.index.load(SeqCst);
//                             unsafe { poll_with_timer(task, index) };
//                         }
//                     }
//                 }
//             });
//         }
//         // spawn adjustor thread
//         thread::spawn(move || {
//             loop {
//                 thread::sleep(Duration::from_secs(1));
//                 let small = SMALL_TASK_CNT.load(SeqCst);
//                 let huge = HUGE_TASK_CNT.load(SeqCst);
//                 if (small + huge) == 0 {
//                     continue;
//                 }
//                 let cur_perc: u64 = small * 100 / (small + huge);
//                 let first_privilege = FIRST_PRIVILEGE.load(SeqCst);
//                 // println!("{}  {} -> {}\t{}", a, b, cur_perc, first_privilege);
//                 // need to decrease first priority
//                 if cur_perc > *PERCENTAGE + 5 {
//                     let mut new_pri = first_privilege / 2;
//                     if new_pri < *MIN_PRI {
//                         new_pri = *MIN_PRI;
//                     }
//                     FIRST_PRIVILEGE.store(new_pri, SeqCst);
//                 }
//                 // need to increase first priority
//                 else if cur_perc < *PERCENTAGE - 5 {
//                     let mut new_pri = first_privilege * 2;
//                     if new_pri > *MAX_PRI {
//                         new_pri = *MAX_PRI;
//                     }
//                     FIRST_PRIVILEGE.store(new_pri, SeqCst);
//                 }
//                 // reset counter
//                 SMALL_TASK_CNT.store(0, SeqCst);
//                 HUGE_TASK_CNT.store(0, SeqCst);
//             }
//         });
//         ThreadPool {
//             first_queues,
//             queues,
//             stats,
//             num_threads,
//             first_queue_iter: Arc::default(),
//         }
//     }

//     pub fn spawn<F>(&self, task: F, token: u64, nice: u8)
//     where
//         F: Future<Output = ()> + Send + 'static,
//     {
//         // at begin a token has top priority
//         let (atom_elapsed, atom_index) = &*self
//             .stats
//             .get_or_insert(&token, (Arc::default(), Arc::default()));
//         let index = atom_index.load(SeqCst);
//         if index == 0 || nice == 0 {
//             let thd_idx = self.first_queue_iter.fetch_add(1, SeqCst) % self.num_threads;
//             let sender = &self.first_queues[thd_idx];
//             sender
//                 .send(ArcTask::new(
//                     task,
//                     sender.clone(),
//                     self.queues.clone(),
//                     atom_index.clone(),
//                     atom_elapsed.clone(),
//                     nice,
//                     token,
//                 ))
//                 .unwrap();
//         } else {
//             self.queues[index]
//                 .send(ArcTask::new(
//                     task,
//                     // don't care
//                     self.first_queues[0].clone(),
//                     self.queues.clone(),
//                     atom_index.clone(),
//                     atom_elapsed.clone(),
//                     nice,
//                     token,
//                 ))
//                 .unwrap();
//         }
//     }

//     pub fn new_from_config(f: Arc<dyn Fn() + Send + Sync + 'static>) -> ThreadPool {
//         ThreadPool::new(CONFIG.num_thread, f)
//     }
// }

// unsafe fn poll_with_timer(task: ArcTask, incoming_index: usize) {
//     let task_elapsed = task.0.elapsed.clone();
//     // adjust queue level
//     let mut index = task.0.index.load(SeqCst);
//     if incoming_index != index {
//         task.0.queues[index].send(clone_task(&*task.0)).unwrap();
//         return;
//     }
//     if task_elapsed.load(SeqCst) > TIME_FEEDBACK[index]
//         && index < TIME_FEEDBACK.len() - 1
//         && task.0.nice != 0
//     {
//         index += 1;
//         task.0.index.store(index, SeqCst);
//         task.0.queues[index].send(clone_task(&*task.0)).unwrap();
//         return;
//     }
//     // polling
//     let begin = Instant::now();
//     task.poll();
//     let elapsed = begin.elapsed().as_micros() as u64;
//     if incoming_index == 0 {
//         SMALL_TASK_CNT.fetch_add(elapsed, SeqCst);
//     } else {
//         HUGE_TASK_CNT.fetch_add(elapsed, SeqCst);
//     }
//     task_elapsed.fetch_add(elapsed, SeqCst);
// }

impl AdaptiveSpawn for ThreadPool {
    fn spawn_opt<Fut>(&self, f: Fut, opt: Options)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn(f, opt.token, opt.nice);
    }
}

impl Default for ThreadPool {
    fn default() -> ThreadPool {
        ThreadPool::new(num_cpus::get_physical(), Arc::new(|| {}))
    }
}

struct Task {
    task: UnsafeCell<BoxFuture<'static, ()>>,
    injectors: Arc<[Injector<ArcTask>]>,
    parker: Arc<Parker>,
    status: AtomicU8,
    // this token's total epalsed time
    stat: TaskStats,
    nice: u8,
    _token: u64,
}

#[derive(Clone)]
struct ArcTask(Arc<Task>);

const WAITING: u8 = 0; // --> POLLING
const POLLING: u8 = 1; // --> WAITING, REPOLL, or COMPLETE
const REPOLL: u8 = 2; // --> POLLING
const COMPLETE: u8 = 3; // No transitions out

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

impl ArcTask {
    fn new<F>(
        future: F,
        injectors: Arc<[Injector<ArcTask>]>,
        parker: Arc<Parker>,
        stat: TaskStats,
        nice: u8,
        token: u64,
    ) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            injectors,
            parker,
            status: AtomicU8::new(WAITING),
            stat,
            nice,
            _token: token,
        });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    unsafe fn poll(self) {
        self.0.status.store(POLLING, SeqCst);
        let waker = ManuallyDrop::new(waker(&*self.0));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(_) = (&mut *self.0.task.get()).poll_unpin(&mut cx) {
                break self.0.status.store(COMPLETE, SeqCst);
            }
            match self
                .0
                .status
                .compare_exchange(POLLING, WAITING, SeqCst, SeqCst)
            {
                Ok(_) => break,
                Err(_) => self.0.status.store(POLLING, SeqCst),
            }
        }
    }
}

unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

unsafe fn drop_raw(this: *const ()) {
    drop(task(this as *const Task))
}

unsafe fn wake_raw(this: *const ()) {
    let task = task(this as *const Task);
    let mut status = task.0.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match task
                    .0
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        let index = thread_rng().gen::<usize>() % 3;
                        task.0.injectors[index].push(clone_task(&*task.0));
                        task.0.parker.notify_one();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task
                    .0
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn wake_ref_raw(this: *const ()) {
    let task = ManuallyDrop::new(task(this as *const Task));
    let mut status = task.0.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match task
                    .0
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        let index = thread_rng().gen::<usize>() % 3;
                        task.0.injectors[index].push(clone_task(&*task.0));
                        task.0.parker.notify_one();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task
                    .0
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn task(future: *const Task) -> ArcTask {
    ArcTask(Arc::from_raw(future))
}

unsafe fn clone_task(future: *const Task) -> ArcTask {
    let task = task(future);
    forget(task.clone());
    task
}
