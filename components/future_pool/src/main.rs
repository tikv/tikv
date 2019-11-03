#![feature(test)]

use adaptive_spawn::*;
use futures::channel::{mpsc, oneshot};
use futures::future::join_all;
use futures::join;
use futures::prelude::*;
use futures_timer::Delay;
use histogram::Histogram;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn idle(iter: u64) {
    let x = 1 << 31 - 1;
    for _ in 0..iter {
        black_box(black_box(x) % 257);
    }
}

async fn run_small_serial_task(spawn: &impl AdaptiveSpawn) {
    let token: u64 = rand::random();
    let options = Options { token, nice: 1 };
    const FUT_CNT: usize = 3;
    for _ in 0..FUT_CNT {
        let (tx, rx) = oneshot::channel();
        let fut = async move {
            idle(1000);
            Delay::new(Duration::from_micros(100)).await;
            idle(10000);
            Delay::new(Duration::from_micros(100)).await;
            idle(100000);
            Delay::new(Duration::from_micros(100)).await;
            tx.send(()).unwrap();
        };
        spawn.spawn_opt(fut, options);
        rx.await.unwrap();
    }
}

async fn run_small_parallel_task(spawn: &impl AdaptiveSpawn) {
    let token: u64 = rand::random();
    let options = Options { token, nice: 1 };
    const FUT_CNT: usize = 3;
    let (tx, mut rx) = mpsc::channel(FUT_CNT);
    for _ in 0..FUT_CNT {
        let mut tx = tx.clone();
        let fut = async move {
            idle(1000);
            Delay::new(Duration::from_micros(100)).await;
            idle(10000);
            Delay::new(Duration::from_micros(100)).await;
            idle(100000);
            Delay::new(Duration::from_micros(100)).await;
            tx.send(()).await.unwrap();
        };
        spawn.spawn_opt(fut, options);
    }
    for _ in 0..FUT_CNT {
        rx.next().await.unwrap();
    }
}

async fn run_big_serial_task(spawn: &impl AdaptiveSpawn) {
    let token: u64 = rand::random();
    let options = Options { token, nice: 128 };
    const FUT_CNT: usize = 100;
    for _ in 0..FUT_CNT {
        let (tx, rx) = oneshot::channel();
        let fut = async move {
            idle(1000);
            Delay::new(Duration::from_micros(100)).await;
            idle(10000);
            Delay::new(Duration::from_micros(100)).await;
            idle(20_000_000);
            Delay::new(Duration::from_micros(100)).await;
            tx.send(()).unwrap();
        };
        spawn.spawn_opt(fut, options);
        rx.await.unwrap();
    }
}

async fn run_big_parallel_task(spawn: &impl AdaptiveSpawn) {
    let token: u64 = rand::random();
    let options = Options { token, nice: 128 };
    const FUT_CNT: usize = 100;
    let (tx, mut rx) = mpsc::channel(FUT_CNT);
    for _ in 0..FUT_CNT {
        let mut tx = tx.clone();
        let fut = async move {
            idle(1000);
            Delay::new(Duration::from_micros(100)).await;
            idle(10000);
            Delay::new(Duration::from_micros(100)).await;
            idle(20_000_000);
            Delay::new(Duration::from_micros(100)).await;
            // for _ in 0..1_000 {
            //     idle(1_000);
            //     Delay::new(Duration::from_micros(100)).await;
            // }
            tx.send(()).await.unwrap();
        };
        spawn.spawn_opt(fut, options);
    }
    for _ in 0..FUT_CNT {
        rx.next().await.unwrap();
    }
}

#[runtime::main]
async fn main() {
    let spawn = future_pool::texn::ThreadPool::new(8, Arc::new(|| {}));
    const SMALL_CLIENT_CNT: usize = 128;
    const BIG_CLIENT_CNT: usize = 0;

    let spawn2 = spawn.clone();
    let small_histogram = Histogram::configure()
        .max_value(60_000_000)
        .precision(4)
        .build()
        .unwrap();
    let small_histogram = Arc::new(Mutex::new(small_histogram));
    let small_histogram2 = small_histogram.clone();
    let small_client = join_all((0..SMALL_CLIENT_CNT).map(move |_| {
        let spawn2 = spawn2.clone();
        let small_histogram2 = small_histogram2.clone();
        async move {
            loop {
                let begin = Instant::now();
                // run_small_serial_task(&spawn2).await;
                run_small_parallel_task(&spawn2).await;
                let elapsed = begin.elapsed().as_micros() as u64;
                small_histogram2.lock().unwrap().increment(elapsed).unwrap();
            }
        }
    }));
    let spawn2 = spawn.clone();
    let big_histogram = Histogram::configure()
        .max_value(600_000_000)
        .precision(4)
        .build()
        .unwrap();
    let big_histogram = Arc::new(Mutex::new(big_histogram));
    let big_histogram2 = big_histogram.clone();
    let big_client = join_all((0..BIG_CLIENT_CNT).map(move |_| {
        let spawn2 = spawn2.clone();
        let big_histogram2 = big_histogram2.clone();
        async move {
            loop {
                let begin = Instant::now();
                // run_big_serial_task(&spawn2).await;
                run_big_parallel_task(&spawn2).await;
                let elapsed = begin.elapsed().as_micros() as u64;
                big_histogram2.lock().unwrap().increment(elapsed).unwrap();
            }
        }
    }));
    fn print_output(tag: &str, histogram: &Histogram) -> Result<(), &'static str> {
        println!(
            "{} mean: {}us, 80th: {}us, 99th: {}us, max: {}us",
            tag,
            histogram.mean()?,
            histogram.percentile(80.0)?,
            histogram.percentile(99.0)?,
            histogram.maximum()?,
        );
        Ok(())
    }
    let output_stats = async move {
        loop {
            Delay::new(Duration::from_secs(5)).await;
            print_output("small", &*small_histogram.lock().unwrap()).ok();
            print_output("big", &*big_histogram.lock().unwrap()).ok();
        }
    };
    join!(small_client, big_client, output_stats);
}
