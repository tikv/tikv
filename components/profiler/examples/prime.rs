// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Profiling sample: Calculate prime numbers.
//!
//! ## Usage
//!
//! ### Build
//!
//! ```bash
//! cargo build --features "profiling" --example prime
//! ```
//!
//! (You may also want `--release` in real scenarios).
//!
//! ### Run using CPU Profiler
//!
//! ```bash
//! ../../target/debug/examples/prime
//! ```
//!
//! ### Run using Callgrind
//!
//! ```bash
//! valgrind --tool=callgrind --instr-atstart=no ../../target/debug/examples/prime
//! ```
//!
//! You must not run example via `valgrind cargo run ...`. The framework won't detect Callgrind!

#[inline(never)]
fn is_prime_number(v: usize, prime_numbers: &[usize]) -> bool {
    if v < 10000 {
        let r = prime_numbers.binary_search(&v);
        return r.is_ok();
    }

    for n in prime_numbers {
        if v % n == 0 {
            return false;
        }
    }

    true
}

#[inline(never)]
fn prepare_prime_numbers() -> Vec<usize> {
    // bootstrap: Generate a prime table of 0..10000
    let mut prime_number_table: [bool; 10000] = [true; 10000];
    prime_number_table[0] = false;
    prime_number_table[1] = false;
    for i in 2..10000 {
        if prime_number_table[i] {
            let mut v = i * 2;
            while v < 10000 {
                prime_number_table[v] = false;
                v += i;
            }
        }
    }
    let mut prime_numbers = vec![];
    for i in 2..10000 {
        if prime_number_table[i] {
            prime_numbers.push(i);
        }
    }
    prime_numbers
}

fn main() {
    let prime_numbers = prepare_prime_numbers();

    profiler::start("./prime.profile");

    let mut v = 0;
    for i in 2..50000 {
        if is_prime_number(i, &prime_numbers) {
            v += 1;
        }
    }

    profiler::stop();

    println!("Prime numbers: {}", v);
}
