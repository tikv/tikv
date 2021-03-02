// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(min_specialization)]

mod hash_aggr;
mod index_scan;
mod integrated;
mod selection;
mod simple_aggr;
mod stream_aggr;
mod table_scan;
mod top_n;
mod util;

fn execute<M: criterion::measurement::Measurement + 'static>(c: &mut criterion::Criterion<M>) {
    util::fixture::bench(c);
    table_scan::bench(c);
    index_scan::bench(c);
    selection::bench(c);
    simple_aggr::bench(c);
    hash_aggr::bench(c);
    stream_aggr::bench(c);
    top_n::bench(c);
    integrated::bench(c);

    c.final_summary();
}

#[cfg(target_os = "linux")]
fn run_bench(measurement: &str) {
    use criterion_perf_events::Perf;
    use perfcnt::linux::HardwareEventType as Hardware;
    use perfcnt::linux::PerfCounterBuilderLinux as Builder;

    match measurement {
        "TOT_INS" => {
            let perf_event_builder = Builder::from_hardware_event(Hardware::Instructions);
            let mut c = criterion::Criterion::default()
                .with_measurement(Perf::new(perf_event_builder))
                .configure_from_args();
            execute(&mut c);
        }
        "CPU_TIME" => {
            let mut c = criterion::Criterion::default()
                .with_measurement(criterion_cpu_time::PosixTime::UserTime)
                .configure_from_args();
            execute(&mut c);
        }
        _ => {
            panic!("unknown measurement");
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn run_bench(measurement: &str) {
    match measurement {
        "CPU_TIME" => {
            let mut c = criterion::Criterion::default()
                .with_measurement(criterion_cpu_time::PosixTime::UserTime)
                .configure_from_args();
            execute(&mut c);
        }
        _ => {
            panic!("unknown measurement");
        }
    }
}

fn main() {
    let measurement = std::env::var("MEASUREMENT").unwrap_or_else(|_| String::from("CPU_TIME"));

    run_bench(&measurement);
}
