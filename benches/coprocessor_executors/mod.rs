// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(specialization)]
#![feature(repeat_generic_slice)]

mod hash_aggr;
mod index_scan;
mod integrated;
mod selection;
mod simple_aggr;
mod stream_aggr;
mod table_scan;
mod top_n;
mod util;

fn main() {
    let mut cpu_set = nix::sched::CpuSet::new();
    cpu_set.set(0).unwrap();
    nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).unwrap();

    let mut c = criterion::Criterion::default()
        .configure_from_args();

    util::fixture::bench(&mut c);
    table_scan::bench(&mut c);
    index_scan::bench(&mut c);
    selection::bench(&mut c);
    simple_aggr::bench(&mut c);
    hash_aggr::bench(&mut c);
    stream_aggr::bench(&mut c);
    top_n::bench(&mut c);
    integrated::bench(&mut c);

    c.final_summary();
}
