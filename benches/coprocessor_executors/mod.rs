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
    let mut c = criterion::Criterion::default()
        .configure_from_args()
        .sample_size(10);

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
