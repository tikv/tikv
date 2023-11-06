// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.



use futures::{stream::StreamExt};



use test_backup::disk_snap::{Suite};
use test_raftstore::must_contains_error;

#[test]
fn test_basic() {
    let mut suite = Suite::new(1);
    let mut call = suite.prepare_backup(1);
    call.prepare(60);
    let resp = suite.split(b"k");
    println!("{:?}", resp.response.get_header().get_error());
    must_contains_error(
        &resp.response,
        "rejecting proposing admin commands while preparing snapshot backup",
    );
}
