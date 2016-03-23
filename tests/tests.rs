#![feature(std_panic, recover)]
#![feature(plugin)]
#![plugin(clippy)]
#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate log;
extern crate protobuf;
extern crate env_logger;
#[macro_use]
extern crate tikv;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate uuid;
extern crate mio;
extern crate kvproto;

mod test_raft;
mod test_raft_snap;
mod test_raft_paper;
mod test_raft_flow_control;
mod test_raw_node;
mod raftserver;
mod storage;
