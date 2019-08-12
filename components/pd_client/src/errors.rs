// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::result;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ClusterBootstrapped(cluster_id: u64) {
            description("cluster bootstrap error")
            display("cluster {} is already bootstrapped", cluster_id)
        }
        ClusterNotBootstrapped(cluster_id: u64) {
            description("cluster not bootstrap error")
            display("cluster {} is not bootstrapped", cluster_id)
        }
        Incompatible {
            description("compatible error")
            display("feature is not supported in other cluster components")
        }
        Grpc(err: grpcio::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
        RegionNotFound(key: Vec<u8>) {
            description("region is not found")
            display("region is not found for key {}", hex::encode_upper(key))
        }
        StoreTombstone(msg: String) {
            description("store is tombstone")
            display("store is tombstone {:?}", msg)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
