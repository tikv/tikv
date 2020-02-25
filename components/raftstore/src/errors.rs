// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::io;
use std::net;
use std::result;

use crossbeam::TrySendError;
#[cfg(feature = "prost-codec")]
use prost::{DecodeError, EncodeError};
use protobuf::ProtobufError;

use kvproto::{errorpb, metapb};
use pd_client;
use raft;
use tikv_util::codec;

use super::coprocessor::Error as CopError;
use super::store::SnapError;

pub const RAFTSTORE_IS_BUSY: &str = "raftstore is busy";

/// Describes why a message is discarded.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiscardReason {
    /// Channel is disconnected, message can't be delivered.
    Disconnected,
    /// Message is dropped due to some filter rules, usually in tests.
    Filtered,
    /// Channel runs out of capacity, message can't be delivered.
    Full,
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RaftEntryTooLarge(region_id: u64, entry_size: u64) {
            description("raft entry is too large")
            display("raft entry is too large, region {}, entry size {}", region_id, entry_size)
        }
        StoreNotMatch(to_store_id: u64, my_store_id: u64) {
            description("store is not match")
            display("to store id {}, mine {}", to_store_id, my_store_id)
        }
        RegionNotFound(region_id: u64) {
            description("region is not found")
            display("region {} not found", region_id)
        }
        RegionNotInitialized(region_id: u64) {
            description("region has not been initialized yet.")
            display("region {} not initialized yet", region_id)
        }
        NotLeader(region_id: u64, leader: Option<metapb::Peer>) {
            description("peer is not leader")
            display("peer is not leader for region {}, leader may {:?}", region_id, leader)
        }
        KeyNotInRegion(key: Vec<u8>, region: metapb::Region) {
            description("key is not in region")
            display("key {} is not in region key range [{}, {}) for region {}",
                    hex::encode_upper(key),
                    hex::encode_upper(region.get_start_key()),
                    hex::encode_upper(region.get_end_key()),
                    region.get_id())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }

        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("Io {}", err)
        }
        Engine(err: engine::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
        }
        EngineTraits(err: engine_traits::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstDecode(err: DecodeError) {
            cause(err)
            description(err.description())
            display("DecodeError {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstEncode(err: EncodeError) {
            cause(err)
            description(err.description())
            display("EncodeError {}", err)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
            display("Codec {}", err)
        }
        AddrParse(err: net::AddrParseError) {
            from()
            cause(err)
            description(err.description())
            display("AddrParse {}", err)
        }
        Pd(err: pd_client::Error) {
            from()
            cause(err)
            description(err.description())
            display("Pd {}", err)
        }
        Raft(err: raft::Error) {
            from()
            cause(err)
            description(err.description())
            display("Raft {}", err)
        }
        Timeout(msg: String) {
            description("request timeout")
            display("Timeout {}", msg)
        }
        EpochNotMatch(msg: String, new_regions: Vec<metapb::Region>) {
            description("region epoch is not match")
            display("EpochNotMatch {}", msg)
        }
        StaleCommand {
            description("stale command")
        }
        Coprocessor(err: CopError) {
            from()
            cause(err)
            description(err.description())
            display("Coprocessor {}", err)
        }
        Transport(reason: DiscardReason) {
            description("failed to send a message")
            display("Discard due to {:?}", reason)
        }
        Snapshot(err: SnapError) {
            from()
            cause(err)
            description(err.description())
            display("Snapshot {}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for errorpb::Error {
    fn from(err: Error) -> errorpb::Error {
        let mut errorpb = errorpb::Error::default();
        errorpb.set_message(format!("{}", err));

        match err {
            Error::RegionNotFound(region_id) => {
                errorpb.mut_region_not_found().set_region_id(region_id);
            }
            Error::NotLeader(region_id, leader) => {
                if let Some(leader) = leader {
                    errorpb.mut_not_leader().set_leader(leader);
                }
                errorpb.mut_not_leader().set_region_id(region_id);
            }
            Error::RaftEntryTooLarge(region_id, entry_size) => {
                errorpb.mut_raft_entry_too_large().set_region_id(region_id);
                errorpb
                    .mut_raft_entry_too_large()
                    .set_entry_size(entry_size);
            }
            Error::StoreNotMatch(to_store_id, my_store_id) => {
                errorpb
                    .mut_store_not_match()
                    .set_request_store_id(to_store_id);
                errorpb
                    .mut_store_not_match()
                    .set_actual_store_id(my_store_id);
            }
            Error::KeyNotInRegion(key, region) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb
                    .mut_key_not_in_region()
                    .set_region_id(region.get_id());
                errorpb
                    .mut_key_not_in_region()
                    .set_start_key(region.get_start_key().to_vec());
                errorpb
                    .mut_key_not_in_region()
                    .set_end_key(region.get_end_key().to_vec());
            }
            Error::EpochNotMatch(_, new_regions) => {
                let mut e = errorpb::EpochNotMatch::default();
                e.set_current_regions(new_regions.into());
                errorpb.set_epoch_not_match(e);
            }
            Error::StaleCommand => {
                errorpb.set_stale_command(errorpb::StaleCommand::default());
            }
            Error::Transport(reason) if reason == DiscardReason::Full => {
                let mut server_is_busy_err = errorpb::ServerIsBusy::default();
                server_is_busy_err.set_reason(RAFTSTORE_IS_BUSY.to_owned());
                errorpb.set_server_is_busy(server_is_busy_err);
            }
            Error::Engine(engine::Error::NotInRange(key, region_id, start_key, end_key)) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb.mut_key_not_in_region().set_region_id(region_id);
                errorpb
                    .mut_key_not_in_region()
                    .set_start_key(start_key.to_vec());
                errorpb
                    .mut_key_not_in_region()
                    .set_end_key(end_key.to_vec());
            }
            _ => {}
        };

        errorpb
    }
}

impl<T> From<TrySendError<T>> for Error {
    #[inline]
    fn from(e: TrySendError<T>) -> Error {
        match e {
            TrySendError::Full(_) => Error::Transport(DiscardReason::Full),
            TrySendError::Disconnected(_) => Error::Transport(DiscardReason::Disconnected),
        }
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Error {
        Error::ProstEncode(err.into())
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Error {
        Error::ProstDecode(err.into())
    }
}
