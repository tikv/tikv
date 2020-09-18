// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::io;
use std::net;
use std::result;

use crossbeam::TrySendError;
#[cfg(feature = "prost-codec")]
use prost::{DecodeError, EncodeError};
use protobuf::ProtobufError;

use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::{errorpb, metapb};
use thiserror::Error;
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

#[derive(Debug, Error)]
pub enum Error {
    #[error("raft entry is too large, region {0}, entry size {1}")]
    RaftEntryTooLarge(u64, u64),
    #[error("to store id {0}, mine {1}")]
    StoreNotMatch(u64, u64),
    #[error("region {0} not found")]
    RegionNotFound(u64),
    #[error("region {0} not initialized yet")]
    RegionNotInitialized(u64),
    #[error("peer is not leader for region {0}, leader may {1:?}")]
    NotLeader(u64, Option<metapb::Peer>),
    #[error(
        "key {} is not in region key range [{}, {}) for region {}",
        hex::encode_upper(.0),
        hex::encode_upper(.1.get_start_key()),
        hex::encode_upper(.1.get_end_key()),
        .1.get_id()
    )]
    KeyNotInRegion(Vec<u8>, metapb::Region),
    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),

    // Following is for From other errors.
    #[error("Io {0}")]
    Io(#[from] io::Error),
    #[error("Engine {0:?}")]
    Engine(#[from] engine_traits::Error),
    #[error("Protobuf {0}")]
    Protobuf(#[from] ProtobufError),
    #[cfg(feature = "prost-codec")]
    #[error("DecodeError {0}")]
    ProstDecode(#[source] DecodeError),
    #[cfg(feature = "prost-codec")]
    #[error("EncodeError {0}")]
    ProstEncode(#[source] EncodeError),
    #[error("Codec {0}")]
    Codec(#[from] codec::Error),
    #[error("AddrParse {0}")]
    AddrParse(#[from] net::AddrParseError),
    #[error("Pd {0}")]
    Pd(#[from] pd_client::Error),
    #[error("Raft {0}")]
    Raft(#[from] raft::Error),
    #[error("Timeout {0}")]
    Timeout(String),
    #[error("EpochNotMatch {0}")]
    EpochNotMatch(String, Vec<metapb::Region>),
    #[error("stale command")]
    StaleCommand,
    #[error("Coprocessor {0}")]
    Coprocessor(#[from] CopError),
    #[error("Discard due to {0:?}")]
    Transport(DiscardReason),
    #[error("Snapshot {0}")]
    Snapshot(#[from] SnapError),
    #[error("SstImporter {0}")]
    SstImporter(#[from] sst_importer::Error),
    #[error("Encryption {0}")]
    Encryption(#[from] encryption::Error),
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
            Error::Engine(engine_traits::Error::NotInRange(key, region_id, start_key, end_key)) => {
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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::RaftEntryTooLarge(_, _) => error_code::raftstore::ENTRY_TOO_LARGE,
            Error::StoreNotMatch(_, _) => error_code::raftstore::STORE_NOT_MATCH,
            Error::RegionNotFound(_) => error_code::raftstore::REGION_NOT_FOUND,
            Error::NotLeader(_, _) => error_code::raftstore::NOT_LEADER,
            Error::StaleCommand => error_code::raftstore::STALE_COMMAND,
            Error::RegionNotInitialized(_) => error_code::raftstore::REGION_NOT_INITIALIZED,
            Error::KeyNotInRegion(_, _) => error_code::raftstore::KEY_NOT_IN_REGION,
            Error::Io(_) => error_code::raftstore::IO,
            Error::Engine(e) => e.error_code(),
            Error::Protobuf(_) => error_code::raftstore::PROTOBUF,
            Error::Codec(e) => e.error_code(),
            Error::AddrParse(_) => error_code::raftstore::ADDR_PARSE,
            Error::Pd(e) => e.error_code(),
            Error::Raft(e) => e.error_code(),
            Error::Timeout(_) => error_code::raftstore::TIMEOUT,
            Error::EpochNotMatch(_, _) => error_code::raftstore::EPOCH_NOT_MATCH,
            Error::Coprocessor(e) => e.error_code(),
            Error::Transport(_) => error_code::raftstore::TRANSPORT,
            Error::Snapshot(e) => e.error_code(),
            Error::SstImporter(e) => e.error_code(),
            Error::Encryption(e) => e.error_code(),
            #[cfg(feature = "prost-codec")]
            Error::ProstDecode(_) => error_code::raftstore::PROTOBUF,
            #[cfg(feature = "prost-codec")]
            Error::ProstEncode(_) => error_code::raftstore::PROTOBUF,

            Error::Other(_) => error_code::raftstore::UNKNOWN,
        }
    }
}
