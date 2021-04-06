// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::io;
use std::net;
use std::result;

use crossbeam::TrySendError;
use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::{errorpb, metapb};
#[cfg(feature = "prost-codec")]
use prost::{DecodeError, EncodeError};
use protobuf::ProtobufError;
use quick_error::quick_error;
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
        ProposalInMergingMode(region_id: u64) {
            display("{} peer in merging mode, can't do proposal", region_id)
        }
        ReadIndexNotReady(reason: &'static str, region_id: u64) {
            display("read index not ready, reason {}, region {}", reason, region_id)
        }
        RaftEntryTooLarge(region_id: u64, entry_size: u64) {
            display("raft entry is too large, region {}, entry size {}", region_id, entry_size)
        }
        StoreNotMatch(to_store_id: u64, my_store_id: u64) {
            display("to store id {}, mine {}", to_store_id, my_store_id)
        }
        RegionNotFound(region_id: u64) {
            display("region {} not found", region_id)
        }
        RegionNotInitialized(region_id: u64) {
            display("region {} not initialized yet", region_id)
        }
        NotLeader(region_id: u64, leader: Option<metapb::Peer>) {
            display("peer is not leader for region {}, leader may {:?}", region_id, leader)
        }
        KeyNotInRegion(key: Vec<u8>, region: metapb::Region) {
            display("key {} is not in region key range [{}, {}) for region {}",
                log_wrappers::Value::key(key),
                log_wrappers::Value::key(region.get_start_key()),
                log_wrappers::Value::key(region.get_end_key()),
                region.get_id())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }

        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            display("Io {}", err)
        }
        Engine(err: engine_traits::Error) {
            from()
            display("Engine {:?}", err)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            display("Protobuf {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstDecode(err: DecodeError) {
            cause(err)
            display("DecodeError {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstEncode(err: EncodeError) {
            cause(err)
            display("EncodeError {}", err)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            display("Codec {}", err)
        }
        AddrParse(err: net::AddrParseError) {
            from()
            cause(err)
            display("AddrParse {}", err)
        }
        Pd(err: pd_client::Error) {
            from()
            cause(err)
            display("Pd {}", err)
        }
        Raft(err: raft::Error) {
            from()
            cause(err)
            display("Raft {}", err)
        }
        Timeout(msg: String) {
            display("Timeout {}", msg)
        }
        EpochNotMatch(msg: String, new_regions: Vec<metapb::Region>) {
            display("EpochNotMatch {}", msg)
        }
        StaleCommand {
            display("stale command")
        }
        Coprocessor(err: CopError) {
            from()
            cause(err)
            display("Coprocessor {}", err)
        }
        Transport(reason: DiscardReason) {
            display("Discard due to {:?}", reason)
        }
        Snapshot(err: SnapError) {
            from()
            cause(err)
            display("Snapshot {}", err)
        }
        SstImporter(err: sst_importer::Error) {
            from()
            cause(err)
            display("SstImporter {}", err)
        }
        Encryption(err: encryption::Error) {
            from()
            display("Encryption {}", err)
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
            Error::ReadIndexNotReady(reason, region_id) => {
                errorpb
                    .mut_read_index_not_ready()
                    .set_reason(reason.to_string());
                errorpb.mut_read_index_not_ready().set_region_id(region_id);
            }
            Error::ProposalInMergingMode(region_id) => {
                errorpb
                    .mut_proposal_in_merging_mode()
                    .set_region_id(region_id);
            }
            Error::Transport(reason) if reason == DiscardReason::Full => {
                let mut server_is_busy_err = errorpb::ServerIsBusy::default();
                server_is_busy_err.set_reason(RAFTSTORE_IS_BUSY.to_owned());
                errorpb.set_server_is_busy(server_is_busy_err);
            }
            Error::Engine(engine_traits::Error::NotInRange {
                key,
                region_id,
                start,
                end,
            }) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb.mut_key_not_in_region().set_region_id(region_id);
                errorpb
                    .mut_key_not_in_region()
                    .set_start_key(start.to_vec());
                errorpb.mut_key_not_in_region().set_end_key(end.to_vec());
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
            Error::ProposalInMergingMode(_) => error_code::raftstore::PROPOSAL_IN_MERGING_MODE,
            Error::ReadIndexNotReady(_, _) => error_code::raftstore::READ_INDEX_NOT_READY,
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
