// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, io, net};

use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::{errorpb, metapb};
use protobuf::ProtobufError;
use raftstore::coprocessor::Error as CopError;
use thiserror::Error;
use tikv_util::codec;

use crate::store::PeerMsg;

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

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("read index not ready, reason {}, region {}", .reason, .region_id)]
    ReadIndexNotReady {
        reason: &'static str,
        region_id: u64,
    },

    #[error("raft entry is too large, region {}, entry size {}", .region_id, .entry_size)]
    RaftEntryTooLarge { region_id: u64, entry_size: u64 },

    #[error("to store id {}, mine {}", .to_store_id, .my_store_id)]
    StoreNotMatch { to_store_id: u64, my_store_id: u64 },

    #[error("region {0} not found")]
    RegionNotFound(u64, Option<PeerMsg>),

    #[error("region {0} not initialized yet")]
    RegionNotInitialized(u64),

    #[error("peer is not leader for region {0}, leader may {1:?}")]
    NotLeader(u64, Option<metapb::Peer>),

    #[error(
    "key {} is not in region key range [{}, {}) for region {}",
    log_wrappers::Value::key(.0),
    log_wrappers::Value::key(.1.get_start_key()),
    log_wrappers::Value::key(.1.get_end_key()),
    .1.get_id()
    )]
    KeyNotInRegion(Vec<u8>, metapb::Region),

    #[error("peer {} is not ready, safe_ts {}, region {}", .peer_id, .safe_ts, .region_id)]
    DataIsNotReady {
        region_id: u64,
        peer_id: u64,
        safe_ts: u64,
    },

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),

    // Following is for From other errors.
    #[error("Io {0}")]
    Io(#[from] io::Error),

    #[error("Engine {0:?}")]
    Engine(#[from] engine_traits::Error),

    #[error("Protobuf {0}")]
    Protobuf(#[from] ProtobufError),

    #[error("Codec {0}")]
    Codec(#[from] codec::Error),

    #[error("AddrParse {0}")]
    AddrParse(#[from] net::AddrParseError),

    #[error("Pd {0}")]
    Pd(#[from] pd_client::Error),

    #[error("Raft {0}")]
    Raft(#[from] raft::Error),

    #[error("RFEngine {0}")]
    RFEngineError(#[from] rfengine::Error),

    #[error("KVEngine {0}")]
    KVEngineError(#[from] kvengine::Error),

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

    #[error("Encryption {0}")]
    Encryption(#[from] encryption::Error),

    #[error("Deadline is exceeded")]
    DeadlineExceeded,

    #[error("send msg error")]
    SendPeerMsgError(PeerMsg),

    #[error("SstImporter {0}")]
    SstImporter(#[from] sst_importer::Error),

    #[error("txn types {0}")]
    TxnTypes(#[from] txn_types::Error),
}

impl From<Error> for errorpb::Error {
    fn from(err: Error) -> errorpb::Error {
        let mut errorpb = errorpb::Error::default();
        errorpb.set_message(format!("{}", err));

        match err {
            Error::RegionNotFound(region_id, _) => {
                errorpb.mut_region_not_found().set_region_id(region_id);
            }
            Error::NotLeader(region_id, leader) => {
                if let Some(leader) = leader {
                    errorpb.mut_not_leader().set_leader(leader);
                }
                errorpb.mut_not_leader().set_region_id(region_id);
            }
            Error::RaftEntryTooLarge {
                region_id,
                entry_size,
            } => {
                errorpb.mut_raft_entry_too_large().set_region_id(region_id);
                errorpb
                    .mut_raft_entry_too_large()
                    .set_entry_size(entry_size);
            }
            Error::StoreNotMatch {
                to_store_id,
                my_store_id,
            } => {
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
            Error::ReadIndexNotReady { reason, region_id } => {
                errorpb
                    .mut_read_index_not_ready()
                    .set_reason(reason.to_string());
                errorpb.mut_read_index_not_ready().set_region_id(region_id);
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
            Error::DataIsNotReady {
                region_id,
                peer_id,
                safe_ts,
            } => {
                let mut e = errorpb::DataIsNotReady::default();
                e.set_region_id(region_id);
                e.set_peer_id(peer_id);
                e.set_safe_ts(safe_ts);
                errorpb.set_data_is_not_ready(e);
            }
            Error::RegionNotInitialized(region_id) => {
                let mut e = errorpb::RegionNotInitialized::default();
                e.set_region_id(region_id);
                errorpb.set_region_not_initialized(e);
            }
            _ => {}
        };

        errorpb
    }
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::ReadIndexNotReady { .. } => error_code::raftstore::READ_INDEX_NOT_READY,
            Error::RaftEntryTooLarge { .. } => error_code::raftstore::ENTRY_TOO_LARGE,
            Error::StoreNotMatch { .. } => error_code::raftstore::STORE_NOT_MATCH,
            Error::RegionNotFound(..) => error_code::raftstore::REGION_NOT_FOUND,
            Error::NotLeader(..) => error_code::raftstore::NOT_LEADER,
            Error::StaleCommand => error_code::raftstore::STALE_COMMAND,
            Error::RegionNotInitialized(_) => error_code::raftstore::REGION_NOT_INITIALIZED,
            Error::KeyNotInRegion(..) => error_code::raftstore::KEY_NOT_IN_REGION,
            Error::Io(_) => error_code::raftstore::IO,
            Error::Engine(e) => e.error_code(),
            Error::Protobuf(_) => error_code::raftstore::PROTOBUF,
            Error::Codec(e) => e.error_code(),
            Error::AddrParse(_) => error_code::raftstore::ADDR_PARSE,
            Error::Pd(e) => e.error_code(),
            Error::RFEngineError(e) => error_code::engine::ENGINE,
            Error::KVEngineError(e) => error_code::engine::ENGINE,
            Error::Raft(e) => e.error_code(),
            Error::Timeout(_) => error_code::raftstore::TIMEOUT,
            Error::EpochNotMatch(..) => error_code::raftstore::EPOCH_NOT_MATCH,
            Error::Coprocessor(e) => e.error_code(),
            Error::Transport(_) => error_code::raftstore::TRANSPORT,
            Error::Encryption(e) => e.error_code(),
            #[cfg(feature = "prost-codec")]
            Error::ProstDecode(_) => error_code::raftstore::PROTOBUF,
            #[cfg(feature = "prost-codec")]
            Error::ProstEncode(_) => error_code::raftstore::PROTOBUF,
            Error::DataIsNotReady { .. } => error_code::raftstore::DATA_IS_NOT_READY,
            Error::DeadlineExceeded => error_code::raftstore::DEADLINE_EXCEEDED,

            Error::Other(_) => error_code::raftstore::UNKNOWN,
            Error::SendPeerMsgError(_) => error_code::raftstore::UNKNOWN,
            Error::SstImporter(e) => e.error_code(),
            Error::TxnTypes(e) => e.error_code(),
        }
    }
}
