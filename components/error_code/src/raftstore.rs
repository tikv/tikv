// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::errorpb;

use super::ErrorCodeExt;

define_error_codes!(
    "KV:Raftstore:",

    PROPOSAL_IN_MERGING_MODE => ("ProposalInMergingMode", "", ""),
    READ_INDEX_NOT_READY => ("ReadIndexNotReady", "", ""),
    ENTRY_TOO_LARGE => ("EntryTooLarge", "", ""),
    NOT_LEADER => ("NotLeader", "", ""),
    DISK_FULL => ("DiskFull", "", ""),
    STORE_NOT_MATCH => ("StoreNotMatch", "", ""),
    REGION_NOT_FOUND => ("RegionNotFound", "", ""),
    REGION_NOT_INITIALIZED => ("RegionNotInitialized", "", ""),
    KEY_NOT_IN_REGION => ("KeyNotInRegion", "", ""),
    STALE_COMMAND => ("StaleCommand", "", ""),
    TRANSPORT => ("Transport", "", ""),
    COPROCESSOR => ("Coprocessor", "", ""),
    IO => ("IO", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    ADDR_PARSE => ("AddressParse", "", ""),
    TIMEOUT => ("Timeout", "", ""),
    EPOCH_NOT_MATCH => ("EpochNotMatch", "", ""),
    UNKNOWN => ("Unknown", "", ""),
    SERVER_IS_BUSY => ("ServerIsBusy", "", ""),
    DATA_IS_NOT_READY => ("DataIsNotReady", "", ""),
    DEADLINE_EXCEEDED => ("DeadlineExceeded", "", ""),
    PENDING_PREPARE_MERGE => ("PendingPrepareMerge", "", ""),
    RECOVERY_IN_PROGRESS => ("RecoveryInProgress", "", ""),

    SNAP_ABORT => ("SnapAbort", "", ""),
    SNAP_TOO_MANY => ("SnapTooMany", "", ""),
    SNAP_UNKNOWN => ("SnapUnknown", "", "")
);

impl ErrorCodeExt for errorpb::Error {
    fn error_code(&self) -> ErrorCode {
        if self.has_not_leader() {
            NOT_LEADER
        } else if self.has_disk_full() {
            DISK_FULL
        } else if self.has_region_not_found() {
            REGION_NOT_FOUND
        } else if self.has_key_not_in_region() {
            KEY_NOT_IN_REGION
        } else if self.has_epoch_not_match() {
            EPOCH_NOT_MATCH
        } else if self.has_server_is_busy() {
            SERVER_IS_BUSY
        } else if self.has_stale_command() {
            STALE_COMMAND
        } else if self.has_store_not_match() {
            STORE_NOT_MATCH
        } else if self.has_raft_entry_too_large() {
            ENTRY_TOO_LARGE
        } else if self.has_read_index_not_ready() {
            READ_INDEX_NOT_READY
        } else if self.has_proposal_in_merging_mode() {
            PROPOSAL_IN_MERGING_MODE
        } else if self.has_data_is_not_ready() {
            DATA_IS_NOT_READY
        } else if self.has_recovery_in_progress() {
            RECOVERY_IN_PROGRESS
        } else {
            UNKNOWN
        }
    }
}
