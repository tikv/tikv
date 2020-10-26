// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::ErrorCodeExt;
use raft::Error;

define_error_codes!(
    "KV:Raft:",

    IO => ("Io", "", ""),
    STORE => ("Store", "", ""),
    STEP_LOCAL_MSG => ("StepLocalMsg", "", ""),
    STEP_PEER_NOT_FOUND => ("StepPeerNotFound", "", ""),
    PROPOSAL_DROPPED => ("ProposalDropped", "", ""),
    CONFIG_INVALID => ("ConfigInvalid", "", ""),
    CODEC_ERROR => ("CodecError", "", ""),
    EXISTS => ("Exists", "", ""),
    NOT_EXISTS => ("NotExists", "", ""),
    REQUEST_SNAPSHOT_DROPPED => ("RequestSnapshotDropped", "", ""),
    CONF_CHANGE_ERROR => ("ConfChangeError", "", "")
);

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => IO,
            Error::Store(_) => STORE,
            Error::StepLocalMsg => STEP_LOCAL_MSG,
            Error::StepPeerNotFound => STEP_PEER_NOT_FOUND,
            Error::ProposalDropped => PROPOSAL_DROPPED,
            Error::ConfigInvalid(_) => CONFIG_INVALID,
            Error::CodecError(_) => CODEC_ERROR,
            Error::Exists(_, _) => EXISTS,
            Error::NotExists(_, _) => NOT_EXISTS,
            Error::RequestSnapshotDropped => REQUEST_SNAPSHOT_DROPPED,
            Error::ConfChangeError(_) => CONF_CHANGE_ERROR,
        }
    }
}
