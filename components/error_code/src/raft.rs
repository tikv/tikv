// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use raft::Error;

use super::ErrorCodeExt;

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
            Error::Exists { .. } => EXISTS,
            Error::NotExists { .. } => NOT_EXISTS,
            Error::ConfChangeError(_) => CONF_CHANGE_ERROR,
            Error::RequestSnapshotDropped => unreachable!(),
        }
    }
}
