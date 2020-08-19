// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

macro_rules! define_error_codes {
    ($prefix:literal,
        $($name:ident => ($suffix:literal, $description:literal, $workaround:literal)),+
    ) => {
        use crate::ErrorCode;
        $(pub const $name: ErrorCode = ErrorCode {
            code: concat!($prefix, $suffix),
            description: $description,
            workaround: $workaround,
        };)+
    };
}

pub mod codec;
pub mod coprocessor;
pub mod encryption;
pub mod engine;
pub mod pd;
pub mod raft;
pub mod raftstore;
pub mod sst_importer;
pub mod storage;

use std::fmt::{self, Display, Formatter};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct ErrorCode {
    pub code: &'static str,
    pub description: &'static str,
    pub workaround: &'static str,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.description)
    }
}

pub trait ErrorCodeExt {
    fn error_code(&self) -> ErrorCode;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_define_error_code() {
        define_error_codes!(
            "KV:Raftstore:",

            ENTRY_TOO_LARGE => ("EntryTooLarge", "", ""),
            NOT_LEADER => ("NotLeader", "", "")
        );

        assert_eq!(
            ENTRY_TOO_LARGE,
            ErrorCode {
                code: "KV:Raftstore:EntryTooLarge",
                description: "",
                workaround: "",
            }
        );
        assert_eq!(
            NOT_LEADER,
            ErrorCode {
                code: "KV:Raftstore:NotLeader",
                description: "",
                workaround: "",
            }
        );
    }
}
