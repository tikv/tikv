macro_rules! define_error_codes {
    ($prefix:literal,
        $($name:ident => ($suffix:literal, $description:literal, $workaround:literal)),+
    ) => {
        use crate::error_code::ErrorCode;
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
pub mod server;
pub mod sst_importer;
pub mod storage;

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct ErrorCode {
    pub code: &'static str,
    pub description: &'static str,
    pub workaround: &'static str,
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
