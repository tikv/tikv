// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

macro_rules! ctx {
    () => {
        fn get_ctx(&self) -> &crate::storage::Context {
            &self.ctx
        }
        fn get_ctx_mut(&mut self) -> &mut crate::storage::Context {
            &mut self.ctx
        }
    };
}

/// Generate the struct definition and Debug, Display methods for a passed in
/// storage command.
/// Parameters:
/// cmd -> Used as the type name for the struct. It must match one of the variants of the enum
/// `storage::txns::commands::Command`.
/// cmd_ty -> The type that encapsulates the result of executing this command. It is passed as
/// a generic parameter to `TypedCommand`.
/// display -? Information needed to implement the `Display` trait for the command.
/// content -> The contents of the struct definition for the command.
macro_rules! command {
    (
        $(#[$outer_doc: meta])*
        $cmd: ident:
            cmd_ty => $cmd_ty: ty,
            display => $format_str: expr, ($($fields: ident$(.$sub_field:ident)?),*),
            content => {
                $($(#[$inner_doc:meta])* $arg: ident : $arg_ty: ty,)*
            }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            pub ctx: crate::storage::Context,
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            /// Return a `TypedCommand` encapsulating the result of executing this command.
            /// The result is of type `StorageCallbackType`
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: crate::storage::Context,
            ) -> TypedCommand<$cmd_ty> {
                Command::$cmd($cmd {
                        ctx,
                        $($arg,)*
                }).into()
            }
        }

        impl std::fmt::Display for $cmd {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    $format_str,
                    $(
                        self.$fields$(.$sub_field())?,
                    )*
                )
            }
        }

        impl std::fmt::Debug for $cmd {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self)
            }
        }
    }
}

macro_rules! ts {
    ($ts:ident) => {
        fn ts(&self) -> txn_types::TimeStamp {
            self.$ts
        }
    };
}

macro_rules! tag {
    ($tag:ident) => {
        fn tag(&self) -> crate::storage::metrics::CommandKind {
            crate::storage::metrics::CommandKind::$tag
        }

        fn incr_cmd_metric(&self) {
            crate::storage::metrics::KV_COMMAND_COUNTER_VEC_STATIC
                .$tag
                .inc();
        }
    };
}

macro_rules! write_bytes {
    ($field: ident) => {
        fn write_bytes(&self) -> usize {
            self.$field.as_encoded().len()
        }
    };
    ($field: ident: multiple) => {
        fn write_bytes(&self) -> usize {
            self.$field.iter().map(|x| x.as_encoded().len()).sum()
        }
    };
}

macro_rules! gen_lock {
    (empty) => {
        fn gen_lock(
            &self,
            _latches: &crate::storage::txn::latch::Latches,
        ) -> crate::storage::txn::latch::Lock {
            crate::storage::txn::latch::Lock::new(vec![])
        }
    };
    ($field: ident) => {
        fn gen_lock(
            &self,
            latches: &crate::storage::txn::latch::Latches,
        ) -> crate::storage::txn::latch::Lock {
            latches.gen_lock(std::iter::once(&self.$field))
        }
    };
    ($field: ident: multiple) => {
        fn gen_lock(
            &self,
            latches: &crate::storage::txn::latch::Latches,
        ) -> crate::storage::txn::latch::Lock {
            latches.gen_lock(&self.$field)
        }
    };
    ($field: ident: multiple$transform: tt) => {
        fn gen_lock(
            &self,
            latches: &crate::storage::txn::latch::Latches,
        ) -> crate::storage::txn::latch::Lock {
            #![allow(unused_parens)]
            let keys = self.$field.iter().map($transform);
            latches.gen_lock(keys)
        }
    };
}

macro_rules! command_method {
    ($name:ident, $return_ty: ty, $value: expr) => {
        fn $name(&self) -> $return_ty {
            $value
        }
    };
}
