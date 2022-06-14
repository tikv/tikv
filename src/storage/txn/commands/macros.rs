// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
macro_rules! ctx {
    () => {
        fn get_ctx(&self) -> &crate::storage::Context {
            &self.ctx
        }
        fn get_ctx_mut(&mut self) -> &mut crate::storage::Context {
            &mut self.ctx
        }
        fn deadline(&self) -> ::tikv_util::deadline::Deadline {
            self.deadline
        }
    };
}

/// Generate the struct definition and Debug, Display methods for a passed-in
/// storage command.
/// Parameters:
/// cmd -> Used as the type name for the generated struct. A variant of the
/// enum `storage::txns::commands::Command` must exist whose name matches the
/// value of `cmd` and which accepts one parameter whose type name matches
/// the value of `cmd`.
/// cmd_ty -> The type of the result of executing this command.
/// display -> Information needed to implement the `Display` trait for the command.
/// content -> The fields of the struct definition for the command.
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
            pub deadline: ::tikv_util::deadline::Deadline,
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            /// Return a `TypedCommand` that encapsulates the result of executing this command.
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: crate::storage::Context,
            ) -> TypedCommand<$cmd_ty> {
                let execution_duration_limit = if ctx.max_execution_duration_ms == 0 {
                    crate::storage::txn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT
                } else {
                    ::std::time::Duration::from_millis(ctx.max_execution_duration_ms)
                };
                let deadline = ::tikv_util::deadline::Deadline::from_now(execution_duration_limit);
                Command::$cmd($cmd {
                        ctx,
                        deadline,
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
    };
    (
        $(#[$outer_doc: meta])*
        $cmd: ident:
            cmd_ty => $cmd_ty: ty,
            content => {
                $($(#[$inner_doc:meta])* $arg: ident : $arg_ty: ty,)*
            }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            pub ctx: crate::storage::Context,
            pub deadline: ::tikv_util::deadline::Deadline,
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            /// Return a `TypedCommand` that encapsulates the result of executing this command.
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: crate::storage::Context,
            ) -> TypedCommand<$cmd_ty> {
                let execution_duration_limit = if ctx.max_execution_duration_ms == 0 {
                    crate::storage::txn::scheduler::DEFAULT_EXECUTION_DURATION_LIMIT
                } else {
                    ::std::time::Duration::from_millis(ctx.max_execution_duration_ms)
                };
                let deadline = ::tikv_util::deadline::Deadline::from_now(execution_duration_limit);
                Command::$cmd($cmd {
                        ctx,
                        deadline,
                        $($arg,)*
                }).into()
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
        fn gen_lock(&self) -> crate::storage::txn::latch::Lock {
            crate::storage::txn::latch::Lock::new::<(), _>(vec![])
        }
    };
    ($field: ident) => {
        fn gen_lock(&self) -> crate::storage::txn::latch::Lock {
            crate::storage::txn::latch::Lock::new(std::iter::once(&self.$field))
        }
    };
    ($field: ident: multiple) => {
        fn gen_lock(&self) -> crate::storage::txn::latch::Lock {
            crate::storage::txn::latch::Lock::new(&self.$field)
        }
    };
    ($field: ident: multiple$transform: tt) => {
        fn gen_lock(&self) -> crate::storage::txn::latch::Lock {
            #![allow(unused_parens)]
            let keys = self.$field.iter().map($transform);
            crate::storage::txn::latch::Lock::new(keys)
        }
    };
}

macro_rules! property {
    ($property:ident) => {
        fn $property(&self) -> bool {
            true
        }
    };
}
