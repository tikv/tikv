use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, Result, TimeStamp};
use crate::{command, command_method, ctx, gen_lock, tag, ts};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Mutation;

command! {
    /// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](Command::Commit)
    /// or a [`Rollback`](Command::Rollback) should follow.
    PrewritePessimistic:
        cmd_ty => Vec<Result<()>>,
        display => "kv::command::prewrite_pessimistic mutations({}) @ {} | {:?}", (mutations.len, start_ts, ctx),
        content => {
            /// The set of mutations to apply; the bool = is pessimistic lock.
            mutations: Vec<(Mutation, bool)>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for PrewritePessimistic {
    ctx!();
    tag!(prewrite);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for (m, _) in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|(x, _)| x.key()));
}
