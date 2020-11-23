use crate::storage::mvcc::{ErrorInner, MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use txn_types::{Key, TimeStamp, Write, WriteType};

/// Checks the existence of the key according to `should_not_exist`.
/// If not, returns an `AlreadyExist` error.
pub(crate) fn check_data_constraint<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    should_not_exist: bool,
    write: &Write,
    write_commit_ts: TimeStamp,
    key: &Key,
) -> MvccResult<()> {
    if !should_not_exist || write.write_type == WriteType::Delete {
        return Ok(());
    }

    // The current key exists under any of the following conditions:
    // 1.The current write type is `PUT`
    // 2.The current write type is `Rollback` or `Lock`, and the key have an older version.
    if write.write_type == WriteType::Put || txn.key_exist(&key, write_commit_ts.prev())? {
        return Err(ErrorInner::AlreadyExist { key: key.to_raw()? }.into());
    }
    Ok(())
}
