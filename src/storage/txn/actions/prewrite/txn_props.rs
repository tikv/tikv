use crate::storage::mvcc::TimeStamp;

#[derive(Clone, Debug)]
pub struct TransactionProperties<'a> {
    pub start_ts: TimeStamp,
    pub kind: TransactionKind,
    pub commit_kind: CommitKind,
    pub primary: &'a [u8],
    pub txn_size: u64,
    pub lock_ttl: u64,
    pub min_commit_ts: TimeStamp,
}

impl<'a> TransactionProperties<'a> {
    pub(crate) fn max_commit_ts(&self) -> TimeStamp {
        match &self.commit_kind {
            CommitKind::TwoPc => unreachable!(),
            CommitKind::OnePc(ts) => *ts,
            CommitKind::Async(ts) => *ts,
        }
    }

    pub(crate) fn is_pessimistic(&self) -> bool {
        match &self.kind {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    pub(crate) fn for_update_ts(&self) -> TimeStamp {
        match &self.kind {
            TransactionKind::Optimistic(_) => TimeStamp::zero(),
            TransactionKind::Pessimistic(ts) => *ts,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommitKind {
    TwoPc,
    /// max_commit_ts
    OnePc(TimeStamp),
    /// max_commit_ts
    Async(TimeStamp),
}

#[derive(Clone, Debug)]
pub enum TransactionKind {
    // bool is skip_constraint_check
    Optimistic(bool),
    // for_update_ts
    Pessimistic(TimeStamp),
}

#[cfg(test)]
pub mod test_util {
    use super::*;

    #[derive(Clone)]
    pub struct TxnPropsBuilder<'a> {
        primary: &'a [u8],
        start_ts: TimeStamp,
        kind: TransactionKind,
        commit_kind: CommitKind,
        txn_size: u64,
        lock_ttl: u64,
        min_commit_ts: TimeStamp,
    }

    impl<'a> TxnPropsBuilder<'a> {
        pub fn optimistic(primary: &'a [u8], start_ts: TimeStamp) -> Self {
            TxnPropsBuilder {
                start_ts,
                kind: TransactionKind::Optimistic(false),
                commit_kind: CommitKind::TwoPc,
                primary,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
            }
        }

        pub fn pessimistic(
            primary: &'a [u8],
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        ) -> Self {
            TxnPropsBuilder {
                start_ts,
                kind: TransactionKind::Pessimistic(for_update_ts),
                commit_kind: CommitKind::TwoPc,
                primary,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
            }
        }

        pub fn build(self) -> TransactionProperties<'a> {
            TransactionProperties {
                start_ts: self.start_ts,
                kind: self.kind,
                commit_kind: self.commit_kind,
                primary: self.primary,
                txn_size: self.txn_size,
                lock_ttl: self.lock_ttl,
                min_commit_ts: self.min_commit_ts,
            }
        }
    }
}
