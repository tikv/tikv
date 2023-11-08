// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub fn to_engine_trait_status(s: tirocks::Status) -> engine_traits::Status {
    let code = match s.code() {
        tirocks::Code::kOk => engine_traits::Code::Ok,
        tirocks::Code::kNotFound => engine_traits::Code::NotFound,
        tirocks::Code::kCorruption => engine_traits::Code::Corruption,
        tirocks::Code::kNotSupported => engine_traits::Code::NotSupported,
        tirocks::Code::kInvalidArgument => engine_traits::Code::InvalidArgument,
        tirocks::Code::kIOError => engine_traits::Code::IoError,
        tirocks::Code::kMergeInProgress => engine_traits::Code::MergeInProgress,
        tirocks::Code::kIncomplete => engine_traits::Code::Incomplete,
        tirocks::Code::kShutdownInProgress => engine_traits::Code::ShutdownInProgress,
        tirocks::Code::kTimedOut => engine_traits::Code::TimedOut,
        tirocks::Code::kAborted => engine_traits::Code::Aborted,
        tirocks::Code::kBusy => engine_traits::Code::Busy,
        tirocks::Code::kExpired => engine_traits::Code::Expired,
        tirocks::Code::kTryAgain => engine_traits::Code::TryAgain,
        tirocks::Code::kCompactionTooLarge => engine_traits::Code::CompactionTooLarge,
        tirocks::Code::kColumnFamilyDropped => engine_traits::Code::ColumnFamilyDropped,
        tirocks::Code::kMaxCode => unreachable!(),
    };
    let sev = match s.severity() {
        tirocks::Severity::kNoError => engine_traits::Severity::NoError,
        tirocks::Severity::kSoftError => engine_traits::Severity::SoftError,
        tirocks::Severity::kHardError => engine_traits::Severity::HardError,
        tirocks::Severity::kFatalError => engine_traits::Severity::FatalError,
        tirocks::Severity::kUnrecoverableError => engine_traits::Severity::UnrecoverableError,
        tirocks::Severity::kMaxSeverity => unreachable!(),
    };
    let sub_code = match s.sub_code() {
        tirocks::SubCode::kNone => engine_traits::SubCode::None,
        tirocks::SubCode::kMutexTimeout => engine_traits::SubCode::MutexTimeout,
        tirocks::SubCode::kLockTimeout => engine_traits::SubCode::LockTimeout,
        tirocks::SubCode::kLockLimit => engine_traits::SubCode::LockLimit,
        tirocks::SubCode::kNoSpace => engine_traits::SubCode::NoSpace,
        tirocks::SubCode::kDeadlock => engine_traits::SubCode::Deadlock,
        tirocks::SubCode::kStaleFile => engine_traits::SubCode::StaleFile,
        tirocks::SubCode::kMemoryLimit => engine_traits::SubCode::MemoryLimit,
        tirocks::SubCode::kSpaceLimit => engine_traits::SubCode::SpaceLimit,
        tirocks::SubCode::kPathNotFound => engine_traits::SubCode::PathNotFound,
        tirocks::SubCode::KMergeOperandsInsufficientCapacity => {
            engine_traits::SubCode::MergeOperandsInsufficientCapacity
        }
        tirocks::SubCode::kManualCompactionPaused => engine_traits::SubCode::ManualCompactionPaused,
        tirocks::SubCode::kOverwritten => engine_traits::SubCode::Overwritten,
        tirocks::SubCode::kTxnNotPrepared => engine_traits::SubCode::TxnNotPrepared,
        tirocks::SubCode::kIOFenced => engine_traits::SubCode::IoFenced,
        tirocks::SubCode::kMaxSubCode => unreachable!(),
    };
    let mut es = match s.state().map(|s| String::from_utf8_lossy(s).into_owned()) {
        Some(msg) => engine_traits::Status::with_error(code, msg),
        None => engine_traits::Status::with_code(code),
    };
    es.set_severity(sev).set_sub_code(sub_code);
    es
}

/// A function that will transform a rocksdb error to engine trait error.
///
/// r stands for rocksdb, e stands for engine_trait.
pub fn r2e(s: tirocks::Status) -> engine_traits::Error {
    engine_traits::Error::Engine(to_engine_trait_status(s))
}

/// A function that will transform a engine trait error to rocksdb error.
///
/// r stands for rocksdb, e stands for engine_trait.
pub fn e2r(s: engine_traits::Error) -> tirocks::Status {
    let s = match s {
        engine_traits::Error::Engine(s) => s,
        // Any better options than IOError?
        _ => return tirocks::Status::with_error(tirocks::Code::kIOError, format!("{}", s)),
    };
    let code = match s.code() {
        engine_traits::Code::Ok => tirocks::Code::kOk,
        engine_traits::Code::NotFound => tirocks::Code::kNotFound,
        engine_traits::Code::Corruption => tirocks::Code::kCorruption,
        engine_traits::Code::NotSupported => tirocks::Code::kNotSupported,
        engine_traits::Code::InvalidArgument => tirocks::Code::kInvalidArgument,
        engine_traits::Code::IoError => tirocks::Code::kIOError,
        engine_traits::Code::MergeInProgress => tirocks::Code::kMergeInProgress,
        engine_traits::Code::Incomplete => tirocks::Code::kIncomplete,
        engine_traits::Code::ShutdownInProgress => tirocks::Code::kShutdownInProgress,
        engine_traits::Code::TimedOut => tirocks::Code::kTimedOut,
        engine_traits::Code::Aborted => tirocks::Code::kAborted,
        engine_traits::Code::Busy => tirocks::Code::kBusy,
        engine_traits::Code::Expired => tirocks::Code::kExpired,
        engine_traits::Code::TryAgain => tirocks::Code::kTryAgain,
        engine_traits::Code::CompactionTooLarge => tirocks::Code::kCompactionTooLarge,
        engine_traits::Code::ColumnFamilyDropped => tirocks::Code::kColumnFamilyDropped,
    };
    let sev = match s.severity() {
        engine_traits::Severity::NoError => tirocks::Severity::kNoError,
        engine_traits::Severity::SoftError => tirocks::Severity::kSoftError,
        engine_traits::Severity::HardError => tirocks::Severity::kHardError,
        engine_traits::Severity::FatalError => tirocks::Severity::kFatalError,
        engine_traits::Severity::UnrecoverableError => tirocks::Severity::kUnrecoverableError,
    };
    let sub_code = match s.sub_code() {
        engine_traits::SubCode::None => tirocks::SubCode::kNone,
        engine_traits::SubCode::MutexTimeout => tirocks::SubCode::kMutexTimeout,
        engine_traits::SubCode::LockTimeout => tirocks::SubCode::kLockTimeout,
        engine_traits::SubCode::LockLimit => tirocks::SubCode::kLockLimit,
        engine_traits::SubCode::NoSpace => tirocks::SubCode::kNoSpace,
        engine_traits::SubCode::Deadlock => tirocks::SubCode::kDeadlock,
        engine_traits::SubCode::StaleFile => tirocks::SubCode::kStaleFile,
        engine_traits::SubCode::MemoryLimit => tirocks::SubCode::kMemoryLimit,
        engine_traits::SubCode::SpaceLimit => tirocks::SubCode::kSpaceLimit,
        engine_traits::SubCode::PathNotFound => tirocks::SubCode::kPathNotFound,
        engine_traits::SubCode::MergeOperandsInsufficientCapacity => {
            tirocks::SubCode::KMergeOperandsInsufficientCapacity
        }
        engine_traits::SubCode::ManualCompactionPaused => tirocks::SubCode::kManualCompactionPaused,
        engine_traits::SubCode::Overwritten => tirocks::SubCode::kOverwritten,
        engine_traits::SubCode::TxnNotPrepared => tirocks::SubCode::kTxnNotPrepared,
        engine_traits::SubCode::IoFenced => tirocks::SubCode::kIOFenced,
    };
    let mut ts = tirocks::Status::with_error(code, s.state());
    ts.set_severity(sev);
    ts.set_sub_code(sub_code);
    ts
}
