// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use raft_proto::eraftpb::{self, MessageType};
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use tikv_util::time::monotonic_raw_now;
use tikv_util::Either;
use time::{Duration, Timespec};

/// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
// 1. Target peer already exists but has not established communication with leader yet
// 2. Target peer is added newly due to member change or region split, but it's not
//    created yet
// For both cases the region start key and end key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a pending region split to perform
// later.
#[inline]
pub fn is_initial_msg(msg: &eraftpb::Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        // the peer has not been known to this leader, it may exist or not.
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == raft::INVALID_INDEX)
}

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the Raft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when raft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `send_ts + max_lease` in short.
pub struct Lease {
    // A suspect timestamp is in the Either::Left(_),
    // a valid timestamp is in the Either::Right(_).
    bound: Option<Either<Timespec, Timespec>>,
    max_lease: Duration,

    max_drift: Duration,
    last_update: Timespec,
    remote: Option<RemoteLease>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeaseState {
    /// The lease is suspicious, may be invalid.
    Suspect,
    /// The lease is valid.
    Valid,
    /// The lease is expired.
    Expired,
}

impl Lease {
    pub fn new(max_lease: Duration) -> Lease {
        Lease {
            bound: None,
            max_lease,

            max_drift: max_lease / 3,
            last_update: Timespec::new(0, 0),
            remote: None,
        }
    }

    /// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
    /// And the expired timestamp for that leader lease is `commit_ts + lease`,
    /// which is `send_ts + max_lease` in short.
    fn next_expired_time(&self, send_ts: Timespec) -> Timespec {
        send_ts + self.max_lease
    }

    /// Renew the lease to the bound.
    pub fn renew(&mut self, send_ts: Timespec) {
        let bound = self.next_expired_time(send_ts);
        match self.bound {
            // Longer than suspect ts or longer than valid ts.
            Some(Either::Left(ts)) | Some(Either::Right(ts)) => {
                if ts <= bound {
                    self.bound = Some(Either::Right(bound));
                }
            }
            // Or an empty lease
            None => {
                self.bound = Some(Either::Right(bound));
            }
        }
        // Renew remote if it's valid.
        if let Some(Either::Right(bound)) = self.bound {
            if bound - self.last_update > self.max_drift {
                self.last_update = bound;
                if let Some(ref r) = self.remote {
                    r.renew(bound);
                }
            }
        }
    }

    /// Suspect the lease to the bound.
    pub fn suspect(&mut self, send_ts: Timespec) {
        self.expire_remote_lease();
        let bound = self.next_expired_time(send_ts);
        self.bound = Some(Either::Left(bound));
    }

    /// Inspect the lease state for the ts or now.
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        match self.bound {
            Some(Either::Left(_)) => LeaseState::Suspect,
            Some(Either::Right(bound)) => {
                if ts.unwrap_or_else(monotonic_raw_now) < bound {
                    LeaseState::Valid
                } else {
                    LeaseState::Expired
                }
            }
            None => LeaseState::Expired,
        }
    }

    pub fn expire(&mut self) {
        self.expire_remote_lease();
        self.bound = None;
    }

    pub fn expire_remote_lease(&mut self) {
        // Expire remote lease if there is any.
        if let Some(r) = self.remote.take() {
            r.expire();
        }
    }

    /// Return a new `RemoteLease` if there is none.
    pub fn maybe_new_remote_lease(&mut self, term: u64) -> Option<RemoteLease> {
        if let Some(ref remote) = self.remote {
            if remote.term() == term {
                // At most one connected RemoteLease in the same term.
                return None;
            } else {
                // Term has changed. It is unreachable in the current implementation,
                // because we expire remote lease when leaders step down.
                unreachable!("Must expire the old remote lease first!");
            }
        }
        let expired_time = match self.bound {
            Some(Either::Right(ts)) => timespec_to_u64(ts),
            _ => 0,
        };
        let remote = RemoteLease {
            expired_time: Arc::new(AtomicU64::new(expired_time)),
            term,
        };
        // Clone the remote.
        let remote_clone = remote.clone();
        self.remote = Some(remote);
        Some(remote_clone)
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmter = fmt.debug_struct("Lease");
        match self.bound {
            Some(Either::Left(ts)) => fmter.field("suspect", &ts).finish(),
            Some(Either::Right(ts)) => fmter.field("valid", &ts).finish(),
            None => fmter.finish(),
        }
    }
}

/// A remote lease, it can only be derived by `Lease`. It will be sent
/// to the local read thread, so name it remote. If Lease expires, the remote must
/// expire too.
#[derive(Clone)]
pub struct RemoteLease {
    expired_time: Arc<AtomicU64>,
    term: u64,
}

impl RemoteLease {
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        let expired_time = self.expired_time.load(AtomicOrdering::Acquire);
        if ts.unwrap_or_else(monotonic_raw_now) < u64_to_timespec(expired_time) {
            LeaseState::Valid
        } else {
            LeaseState::Expired
        }
    }

    fn renew(&self, bound: Timespec) {
        self.expired_time
            .store(timespec_to_u64(bound), AtomicOrdering::Release);
    }

    fn expire(&self) {
        self.expired_time.store(0, AtomicOrdering::Release);
    }

    pub fn term(&self) -> u64 {
        self.term
    }
}

impl fmt::Debug for RemoteLease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RemoteLease")
            .field(
                "expired_time",
                &u64_to_timespec(self.expired_time.load(AtomicOrdering::Relaxed)),
            )
            .field("term", &self.term)
            .finish()
    }
}

// Contants used in `timespec_to_u64` and `u64_to_timespec`.
const NSEC_PER_MSEC: i32 = 1_000_000;
const TIMESPEC_NSEC_SHIFT: usize = 32 - NSEC_PER_MSEC.leading_zeros() as usize;

const MSEC_PER_SEC: i64 = 1_000;
const TIMESPEC_SEC_SHIFT: usize = 64 - MSEC_PER_SEC.leading_zeros() as usize;

const TIMESPEC_NSEC_MASK: u64 = (1 << TIMESPEC_SEC_SHIFT) - 1;

/// Convert Timespec to u64. It's millisecond precision and
/// covers a range of about 571232829 years in total.
///
/// # Panics
///
/// If Timespecs have negative sec.
#[inline]
fn timespec_to_u64(ts: Timespec) -> u64 {
    // > Darwin's and Linux's struct timespec functions handle pre-
    // > epoch timestamps using a "two steps back, one step forward" representation,
    // > though the man pages do not actually document this. For example, the time
    // > -1.2 seconds before the epoch is represented by `Timespec { sec: -2_i64,
    // > nsec: 800_000_000 }`.
    //
    // Quote from crate time,
    //   https://github.com/rust-lang-deprecated/time/blob/
    //   e313afbd9aad2ba7035a23754b5d47105988789d/src/lib.rs#L77
    assert!(ts.sec >= 0 && ts.sec < (1i64 << (64 - TIMESPEC_SEC_SHIFT)));
    assert!(ts.nsec >= 0);

    // Round down to millisecond precision.
    let ms = ts.nsec >> TIMESPEC_NSEC_SHIFT;
    let sec = ts.sec << TIMESPEC_SEC_SHIFT;
    sec as u64 | ms as u64
}

/// Convert Timespec to u64.
///
/// # Panics
///
/// If nsec is negative or GE than 1_000_000_000(nano seconds pre second).
#[inline]
pub(crate) fn u64_to_timespec(u: u64) -> Timespec {
    let sec = u >> TIMESPEC_SEC_SHIFT;
    let nsec = (u & TIMESPEC_NSEC_MASK) << TIMESPEC_NSEC_SHIFT;
    Timespec::new(sec as i64, nsec as i32)
}
