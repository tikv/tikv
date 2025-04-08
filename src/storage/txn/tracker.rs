// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use tikv_util::time::Instant;
use tracker::{FutureTrack, TrackerToken, GLOBAL_TRACKERS};

use crate::storage::metrics::CommandKind;

thread_local! {
    // The TLS state exists solely for the purpose of [`TlsFutureTracker::collect_to_tracker`]
    // Remove this complication if we can find a better way to collect the time.
    static CURRENT_STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

/// PollState tracks the state of the future polling.
#[derive(Debug, Clone, Copy)]
enum PollState {
    // The state is set to Collected when [`TlsFutureTracker::collect_to_tracker`]
    // is called, meaning the process time and suspend time are collected.
    Collected,
    // The state is set to Begin when `Future::poll` is called.
    // Record the time when the future is polled, and the time is used to
    // calculate the process time of the future.
    Began(Instant),
    // The state is set to Finish when `Future::poll` is finished.
    // Record the time when the future is finished, and the time is used to
    // calculate the suspend time before the next poll.
    Finished(Instant),
}

#[derive(Debug)]
struct State {
    token: TrackerToken,
    tag: CommandKind,
    cid: u64,
    current_stage: PollState,

    future_process_nanos: u64,
    future_suspend_nanos: u64,
}

pub struct TlsFutureTracker {
    state: Option<State>,
}

impl TlsFutureTracker {
    pub fn new(token: TrackerToken, tag: CommandKind, cid: u64) -> Self {
        let state = State {
            token,
            tag,
            cid,
            // Set to Finish state to tracking the wait time before the first poll.
            current_stage: PollState::Finished(Instant::now()),

            future_process_nanos: 0,
            future_suspend_nanos: 0,
        };
        TlsFutureTracker { state: Some(state) }
    }

    /// Collects the current future process and suspend time to the tracker.
    ///
    /// It exists because a storage callback is invoked during the future
    /// polling. To make measurements more accurate, we need to collect the
    /// time before the callback is invoked.
    pub fn collect_to_tracker(now: Instant, tracker: &mut tracker::Tracker) {
        CURRENT_STATE.with(|current_state| {
            let mut state = current_state.borrow_mut();
            let state = state.as_mut().expect("TLS future tracker state is not set");
            state.collect_to_tracker(now, tracker);
            // Prevent double counting.
            state.current_stage = PollState::Collected;
        });
    }
}

impl State {
    fn collect_to_tracker(&mut self, now: Instant, tracker: &mut tracker::Tracker) {
        if let PollState::Began(at) = self.current_stage {
            self.future_process_nanos += now.saturating_duration_since(at).as_nanos() as u64;
        }
        tracker.metrics.future_process_nanos += self.future_process_nanos;
        tracker.metrics.future_suspend_nanos += self.future_suspend_nanos;
        self.future_process_nanos = 0;
        self.future_suspend_nanos = 0;
    }

    fn on_poll_begin(&mut self, now: Instant) {
        match self.current_stage {
            PollState::Collected => {}
            PollState::Finished(at) => {
                self.future_suspend_nanos += now.saturating_duration_since(at).as_nanos() as u64;
            }
            _ => {
                panic!(
                    "unexpected tracker state {:?}, tag: {:?}, cid: {:?}, token: {:?}",
                    self.current_stage, self.tag, self.cid, self.token,
                )
            }
        }
    }

    fn on_poll_finish(&mut self, now: Instant, tracker: &mut tracker::Tracker) {
        match self.current_stage {
            PollState::Collected => {}
            PollState::Began(_) => {
                self.collect_to_tracker(now, tracker);
            }
            _ => {
                panic!(
                    "unexpected tracker state {:?}, tag: {:?}, cid: {:?}, token: {:?}",
                    self.current_stage, self.tag, self.cid, self.token,
                );
            }
        }
    }
}

impl FutureTrack for TlsFutureTracker {
    fn on_poll_begin(&mut self) {
        let mut state = self.state.take().expect("future tracker state is not set");
        let now = Instant::now();
        state.on_poll_begin(now);
        CURRENT_STATE.with(|current_state| {
            assert!(
                current_state.borrow_mut().is_none(),
                "TLS future tracker state is already set, {:?} vs {:?}",
                current_state.borrow().as_ref().unwrap(),
                state
            );
            state.current_stage = PollState::Began(now);
            *current_state.borrow_mut() = Some(state);
        });
    }

    fn on_poll_finish(&mut self) {
        CURRENT_STATE.with(|current_state| {
            let mut state = current_state
                .borrow_mut()
                .take()
                .expect("TLS future tracker state is not set");
            let now = Instant::now();
            GLOBAL_TRACKERS.with_tracker(state.token, |tracker| {
                state.on_poll_finish(now, tracker);
            });
            // Set to Finish state to tracking the wait time before the next poll.
            state.current_stage = PollState::Finished(now);
            self.state = Some(state);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    };

    use futures::{channel::oneshot::channel, executor::LocalPool, task::LocalSpawnExt};
    use kvproto::kvrpcpb as pb;
    use tracker::*;

    use super::*;

    struct TestTracker<T> {
        inner: T,
        poll_count: Arc<AtomicUsize>,
    }

    impl<T: FutureTrack> FutureTrack for TestTracker<T> {
        fn on_poll_begin(&mut self) {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            self.inner.on_poll_begin();
        }

        fn on_poll_finish(&mut self) {
            self.inner.on_poll_finish();
        }
    }

    #[test]
    fn test_tracker() {
        let (tx, rx) = channel::<()>();

        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &pb::Context::default(),
            RequestType::Unknown,
            0,
        )));
        let poll_count = Arc::new(AtomicUsize::new(0));
        let poll_count_ = poll_count.clone();
        let tracker = TlsFutureTracker::new(token, CommandKind::prewrite, 0);
        let fut = track(
            async move {
                let _ = rx.await;
                sleep(Duration::from_millis(100));

                GLOBAL_TRACKERS.with_tracker(token, |t| {
                    TlsFutureTracker::collect_to_tracker(Instant::now(), t);
                });

                sleep(Duration::from_millis(100));
            },
            TestTracker {
                inner: tracker,
                poll_count,
            },
        );
        let mut local = LocalPool::new();
        let spawner = local.spawner();
        spawner.spawn_local(fut).unwrap();
        assert!(!local.try_run_one());
        assert_eq!(poll_count_.load(Ordering::SeqCst), 1);

        tx.send(()).unwrap();
        sleep(Duration::from_millis(300));
        assert!(local.try_run_one());
        assert_eq!(poll_count_.load(Ordering::SeqCst), 2);

        let mut wait_details = pb::TimeDetailV2::default();
        GLOBAL_TRACKERS.with_tracker(token, |tracker| {
            tracker.merge_time_detail(&mut wait_details);
        });

        let process_suspend_wall_time_ns = wait_details.get_process_suspend_wall_time_ns();
        assert!(
            process_suspend_wall_time_ns >= Duration::from_millis(300).as_nanos() as u64,
            "{}",
            process_suspend_wall_time_ns
        );

        let process_wall_time_ns = wait_details.get_process_wall_time_ns();
        assert!(
            process_wall_time_ns >= Duration::from_millis(100).as_nanos() as u64
                && process_wall_time_ns < Duration::from_millis(200).as_nanos() as u64,
            "{} {}",
            process_wall_time_ns,
            Duration::from_millis(100).as_nanos()
        );
    }

    #[test]
    fn test_no_tracker() {
        let (tx, rx) = channel::<()>();

        let token = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            &pb::Context::default(),
            RequestType::Unknown,
            0,
        )));
        // Remove the tracker early.
        GLOBAL_TRACKERS.remove(token);

        let tracker = TlsFutureTracker::new(token, CommandKind::prewrite, 0);
        let fut = track(
            async move {
                let _ = rx.await;
                sleep(Duration::from_millis(100));

                // The tracker is removed, so the time should not be collected.
                GLOBAL_TRACKERS.with_tracker(token, |t| {
                    TlsFutureTracker::collect_to_tracker(Instant::now(), t);
                });

                let mut tracker = Tracker::new(RequestInfo::new(
                    &pb::Context::default(),
                    RequestType::Unknown,
                    0,
                ));
                TlsFutureTracker::collect_to_tracker(Instant::now(), &mut tracker);
                let mut wait_details = pb::TimeDetailV2::default();
                tracker.merge_time_detail(&mut wait_details);
                assert!(
                    wait_details.get_process_suspend_wall_time_ns() > 0
                        && wait_details.get_process_wall_time_ns() > 0,
                    "{:?}",
                    wait_details
                );
            },
            tracker,
        );
        let mut local = LocalPool::new();
        let spawner = local.spawner();
        spawner.spawn_local(fut).unwrap();
        assert!(!local.try_run_one());

        tx.send(()).unwrap();
        sleep(Duration::from_millis(300));
        assert!(local.try_run_one());

        CURRENT_STATE.with(|current_state| {
            let state = current_state.borrow();
            assert!(state.is_none(), "{:?}", state);
        });
    }
}
