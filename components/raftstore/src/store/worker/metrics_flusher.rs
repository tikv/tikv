// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt};

use crate::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use crate::store::{metrics::*, GroupState, LeaderState};
use raft::StateRole;
use tikv_util::{
    time::Duration,
    warn,
    worker::{Runnable, RunnableWithTimer},
};

const FLUSH_REGION_METRICS_INTERVAL: Duration = Duration::from_secs(30);

pub struct Runner {
    region_info_accessor: RegionInfoAccessor,
}

impl Runner {
    pub fn new(region_info_accessor: RegionInfoAccessor) -> Self {
        Runner {
            region_info_accessor,
        }
    }
}

#[derive(Debug)]
pub struct Task {}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlushRegionMetricsTask")
    }
}

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, _task: Self::Task) {
        // do nothing
    }

    fn shutdown(&mut self) {
        // do nothing
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum RoleType {
    Leader,
    NonLeader,
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum LeaderStateType {
    Awaken,
    Poll,
    Hibernated,
}

macro_rules! set_hibernate_state {
    ($metrics: expr, $states: expr) => {
        $metrics
            .ordered
            .set(*$states.entry(GroupState::Ordered).or_default() as _);
        $metrics
            .chaos
            .set(*$states.entry(GroupState::Chaos).or_default() as _);
        $metrics
            .pre_chaos
            .set(*$states.entry(GroupState::PreChaos).or_default() as _);
        $metrics
            .idle
            .set(*$states.entry(GroupState::Idle).or_default() as _);
    };
}

macro_rules! set_hibernate_leader_state {
    ($metrics: expr, $states: expr) => {
        $metrics
            .awaken
            .set(*$states.entry(LeaderStateType::Awaken).or_default() as _);
        $metrics
            .poll
            .set(*$states.entry(LeaderStateType::Poll).or_default() as _);
        $metrics
            .hibernated
            .set(*$states.entry(LeaderStateType::Hibernated).or_default() as _);
    };
}

impl RunnableWithTimer for Runner {
    fn on_timeout(&mut self) {
        // Iterate through all regions.
        if let Err(e) = self.region_info_accessor.seek_region(
            None, /*from*/
            Box::new(|iter| {
                let mut hibernate_states: HashMap<RoleType, HashMap<GroupState, usize>> =
                    HashMap::new();
                let mut hibernate_leader_states: HashMap<LeaderStateType, usize> = HashMap::new();
                for info in iter {
                    let role = match info.role {
                        StateRole::Leader => RoleType::Leader,
                        _ => RoleType::NonLeader,
                    };
                    *(hibernate_states
                        .entry(role)
                        .or_default()
                        .entry(info.hibernate_state.group)
                        .or_default()) += 1;
                    if info.role == StateRole::Leader {
                        let leader_state = match info.hibernate_state.leader {
                            LeaderState::Awaken => LeaderStateType::Awaken,
                            LeaderState::Poll(_) => LeaderStateType::Poll,
                            LeaderState::Hibernated => LeaderStateType::Hibernated,
                        };
                        *(hibernate_leader_states.entry(leader_state).or_default()) += 1;
                    }
                }
                set_hibernate_state!(
                    HIBERNATE_STATE.leader,
                    hibernate_states.entry(RoleType::Leader).or_default()
                );
                set_hibernate_state!(
                    HIBERNATE_STATE.non_leader,
                    hibernate_states.entry(RoleType::NonLeader).or_default()
                );
                set_hibernate_leader_state!(HIBERNATE_LEADER_STATE, hibernate_leader_states);
            }),
        ) {
            warn!("failed to flush region metrics"; "err" => ?e);
        }
    }

    fn get_interval(&self) -> Duration {
        FLUSH_REGION_METRICS_INTERVAL
    }
}
