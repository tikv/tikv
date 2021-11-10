// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use online_config::ConfigChange;
use std::fmt::{self, Display, Formatter};

use raftstore::coprocessor::Config;

pub enum Task {
    SplitCheckTask {
        region: Region,
        auto_split: bool,
        policy: CheckPolicy,
    },
    ChangeConfig(ConfigChange),
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&Config) + Send>),
}

impl Task {
    pub fn split_check(region: Region, auto_split: bool, policy: CheckPolicy) -> Task {
        Task::SplitCheckTask {
            region,
            auto_split,
            policy,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::SplitCheckTask {
                region, auto_split, ..
            } => write!(
                f,
                "[split check worker] Split Check Task for {}, auto_split: {:?}",
                region.get_id(),
                auto_split
            ),
            Task::ChangeConfig(_) => write!(f, "[split check worker] Change Config Task"),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "[split check worker] Validate config"),
        }
    }
}
