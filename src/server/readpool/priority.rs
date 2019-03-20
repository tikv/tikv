// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::kvrpcpb;
use std::fmt;

/// A `Priority` decides which thread pool a task is scheduled to.
#[derive(Debug, Copy, Clone)]
pub enum Priority {
    Normal,
    Low,
    High,
}

impl From<kvrpcpb::CommandPri> for Priority {
    fn from(p: kvrpcpb::CommandPri) -> Priority {
        match p {
            kvrpcpb::CommandPri::High => Priority::High,
            kvrpcpb::CommandPri::Normal => Priority::Normal,
            kvrpcpb::CommandPri::Low => Priority::Low,
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Priority::High => write!(f, "high"),
            Priority::Normal => write!(f, "normal"),
            Priority::Low => write!(f, "low"),
        }
    }
}
