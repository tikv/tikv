// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
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
