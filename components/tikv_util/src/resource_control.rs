// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

/// This mod provide some utility types and functions for resource control.
use std::borrow::Cow;

use kvproto::kvrpcpb::ResourceControlContext;
use strum::{EnumCount, EnumIter};

/// default resource group name
pub const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";

const OVERRIDE_PRIORITY_MASK: u8 = 0b1000_0000;
const RESOURCE_GROUP_NAME_MASK: u8 = 0b0100_0000;

#[derive(Clone, Default)]
pub struct TaskMetadata<'a> {
    // The first byte is a bit map to indicate which field exists,
    // then append override priority if nonzero,
    // then append resource group name if not default
    metadata: Cow<'a, [u8]>,
}

impl<'a> TaskMetadata<'a> {
    pub fn deep_clone(&self) -> TaskMetadata<'static> {
        TaskMetadata {
            metadata: Cow::Owned(self.metadata.to_vec()),
        }
    }

    pub fn from_ctx(ctx: &ResourceControlContext) -> Self {
        let mut mask = 0;
        let mut buf = vec![];
        if ctx.override_priority != 0 {
            mask |= OVERRIDE_PRIORITY_MASK;
        }
        if !ctx.resource_group_name.is_empty()
            && ctx.resource_group_name != DEFAULT_RESOURCE_GROUP_NAME
        {
            mask |= RESOURCE_GROUP_NAME_MASK;
        }
        if mask == 0 {
            // if all are default value, no need to write anything to save copy cost
            return Self {
                metadata: Cow::Owned(buf),
            };
        }
        buf.push(mask);
        if mask & OVERRIDE_PRIORITY_MASK != 0 {
            buf.extend_from_slice(&(ctx.override_priority as u32).to_ne_bytes());
        }
        if mask & RESOURCE_GROUP_NAME_MASK != 0 {
            buf.extend_from_slice(ctx.resource_group_name.as_bytes());
        }
        Self {
            metadata: Cow::Owned(buf),
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.metadata.into_owned()
    }

    pub fn override_priority(&self) -> u32 {
        if self.metadata.is_empty() {
            return 0;
        }
        if self.metadata[0] & OVERRIDE_PRIORITY_MASK == 0 {
            return 0;
        }
        u32::from_ne_bytes(self.metadata[1..5].try_into().unwrap())
    }

    pub fn group_name(&self) -> &[u8] {
        if self.metadata.is_empty() {
            return DEFAULT_RESOURCE_GROUP_NAME.as_bytes();
        }
        if self.metadata[0] & RESOURCE_GROUP_NAME_MASK == 0 {
            return DEFAULT_RESOURCE_GROUP_NAME.as_bytes();
        }
        let start = if self.metadata[0] & OVERRIDE_PRIORITY_MASK != 0 {
            5
        } else {
            1
        };
        &self.metadata[start..]
    }
}

impl<'a> From<&'a [u8]> for TaskMetadata<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self {
            metadata: Cow::Borrowed(bytes),
        }
    }
}

// return the TaskPriority value from task metadata.
pub fn priority_from_task_meta(meta: &[u8]) -> TaskPriority {
    let priority = TaskMetadata::from(meta).override_priority();
    // mapping (high(15), medium(8), low(1)) -> (0, 1, 2)
    debug_assert!(priority <= 16);
    TaskPriority::from(priority)
}

#[derive(Copy, Clone, Eq, PartialEq, EnumCount, EnumIter, Debug)]
#[repr(usize)]
pub enum TaskPriority {
    High = 0,
    Medium = 1,
    Low = 2,
}

impl TaskPriority {
    // reexport enum count, caller can use it without importing `EnumCount`.
    pub const PRIORITY_COUNT: usize = Self::COUNT;
    pub fn as_str(&self) -> &'static str {
        match *self {
            TaskPriority::High => "high",
            TaskPriority::Medium => "medium",
            TaskPriority::Low => "low",
        }
    }

    pub fn priorities() -> [Self; Self::COUNT] {
        use TaskPriority::*;
        [High, Medium, Low]
    }
}

impl From<u32> for TaskPriority {
    fn from(value: u32) -> Self {
        // map the resource group priority value (1,8,16) to (Low,Medium,High)
        // 0 means the priority is not set, so map it to medium by default.
        if value == 0 {
            Self::Medium
        } else if value < 6 {
            Self::Low
        } else if value < 11 {
            Self::Medium
        } else {
            Self::High
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_metadata() {
        let cases = [
            ("default", 0u32),
            ("default", 6u32),
            ("test", 0u32),
            ("test", 15u32),
        ];

        let metadata = TaskMetadata::from_ctx(&ResourceControlContext::default());
        assert_eq!(metadata.group_name(), b"default");
        for (group_name, priority) in cases {
            let metadata = TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: group_name.to_string(),
                override_priority: priority as u64,
                ..Default::default()
            });
            assert_eq!(metadata.override_priority(), priority);
            assert_eq!(metadata.group_name(), group_name.as_bytes());
            let vec = metadata.to_vec();
            let metadata1 = TaskMetadata::from(vec.as_slice());
            assert_eq!(metadata1.override_priority(), priority);
            assert_eq!(metadata1.group_name(), group_name.as_bytes());
        }
    }

    #[test]
    fn test_task_priority() {
        use TaskPriority::*;
        let cases = [
            (0, Medium),
            (1, Low),
            (7, Medium),
            (8, Medium),
            (15, High),
            (16, High),
        ];
        for (value, priority) in cases {
            assert_eq!(TaskPriority::from(value), priority);
        }
    }
}
