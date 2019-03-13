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

bitflags! {
    pub struct WeekMode: u32 {
        const BEHAVIOR_MONDAY_FIRST  = 0b00000001;
        const BEHAVIOR_YEAR          = 0b00000010;
        const BEHAVIOR_FIRST_WEEKDAY = 0b00000100;
    }
}

impl WeekMode {
    pub fn to_normalized(self) -> WeekMode {
        let mut mode = self;
        if !mode.contains(WeekMode::BEHAVIOR_MONDAY_FIRST) {
            mode ^= WeekMode::BEHAVIOR_FIRST_WEEKDAY;
        }
        mode
    }
}
