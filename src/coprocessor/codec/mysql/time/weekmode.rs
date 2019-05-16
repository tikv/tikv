// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

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
