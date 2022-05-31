// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use chrono::Weekday;

use super::{weekmode::WeekMode, Time};

pub trait WeekdayExtension {
    fn name(&self) -> &'static str;
    fn name_abbr(&self) -> &'static str;
}

impl WeekdayExtension for Weekday {
    fn name(&self) -> &'static str {
        match *self {
            Weekday::Mon => "Monday",
            Weekday::Tue => "Tuesday",
            Weekday::Wed => "Wednesday",
            Weekday::Thu => "Thursday",
            Weekday::Fri => "Friday",
            Weekday::Sat => "Saturday",
            Weekday::Sun => "Sunday",
        }
    }

    fn name_abbr(&self) -> &'static str {
        match *self {
            Weekday::Mon => "Mon",
            Weekday::Tue => "Tue",
            Weekday::Wed => "Wed",
            Weekday::Thu => "Thu",
            Weekday::Fri => "Fri",
            Weekday::Sat => "Sat",
            Weekday::Sun => "Sun",
        }
    }
}

pub trait DateTimeExtension {
    fn days(&self) -> i32;
    fn calc_year_week(
        &self,
        monday_first: bool,
        week_year: bool,
        first_weekday: bool,
    ) -> (i32, i32);
    fn calc_year_week_by_week_mode(&self, week_mode: WeekMode) -> (i32, i32);
    fn week(&self, mode: WeekMode) -> i32;
    fn year_week(&self, mode: WeekMode) -> (i32, i32);
    fn abbr_day_of_month(&self) -> &'static str;
    fn day_number(&self) -> i32;
    fn second_number(&self) -> i64;
}

impl DateTimeExtension for Time {
    /// returns the day of year starting from 1.
    /// implements TiDB YearDay().
    fn days(&self) -> i32 {
        self.ordinal()
    }

    /// returns the week of year and year. should not be called directly.
    /// when monday_first == true, Monday is considered as the first day in the week,
    ///         otherwise Sunday.
    /// when week_year == true, week is from 1 to 53, otherwise from 0 to 53.
    /// when first_weekday == true, the week that contains the first 'first-day-of-week' is week 1,
    ///         otherwise weeks are numbered according to ISO 8601:1988.
    fn calc_year_week(
        &self,
        monday_first: bool,
        mut week_year: bool,
        first_weekday: bool,
    ) -> (i32, i32) {
        let mut year = self.year() as i32;
        let daynr = calc_day_number(year, self.month() as i32, self.day() as i32);
        let mut first_daynr = calc_day_number(year, 1, 1);
        let mut weekday = calc_weekday(first_daynr, !monday_first);
        let mut days: i32;

        if self.month() == 1 && (self.day() as i32) <= 7 - weekday {
            if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
                return (year, 0);
            }
            week_year = true;
            year -= 1;
            days = calc_days_in_year(year);
            first_daynr -= days;
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
            days = daynr - (first_daynr + 7 - weekday);
        } else {
            days = daynr - (first_daynr - weekday);
        }

        if week_year && days >= 52 * 7 {
            weekday = (weekday + calc_days_in_year(year as i32)) % 7;
            if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
                year += 1;
                return (year, 1);
            }
        }
        let week: i32 = days / 7 + 1;
        (year, week)
    }

    /// returns the week of year according to week mode. should not be called directly.
    /// implements TiDB calcWeek()
    fn calc_year_week_by_week_mode(&self, week_mode: WeekMode) -> (i32, i32) {
        let mode = week_mode.to_normalized();
        let monday_first = mode.contains(WeekMode::BEHAVIOR_MONDAY_FIRST);
        let week_year = mode.contains(WeekMode::BEHAVIOR_YEAR);
        let first_weekday = mode.contains(WeekMode::BEHAVIOR_FIRST_WEEKDAY);
        self.calc_year_week(monday_first, week_year, first_weekday)
    }

    /// returns the week of year.
    /// implements TiDB Week().
    fn week(&self, mode: WeekMode) -> i32 {
        if self.month() == 0 || self.day() == 0 {
            return 0;
        }
        let (_, week) = self.calc_year_week_by_week_mode(mode);
        week
    }

    /// returns the week of year and year.
    /// implements TiDB YearWeek().
    fn year_week(&self, mode: WeekMode) -> (i32, i32) {
        self.calc_year_week_by_week_mode(mode | WeekMode::BEHAVIOR_YEAR)
    }

    /// returns the abbreviation of the day of month.
    fn abbr_day_of_month(&self) -> &'static str {
        match self.day() {
            1 | 21 | 31 => "st",
            2 | 22 => "nd",
            3 | 23 => "rd",
            _ => "th",
        }
    }

    /// returns the days since 0000-00-00
    fn day_number(&self) -> i32 {
        calc_day_number(self.year() as i32, self.month() as i32, self.day() as i32)
    }

    /// returns the seconds since 0000-00-00 00:00:00
    fn second_number(&self) -> i64 {
        let days = self.day_number();
        days as i64 * 86400
            + self.hour() as i64 * 3600
            + self.minute() as i64 * 60
            + self.second() as i64
    }
}

// calculates days since 0000-00-00.
fn calc_day_number(mut year: i32, month: i32, day: i32) -> i32 {
    if year == 0 && month == 0 {
        return 0;
    }
    let mut delsum = 365 * year + 31 * (month - 1) + day;
    if month <= 2 {
        year -= 1;
    } else {
        delsum -= (month * 4 + 23) / 10;
    }
    let temp = ((year / 100 + 1) * 3) / 4;
    delsum + year / 4 - temp
}

/// calculates days in one year, it works with 0 <= year <= 99.
fn calc_days_in_year(year: i32) -> i32 {
    if (year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && (year != 0))) {
        return 366;
    }
    365
}

/// calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
fn calc_weekday(mut daynr: i32, sunday_first_day: bool) -> i32 {
    daynr += 5;
    if sunday_first_day {
        daynr += 1;
    }
    daynr % 7
}
