// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Common utility implementations for time expression

pub fn period_to_month(period: u64) -> u64 {
    if period == 0 {
        return 0;
    }
    let (year, month) = (period / 100, period % 100);
    if year < 70 {
        (year + 2000) * 12 + month - 1
    } else if year < 100 {
        (year + 1900) * 12 + month - 1
    } else {
        year * 12 + month - 1
    }
}

pub fn month_to_period(month: u64) -> u64 {
    if month == 0 {
        return 0;
    }
    let year = month / 12;
    if year < 70 {
        (year + 2000) * 100 + month % 12 + 1
    } else if year < 100 {
        (year + 1900) * 100 + month % 12 + 1
    } else {
        year * 100 + month % 12 + 1
    }
}
