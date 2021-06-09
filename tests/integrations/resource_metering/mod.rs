// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod test_dynamic_config;
pub mod test_suite;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    #[test]
    fn test() {
        let mut ts = test_suite::TestSuite::new();
        test_dynamic_config::case(&mut ts);
    }
}
