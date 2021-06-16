// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod test_suite;

pub mod test_agent;
pub mod test_dynamic_config;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    #[test]
    fn test() {
        let mut ts = test_suite::TestSuite::new();

        // Dynamic config
        test_dynamic_config::case_enable(&mut ts);
        test_dynamic_config::case_report_interval(&mut ts);
        test_dynamic_config::case_max_resource_groups(&mut ts);
        test_dynamic_config::case_precision(&mut ts);

        // Agent
        test_agent::case_alter_agent_addr(&mut ts);
    }
}
