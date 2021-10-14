// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod test_suite;

pub mod test_dynamic_config;
pub mod test_receiver;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    #[test]
    fn test_resource_metering() {
        let mut ts = test_suite::TestSuite::new();

        // Dynamic config
        test_dynamic_config::case_enable(&mut ts);
        test_dynamic_config::case_report_interval(&mut ts);
        test_dynamic_config::case_max_resource_groups(&mut ts);
        test_dynamic_config::case_precision(&mut ts);

        // Receiver
        test_receiver::case_alter_receiver_addr(&mut ts);
        test_receiver::case_receiver_blocking(&mut ts);
        test_receiver::case_receiver_shutdown(&mut ts);
    }
}
