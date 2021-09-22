// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::RawCpuRecords;
use crate::summary::SummaryRecord;
use crate::{Config, CpuReporter, GrpcClient, SummaryReporter};
use collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer};

/// This trait defines a general framework for how to process the collected data.
///
/// [Reporter] will maintain all sub-reporters, accept external scheduling and task
/// distribution, and call the corresponding methods of all sub-reporters when appropriate.
pub trait SubReporter<T> {
    /// Each batch of collected data will be reported through the this method.
    ///
    /// Implementation needs to cache and aggregate the data reported each time.
    fn report(&mut self, cfg: &Config, v: T);

    /// Whenever the external controller decides to upload data, it will call
    /// this method.
    ///
    /// Implementation needs to upload the data that has been cached in the
    /// past to the remote in batches.
    fn upload(&mut self, cfg: &Config);

    /// When the external controller decides to stop scheduling, it will call
    /// this method to shutdown.
    ///
    /// Implementation needs to clean up all inner caches if necessary.
    fn reset(&mut self);
}

/// A structure that combines all the [SubReporter]s.
///
/// `Reporter` implements [Runnable] and [RunnableWithTimer] to handle [Task]s
/// from the [Scheduler], but it does no business logic and instead controls
/// all sub-reporters to perform logic in different business areas.
///
/// [Runnable]: tikv_util::worker::Runnable
/// [RunnableWithTimer]: tikv_util::worker::RunnableWithTimer
/// [Scheduler]: tikv_util::worker::Scheduler
pub struct Reporter<C, S> {
    config: Config,
    cpu_reporter: C,
    summary_reporter: S,
}

impl<C, S> Runnable for Reporter<C, S>
where
    C: SubReporter<Arc<RawCpuRecords>> + Send,
    S: SubReporter<Arc<HashMap<Vec<u8>, SummaryRecord>>> + Send,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::ConfigChange(cfg) => self.config_change(cfg),
            Task::CpuRecords(v) => self.cpu_reporter.report(&self.config, v),
            Task::SummaryRecords(v) => self.summary_reporter.report(&self.config, v),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

impl<C, S> RunnableWithTimer for Reporter<C, S>
where
    C: SubReporter<Arc<RawCpuRecords>> + Send,
    S: SubReporter<Arc<HashMap<Vec<u8>, SummaryRecord>>> + Send,
{
    fn on_timeout(&mut self) {
        self.cpu_reporter.upload(&self.config);
        self.summary_reporter.upload(&self.config);
    }

    fn get_interval(&self) -> Duration {
        self.config.report_agent_interval.0
    }
}

impl<C, S> Reporter<C, S>
where
    C: SubReporter<Arc<RawCpuRecords>>,
    S: SubReporter<Arc<HashMap<Vec<u8>, SummaryRecord>>>,
{
    pub fn new(config: Config, cpu_reporter: C, summary_reporter: S) -> Self {
        Self {
            config,
            cpu_reporter,
            summary_reporter,
        }
    }

    fn config_change(&mut self, cfg: Config) {
        self.config = cfg;
        if !self.config.should_report() {
            self.reset();
            return;
        }
    }

    fn reset(&mut self) {
        self.cpu_reporter.reset();
        self.summary_reporter.reset();
    }
}

/// `Task` represents a task scheduled in [Reporter].
pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<RawCpuRecords>),
    SummaryRecords(Arc<HashMap<Vec<u8>, SummaryRecord>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
            Task::CpuRecords(_) => {
                write!(f, "CpuRecords")?;
            }
            Task::SummaryRecords(_) => {
                write!(f, "SummaryRecords")?;
            }
        }
        Ok(())
    }
}

// Helper functions.
impl Config {
    fn should_report(&self) -> bool {
        self.enabled && !self.agent_address.is_empty() && self.max_resource_groups != 0
    }
}

/// Constructs and returns a default [Reporter].
///
/// This function is intended to simplify external use.
pub fn build_default_reporter(
    cfg: Config,
) -> Reporter<CpuReporter<GrpcClient>, SummaryReporter<GrpcClient>> {
    let client = GrpcClient::default();
    Reporter::new(
        cfg,
        CpuReporter::new(client.clone()),
        SummaryReporter::new(client),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use tikv_util::config::ReadableDuration;
    use tikv_util::worker::{Runnable, RunnableWithTimer};

    static SUB_REPORTER_OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockCpuReporter;

    impl SubReporter<Arc<RawCpuRecords>> for MockCpuReporter {
        fn report(&mut self, cfg: &Config, v: Arc<RawCpuRecords>) {
            assert_eq!(cfg.agent_address, "abc");
            assert_eq!(v.begin_unix_time_secs, 123);
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn upload(&mut self, cfg: &Config) {
            assert_eq!(cfg.agent_address, "abc");
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn reset(&mut self) {
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    struct MockSummaryReporter;

    impl SubReporter<Arc<HashMap<Vec<u8>, SummaryRecord>>> for MockSummaryReporter {
        fn report(&mut self, cfg: &Config, v: Arc<HashMap<Vec<u8>, SummaryRecord>>) {
            assert_eq!(cfg.agent_address, "abc");
            assert_eq!(v.len(), 1);
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn upload(&mut self, cfg: &Config) {
            assert_eq!(cfg.agent_address, "abc");
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }

        fn reset(&mut self) {
            SUB_REPORTER_OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_reporter() {
        let mut r = Reporter::new(Config::default(), MockCpuReporter, MockSummaryReporter);
        r.run(Task::ConfigChange(Config {
            enabled: false,
            agent_address: "abc".to_string(),
            report_agent_interval: ReadableDuration::minutes(2),
            max_resource_groups: 3000,
            precision: ReadableDuration::secs(2),
        }));
        assert_eq!(r.get_interval(), Duration::from_secs(120));
        r.run(Task::CpuRecords(Arc::new(RawCpuRecords {
            begin_unix_time_secs: 123,
            duration: Duration::default(),
            records: HashMap::default(),
        })));
        let mut records = HashMap::default();
        records.insert(b"".to_vec(), SummaryRecord::default());
        r.run(Task::SummaryRecords(Arc::new(records)));
        r.on_timeout();
        r.shutdown();
        assert_eq!(SUB_REPORTER_OP_COUNT.load(SeqCst), 8);
    }
}
