use crate::{Error, Result};
use tikv_util::time::{Duration, Instant};

pub const SINGLE_GROUP: &[u8] = b"SingleGroup";

use kvproto::coprocessor as coppb;
type HandlerStreamStepResult = Result<(Option<coppb::Response>, bool)>;

/// An interface for all kind of Coprocessor request handlers.
pub trait RequestHandler: Send {
    /// Processes current request and produces a response.
    fn handle_request(&mut self) -> Result<coppb::Response> {
        panic!("unary request is not supported for this handler");
    }

    /// Processes current request and produces streaming responses.
    fn handle_streaming_request(&mut self) -> HandlerStreamStepResult {
        panic!("streaming request is not supported for this handler");
    }

    /// Collects metrics generated in this request handler so far.
    fn collect_metrics_into(&mut self, _metrics: &mut crate::executor::ExecutorMetrics) {
        // Do nothing by default
    }

    fn into_boxed(self) -> Box<dyn RequestHandler>
    where
        Self: 'static + Sized,
    {
        Box::new(self)
    }
}

/// Request process dead line.
///
/// When dead line exceeded, the request handling should be stopped.
// TODO: This struct can be removed.
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    /// Used to construct the Error when deadline exceeded
    tag: &'static str,

    start_time: Instant,
    deadline: Instant,
}

impl Deadline {
    /// Initializes a deadline that counting from current.
    pub fn from_now(tag: &'static str, after_duration: Duration) -> Self {
        let start_time = Instant::now_coarse();
        let deadline = start_time + after_duration;
        Self {
            tag,
            start_time,
            deadline,
        }
    }

    /// Returns error if the deadline is exceeded.
    pub fn check_if_exceeded(&self) -> Result<()> {
        fail_point!("coprocessor_deadline_check_exceeded", |_| Err(
            Error::Outdated(Duration::from_secs(60), self.tag)
        ));

        let now = Instant::now_coarse();
        if self.deadline <= now {
            let elapsed = now.duration_since(self.start_time);
            return Err(Error::Outdated(elapsed, self.tag));
        }
        Ok(())
    }
}
