#[derive(Clone, Copy, Debug)]
pub enum TimeMeasureType {
    SystemTime,
    Instant,
}

#[derive(Clone, Copy, Debug)]
pub enum TimeMeasureRoot {
    SystemTime(std::time::SystemTime),
    Instant(std::time::Instant),
}

pub struct TimeMeasureHandle {
    pub root: TimeMeasureRoot,
    pub start: std::time::Duration,
}

pub struct TimeMeasure;
impl TimeMeasure {
    pub fn root(tp: TimeMeasureType) -> TimeMeasureRoot {
        match tp {
            TimeMeasureType::SystemTime => {
                TimeMeasureRoot::SystemTime(std::time::SystemTime::now())
            }
            TimeMeasureType::Instant => TimeMeasureRoot::Instant(std::time::Instant::now()),
        }
    }
}

impl TimeMeasureRoot {
    pub fn start(&self) -> TimeMeasureHandle {
        match self {
            TimeMeasureRoot::SystemTime(s) => TimeMeasureHandle {
                root: *self,
                start: s.elapsed().unwrap(),
            },
            TimeMeasureRoot::Instant(s) => TimeMeasureHandle {
                root: *self,
                start: s.elapsed(),
            },
        }
    }
}

impl TimeMeasureHandle {
    pub fn end(self) -> (std::time::Duration, std::time::Duration) {
        let TimeMeasureHandle { root, start } = self;
        match root {
            TimeMeasureRoot::SystemTime(s) => (start, s.elapsed().unwrap()),
            TimeMeasureRoot::Instant(s) => (start, s.elapsed()),
        }
    }
}
