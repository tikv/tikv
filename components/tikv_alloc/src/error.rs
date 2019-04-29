#[derive(Debug)]
pub enum ProfError {
    MemProfilingNotEnabled,
    JemallocNotEnabled,
    JemallocError(i32),
}

pub type ProfResult<T> = std::result::Result<T, ProfError>;
