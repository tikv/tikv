#[derive(Debug)]
pub enum ProfError {
    MemProfilingNotEnabled,
    IOError(std::io::Error),
    JemallocNotEnabled,
    JemallocError(i32),
}

pub type ProfResult<T> = std::result::Result<T, ProfError>;

impl From<std::io::Error> for ProfError {
    fn from(e: std::io::Error) -> Self {
        ProfError::IOError(e)
    }
}
