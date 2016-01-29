use std::error;
use std::boxed::Box;
use std::result;

pub type Error = Box<error::Error + Send + Sync>;

pub type Result<T> = result::Result<T, Error>;

pub fn other<T>(err: T) -> Error
    where T: Into<Box<error::Error + Sync + Send + 'static>>
{
    err.into()
}
