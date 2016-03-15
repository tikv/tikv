#[macro_export]
macro_rules! format_err (
    ($msg:expr) => (
        {
            error!("{}", $msg);
            return Err(ServerError::FormatError($msg.to_owned()));
        }
    )
);
