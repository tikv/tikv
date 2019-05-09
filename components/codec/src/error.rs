// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        BufferTooSmall {
            description("The buffer is too small to read or write data")
        }
        UnexpectedEOF {
            description("Expecting more data but got EOF")
        }
        BadPadding {
            description("Data padding is wrong")
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
