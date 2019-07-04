use hyper::Error as HttpError;
use std::net::AddrParseError;
use std::result;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        AddrParse(err: AddrParseError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Http(err: HttpError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
