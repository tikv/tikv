// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Result;

pub trait SocketOpt {
    fn set_send_buffer_size(&self, _size: usize) -> Result<()>;
    fn send_buffer_size(&self) -> Result<usize>;
    fn set_recv_buffer_size(&self, _size: usize) -> Result<()>;
    fn recv_buffer_size(&self) -> Result<usize>;
}

#[cfg(unix)]
mod unix {
    use super::SocketOpt;

    use std::io::{Result, Error};
    use std::os::unix::io::AsRawFd;

    use mio::tcp::TcpStream;
    use nix::Error as NixError;
    use nix::sys::socket;
    use nix::sys::socket::sockopt;

    impl SocketOpt for TcpStream {
        fn set_send_buffer_size(&self, size: usize) -> Result<()> {
            socket::setsockopt(self.as_raw_fd(), sockopt::SndBuf, &size).map_err(from_nix_error)
        }
        fn send_buffer_size(&self) -> Result<usize> {
            socket::getsockopt(self.as_raw_fd(), sockopt::SndBuf).map_err(from_nix_error)
        }

        fn set_recv_buffer_size(&self, size: usize) -> Result<()> {
            socket::setsockopt(self.as_raw_fd(), sockopt::RcvBuf, &size).map_err(from_nix_error)
        }

        fn recv_buffer_size(&self) -> Result<usize> {
            socket::getsockopt(self.as_raw_fd(), sockopt::RcvBuf).map_err(from_nix_error)
        }
    }

    fn from_nix_error(err: NixError) -> Error {
        Error::from_raw_os_error(err.errno() as i32)
    }
}


#[cfg(windows)]
mod windows {
    use mio::tcp::TcpStream;
    use std::io::Result;

    impl SocketOpt for TcpStream {
        fn set_send_buffer_size(&self, _size: usize) -> Result<()> {
            error!("set_send_buffer_size is not supported in windows now");
            Ok(())
        }

        fn send_buffer_size(&self) -> Result<usize> {
            error!("send_buffer_size is not supported in windows now");
            Ok(0)
        }

        fn set_recv_buffer_size(&self, _size: usize) -> Result<()> {
            error!("set_recv_buffer_size is not supported in windows now");
            Ok(())
        }

        fn recv_buffer_size(&self) -> Result<usize> {
            error!("recv_buffer_size is not supported in windows now");
            Ok(0)
        }
    }
}

#[cfg(unix)]
pub use self::unix::*;

#[cfg(windows)]
pub use self::windows::*;
