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

    use nix::Error as NixError;
    use nix::sys::socket;
    use nix::sys::socket::sockopt;

    impl<T: AsRawFd> SocketOpt for T {
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

    #[cfg(test)]
    mod tests {
        use std::os::unix::io::AsRawFd;
        use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
        use mio::tcp::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};

        use util::sockopt::SocketOpt;

        #[cfg(unix)]
        fn test_sock_opt<T: AsRawFd>(socket: &T) {
            // For linux, getsockopt will return doubled value set by setsockopt.
            // But for Mac OS X, getsockopt may return the same value. So we can only
            // check value changed.
            socket.set_send_buffer_size(4096).unwrap();
            let mio_s1 = socket.send_buffer_size().unwrap();
            socket.set_send_buffer_size(8192).unwrap();
            let mio_s2 = socket.send_buffer_size().unwrap();
            assert!(mio_s2 != mio_s1,
                    format!("{} should not equal {}", mio_s2, mio_s1));

            socket.set_recv_buffer_size(4096).unwrap();
            let mio_r1 = socket.recv_buffer_size().unwrap();
            socket.set_recv_buffer_size(8192).unwrap();
            let mio_r2 = socket.recv_buffer_size().unwrap();
            assert!(mio_r2 != mio_r1,
                    format!("{} should not equal {}", mio_r2, mio_r1));
        }

        #[cfg(unix)]
        #[test]
        fn test_mio_sock_opt() {
            let addr = "127.0.0.1:0".parse().unwrap();
            let mio_server = MioTcpListener::bind(&addr).unwrap();
            let mio_addr = &format!("{}", mio_server.local_addr().unwrap());
            let mio_sock = MioTcpStream::connect(&mio_addr.parse().unwrap()).unwrap();

            test_sock_opt(&mio_sock);
        }

        #[cfg(unix)]
        #[test]
        fn test_std_sock_opt() {
            let std_server = StdTcpListener::bind("127.0.0.1:0").unwrap();
            let std_addr: SocketAddr =
                (&format!("{}", std_server.local_addr().unwrap())).parse().unwrap();
            let std_sock = StdTcpStream::connect(std_addr).unwrap();

            test_sock_opt(&std_sock);
        }
    }
}


#[cfg(windows)]
mod windows {
    use super::SocketOpt;

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
