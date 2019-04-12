// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub const HIGH_PRI: i32 = -1;

#[cfg(target_os = "linux")]
pub mod thread {
    use libc::{self, c_int};
    use std::io::Error;

    pub fn set_priority(pri: i32) -> Result<(), Error> {
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid);
            if libc::setpriority(libc::PRIO_PROCESS as u32, tid as u32, pri) != 0 {
                let e = Error::last_os_error();
                return Err(e);
            }
            Ok(())
        }
    }

    pub fn get_priority() -> Result<i32, Error> {
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid);
            clear_errno();
            let ret = libc::getpriority(libc::PRIO_PROCESS as u32, tid as u32);
            if ret == -1 {
                let e = Error::last_os_error();
                if let Some(errno) = e.raw_os_error() {
                    if errno != 0 {
                        return Err(e);
                    }
                }
            }
            Ok(ret)
        }
    }

    // Sadly the std lib does not have any support for setting `errno`, so we
    // have to implement this ourselves.
    extern "C" {
        #[link_name = "__errno_location"]
        fn errno_location() -> *mut c_int;
    }

    fn clear_errno() {
        unsafe {
            *errno_location() = 0;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::HIGH_PRI;
        use super::*;
        use std::io::ErrorKind;

        #[test]
        fn test_set_priority() {
            // priority is a value in range -20 to 19, the default priority
            // is 0, lower priorities cause more favorable scheduling.
            assert_eq!(get_priority().unwrap(), 0);
            set_priority(10).unwrap();
            assert_eq!(get_priority().unwrap(), 10);

            // only users who have `SYS_NICE_CAP` capability can increase priority.
            let ret = set_priority(HIGH_PRI);
            if let Err(e) = ret {
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
            } else {
                assert_eq!(get_priority().unwrap(), HIGH_PRI);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub mod thread {
    use std::io::Error;

    pub fn set_priority(_: i32) -> Result<(), Error> {
        Ok(())
    }

    pub fn get_priority() -> Result<i32, Error> {
        Ok(0)
    }
}
