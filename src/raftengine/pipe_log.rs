use errno;
use libc;
use std::cmp;
use std::collections::VecDeque;
use std::ffi::CString;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::u64;

use super::log_batch::{LogBatch, LogItemType};
use super::metrics::*;
use super::Result;

const LOG_SUFFIX: &str = ".raftlog";
const LOG_SUFFIX_LEN: usize = 8;
const FILE_NUM_LEN: usize = 16;
const FILE_NAME_LEN: usize = FILE_NUM_LEN + LOG_SUFFIX_LEN;
pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const VERSION: &[u8] = b"v1.0.0";
const INIT_FILE_NUM: u64 = 1;
const DEFAULT_FILES_COUNT: usize = 32;

#[cfg(target_os = "linux")]
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;
#[cfg(target_os = "linux")]
const NEW_FILE_MODE: libc::mode_t = libc::S_IRUSR | libc::S_IWUSR;
#[cfg(not(target_os = "linux"))]
const NEW_FILE_MODE: libc::c_uint = (libc::S_IRUSR | libc::S_IWUSR) as libc::c_uint;

struct LogManager {
    pub first_file_num: u64,
    pub active_file_num: u64,

    pub active_log_fd: libc::c_int,
    pub active_log_size: usize,
    pub active_log_capacity: usize,
    pub last_sync_size: usize,

    pub all_files: VecDeque<libc::c_int>,
}

impl LogManager {
    pub fn new() -> Self {
        Self {
            first_file_num: INIT_FILE_NUM,
            active_file_num: INIT_FILE_NUM,
            active_log_fd: 0,
            active_log_size: 0,
            active_log_capacity: 0,
            last_sync_size: 0,
            all_files: VecDeque::with_capacity(DEFAULT_FILES_COUNT),
        }
    }
}

pub struct PipeLog {
    log_manager: RwLock<LogManager>,

    rotate_size: usize,

    dir: String,

    bytes_per_sync: usize,

    // Used when recovering from disk.
    current_read_file_num: u64,

    write_lock: Mutex<()>,
}

impl PipeLog {
    pub fn new(dir: &str, bytes_per_sync: usize, rotate_size: usize) -> PipeLog {
        PipeLog {
            log_manager: RwLock::new(LogManager::new()),
            rotate_size,
            dir: dir.to_string(),
            bytes_per_sync,
            current_read_file_num: 0,
            write_lock: Mutex::new(()),
        }
    }

    pub fn open(dir: &str, bytes_per_sync: usize, rotate_size: usize) -> Result<PipeLog> {
        let path = Path::new(dir);
        if !path.exists() {
            info!("Create raft log directory: {}", dir);
            fs::create_dir(dir)
                .unwrap_or_else(|e| panic!("Create raft log directory failed, err: {:?}", e));
        }

        if !path.is_dir() {
            return Err(box_err!("Not directory."));
        }

        let mut min_file_num: u64 = u64::MAX;
        let mut max_file_num: u64 = 0;
        let mut log_files = vec![];
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            if !file_path.is_file() {
                continue;
            }

            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if file_name.ends_with(LOG_SUFFIX) && file_name.len() == FILE_NAME_LEN {
                let file_num = match extract_file_num(file_name) {
                    Ok(num) => num,
                    Err(_) => {
                        continue;
                    }
                };
                min_file_num = cmp::min(min_file_num, file_num);
                max_file_num = cmp::max(max_file_num, file_num);
                log_files.push(file_name.to_string());
            }
        }

        // Initialize.
        let mut pipe_log = PipeLog::new(dir, bytes_per_sync, rotate_size);
        if log_files.is_empty() {
            {
                let mut manager = pipe_log.log_manager.write().unwrap();
                let new_fd = new_log_file(&pipe_log.dir, manager.active_file_num);
                manager.active_log_fd = new_fd;
                manager.all_files.push_back(new_fd);
            }
            pipe_log.write_header()?;
            return Ok(pipe_log);
        }

        log_files.sort();
        log_files.dedup();
        if log_files.len() as u64 != max_file_num - min_file_num + 1 {
            return Err(box_err!("Corruption occurs"));
        }

        {
            let mut manager = pipe_log.log_manager.write().unwrap();
            manager.first_file_num = min_file_num;
            manager.active_file_num = max_file_num;
        }
        pipe_log.open_all_files()?;
        Ok(pipe_log)
    }

    fn open_all_files(&mut self) -> Result<()> {
        let mut manager = self.log_manager.write().unwrap();
        let mut current_file = manager.first_file_num;
        while current_file <= manager.active_file_num {
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(current_file));

            let mode = if current_file < manager.active_file_num {
                // Open inactive files with readonly mode.
                libc::O_RDONLY
            } else {
                // Open active file with readwrite mode.
                libc::O_RDWR
            };

            let path_cstr = CString::new(path.as_path().to_str().unwrap().as_bytes()).unwrap();
            let fd = unsafe { libc::open(path_cstr.as_ptr(), mode) };
            if fd < 0 {
                panic!("open file failed, err {}", errno::errno().to_string());
            }
            manager.all_files.push_back(fd);
            if current_file == manager.active_file_num {
                manager.active_log_fd = fd;
            }
            current_file += 1;
        }
        Ok(())
    }

    pub fn fread(&self, file_num: u64, offset: u64, len: u64) -> Result<Vec<u8>> {
        let manager = self.log_manager.read().unwrap();
        if file_num < manager.first_file_num || file_num > manager.active_file_num {
            return Err(box_err!("File not exist, file number {}", file_num));
        }

        let mut result: Vec<u8> = Vec::with_capacity(len as usize);
        let buf = result.as_mut_ptr();
        unsafe {
            let fd = manager.all_files[(file_num - manager.first_file_num) as usize];
            loop {
                let ret_size = libc::pread(
                    fd,
                    buf as *mut libc::c_void,
                    len as libc::size_t,
                    offset as libc::off_t,
                );
                if ret_size < 0 {
                    let err = errno::errno();
                    if err.0 == libc::EAGAIN {
                        continue;
                    }
                    panic!("pread failed, err {}", err.to_string());
                }
                if ret_size as u64 != len {
                    error!(
                        "Pread failed, expected return size {}, actual return size {}",
                        len, ret_size
                    );
                    return Err(box_err!(
                        "Pread failed, expected return size {}, actual return size {}",
                        len,
                        ret_size
                    ));
                }
                break;
            }
            result.set_len(len as usize);
        }

        Ok(result)
    }

    pub fn close(&self) -> Result<()> {
        let _write_lock = self.write_lock.lock().unwrap();
        {
            let active_log_size = {
                let manager = self.log_manager.read().unwrap();
                manager.active_log_size
            };
            self.truncate_active_log(active_log_size)?;
        }
        unsafe {
            let manager = self.log_manager.read().unwrap();
            for fd in manager.all_files.iter() {
                libc::close(*fd);
            }
        }
        Ok(())
    }

    fn append(&self, content: &[u8], sync: bool) -> Result<(u64, u64)> {
        APPEND_LOG_SIZE_HISTOGRAM.observe(content.len() as f64);

        let (active_log_fd, mut active_log_size, last_sync_size, file_num, offset) = {
            let manager = self.log_manager.read().unwrap();
            (
                manager.active_log_fd,
                manager.active_log_size,
                manager.last_sync_size,
                manager.active_file_num,
                manager.active_log_size as u64,
            )
        };
        #[cfg(target_os = "linux")]
        {
            // Use fallocate to pre-allocate disk space for active file. fallocate is faster than File::set_len,
            // because it will not fill the space with 0s, but File::set_len does.
            let (new_size, mut active_log_capacity) = {
                let manager = self.log_manager.read().unwrap();
                (
                    manager.active_log_size + content.len(),
                    manager.active_log_capacity,
                )
            };
            while active_log_capacity < new_size {
                let allocate_ret = unsafe {
                    libc::fallocate(
                        active_log_fd,
                        libc::FALLOC_FL_KEEP_SIZE,
                        active_log_capacity as libc::off_t,
                        FILE_ALLOCATE_SIZE as libc::off_t,
                    )
                };

                if allocate_ret != 0 {
                    panic!(
                        "Allocate disk space for active log failed, ret {}, err {}",
                        allocate_ret,
                        errno::errno().to_string()
                    );
                }
                {
                    let mut manager = self.log_manager.write().unwrap();
                    manager.active_log_capacity += FILE_ALLOCATE_SIZE;
                    active_log_capacity = manager.active_log_capacity;
                }
            }
        }

        // Write to file
        let mut written_bytes: usize = 0;
        let len = content.len();
        while written_bytes < len {
            let write_ret = unsafe {
                libc::pwrite(
                    active_log_fd,
                    content.as_ptr().add(written_bytes) as *const libc::c_void,
                    (len - written_bytes) as libc::size_t,
                    active_log_size as libc::off_t,
                )
            };
            if write_ret >= 0 {
                active_log_size += write_ret as usize;
                written_bytes += write_ret as usize;
                continue;
            }
            let err = errno::errno();
            if err.0 != libc::EAGAIN {
                panic!("Write to active log failed, err {}", err.to_string());
            }
        }
        {
            // Update active log size.
            let mut manager = self.log_manager.write().unwrap();
            manager.active_log_size = active_log_size;
        }

        // Sync data if needed.
        if sync
            || self.bytes_per_sync > 0 && active_log_size - last_sync_size >= self.bytes_per_sync
        {
            let sync_ret = unsafe { libc::fsync(active_log_fd) };
            if sync_ret != 0 {
                panic!("fsync failed, err {}", errno::errno().to_string());
            }
            {
                // Update last sync size.
                let mut manager = self.log_manager.write().unwrap();
                manager.last_sync_size = active_log_size;
            }
        }

        // Rotate if needed
        if active_log_size >= self.rotate_size {
            self.rotate_log();
        }

        Ok((file_num, offset))
    }

    fn write_header(&self) -> Result<(u64, u64)> {
        // Write HEADER.
        let mut header = Vec::with_capacity(FILE_MAGIC_HEADER.len() + VERSION.len());
        header.extend_from_slice(FILE_MAGIC_HEADER);
        header.extend_from_slice(VERSION);
        self.append(header.as_slice(), true)
    }

    fn rotate_log(&self) {
        {
            let active_log_size = {
                let manager = self.log_manager.read().unwrap();
                manager.active_log_size
            };
            self.truncate_active_log(active_log_size).unwrap();
        }

        // New log file.
        let next_file_num = {
            let manager = self.log_manager.read().unwrap();
            manager.active_file_num + 1
        };
        let new_fd = new_log_file(&self.dir, next_file_num);
        {
            let mut manager = self.log_manager.write().unwrap();
            manager.all_files.push_back(new_fd);
            manager.active_log_fd = new_fd;
            manager.active_log_size = 0;
            manager.active_log_capacity = 0;
            manager.last_sync_size = 0;
            manager.active_file_num = next_file_num;
        }

        // Write Header
        self.write_header()
            .unwrap_or_else(|e| panic!("Write header failed, error {:?}", e));
    }

    pub fn append_log_batch(&self, batch: &LogBatch, sync: bool) -> Result<u64> {
        if let Some(content) = batch.encode_to_bytes() {
            let (file_num, offset) = {
                let _write_lock = self.write_lock.lock().unwrap();
                self.append(&content, sync)?
            };
            for item in batch.items.borrow_mut().iter_mut() {
                match item.item_type {
                    LogItemType::Entries => item
                        .entries
                        .as_mut()
                        .unwrap()
                        .update_offset_when_needed(file_num, offset),
                    LogItemType::KV | LogItemType::CMD => {}
                }
            }
            Ok(file_num)
        } else {
            Ok(0)
        }
    }

    pub fn purge_to(&self, file_num: u64) -> Result<()> {
        let (mut first_file_num, active_file_num) = {
            let manager = self.log_manager.read().unwrap();
            (manager.first_file_num, manager.active_file_num)
        };
        PIPE_FILES_COUNT_GAUGE.set((active_file_num - first_file_num + 1) as f64);
        if first_file_num >= file_num {
            debug!("Purge nothing.");
            EXPIRED_FILES_PURGED_HISTOGRAM.observe(0.0);
            return Ok(());
        }

        if file_num > active_file_num {
            return Err(box_err!("Can't purge active log."));
        }

        let old_first_file_num = first_file_num;
        loop {
            if first_file_num >= file_num {
                break;
            }

            // Pop the oldest file.
            let (old_fd, old_file_num) = {
                let mut manager = self.log_manager.write().unwrap();
                manager.first_file_num += 1;
                first_file_num = manager.first_file_num;
                (
                    manager.all_files.pop_front().unwrap(),
                    manager.first_file_num - 1,
                )
            };
            // Close the file.
            let close_res = unsafe { libc::close(old_fd) };
            if close_res != 0 {
                panic!("close file failed, err {}", errno::errno().to_string());
            }

            // Remove the file
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(old_file_num));
            fs::remove_file(path)?;
        }

        debug!(
            "purge {} expired files",
            first_file_num - old_first_file_num
        );
        EXPIRED_FILES_PURGED_HISTOGRAM.observe((first_file_num - old_first_file_num) as f64);
        Ok(())
    }

    // Shrink file size and synchronize.
    pub fn truncate_active_log(&self, offset: usize) -> Result<()> {
        {
            let manager = self.log_manager.read().unwrap();
            let truncate_res =
                unsafe { libc::ftruncate(manager.active_log_fd, offset as libc::off_t) };
            if truncate_res != 0 {
                panic!("Ftruncate file failed, err {}", errno::errno().to_string());
            }
            let sync_res = unsafe { libc::fsync(manager.active_log_fd) };
            if sync_res != 0 {
                panic!("Fsync file failed, err {}", errno::errno().to_string());
            }
        }
        {
            let mut manager = self.log_manager.write().unwrap();
            manager.active_log_size = offset;
            manager.active_log_capacity = offset;
            manager.last_sync_size = manager.active_log_size;
        }

        Ok(())
    }

    pub fn sync(&self) {
        let manager = self.log_manager.read().unwrap();
        let sync_res = unsafe { libc::fsync(manager.active_log_fd) };
        if sync_res != 0 {
            panic!("Fsync failed, err {}", errno::errno().to_string());
        }
    }

    pub fn active_log_size(&self) -> usize {
        let manager = self.log_manager.read().unwrap();
        manager.active_log_size
    }

    pub fn active_file_num(&self) -> u64 {
        let manager = self.log_manager.read().unwrap();
        manager.active_file_num
    }

    pub fn first_file_num(&self) -> u64 {
        let manager = self.log_manager.read().unwrap();
        manager.first_file_num
    }

    pub fn total_size(&self) -> usize {
        let manager = self.log_manager.read().unwrap();
        (manager.active_file_num - manager.first_file_num) as usize * self.rotate_size
            + manager.active_log_size
    }

    pub fn read_next_file(&mut self) -> Result<Option<Vec<u8>>> {
        let manager = self.log_manager.read().unwrap();
        if self.current_read_file_num == 0 {
            self.current_read_file_num = manager.first_file_num;
        }

        if self.current_read_file_num > manager.active_file_num {
            return Ok(None);
        }

        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(self.current_read_file_num));
        self.current_read_file_num += 1;
        let meta = fs::metadata(&path)?;
        let mut vec = Vec::with_capacity(meta.len() as usize);

        // Read the whole file.
        let mut file = File::open(&path)?;
        file.read_to_end(&mut vec)?;
        Ok(Some(vec))
    }

    pub fn files_before(&self, size: usize) -> u64 {
        let cur_size = self.total_size();
        if cur_size > size {
            let count = (cur_size - size) / self.rotate_size;
            let manager = self.log_manager.read().unwrap();
            manager.first_file_num + count as u64
        } else {
            0
        }
    }
}

fn new_log_file(dir: &str, file_num: u64) -> libc::c_int {
    let mut path = PathBuf::from(dir);
    path.push(generate_file_name(file_num));

    let path_cstr = CString::new(path.as_path().to_str().unwrap().as_bytes()).unwrap();
    let fd = unsafe {
        libc::open(
            path_cstr.as_ptr(),
            libc::O_RDWR | libc::O_CREAT,
            NEW_FILE_MODE,
        )
    };
    if fd < 0 {
        panic!("Open file failed, err {}", errno::errno().to_string());
    }
    fd
}

fn generate_file_name(file_num: u64) -> String {
    format!("{:016}{}", file_num, LOG_SUFFIX)
}

fn extract_file_num(file_name: &str) -> Result<u64> {
    match file_name[..FILE_NUM_LEN].parse::<u64>() {
        Ok(num) => Ok(num),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000123.log";
        assert_eq!(extract_file_num(file_name).unwrap(), 123);
        assert_eq!(generate_file_name(123), file_name);

        let invalid_file_name: &str = "0000abc123.log";
        assert!(extract_file_num(invalid_file_name).is_err());
    }

    #[test]
    fn test_pipe_log() {
        let dir = TempDir::new("test_pipe_log").unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let mut pipe_log = PipeLog::open(path, bytes_per_sync, rotate_size).unwrap();
        assert_eq!(pipe_log.first_file_num(), INIT_FILE_NUM);
        assert_eq!(pipe_log.active_file_num(), INIT_FILE_NUM);

        let header_size = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        assert_eq!(
            pipe_log.append(content.as_slice(), false).unwrap(),
            (1, header_size)
        );
        assert_eq!(pipe_log.active_file_num(), 2);
        assert_eq!(
            pipe_log.append(content.as_slice(), false).unwrap(),
            (2, header_size)
        );
        assert_eq!(pipe_log.active_file_num(), 3);

        // purge file 1
        pipe_log.purge_to(2).unwrap();
        assert_eq!(pipe_log.first_file_num(), 2);

        // purge file 2
        pipe_log.purge_to(3).unwrap();
        assert_eq!(pipe_log.first_file_num(), 3);

        // cannot purge active file
        assert!(pipe_log.purge_to(4).is_err());

        // append position
        let s_content = b"short content";
        assert_eq!(
            pipe_log.append(s_content.as_ref(), false).unwrap(),
            (3, header_size)
        );
        assert_eq!(
            pipe_log.append(s_content.as_ref(), false).unwrap(),
            (3, header_size + s_content.len() as u64)
        );
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len() + 2 * s_content.len()
        );

        // fread
        let content_readed = pipe_log
            .fread(3, header_size, s_content.len() as u64)
            .unwrap();
        assert_eq!(content_readed.as_slice(), s_content.as_ref());

        // truncate file
        pipe_log
            .truncate_active_log(FILE_MAGIC_HEADER.len() + VERSION.len())
            .unwrap();
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        assert!(pipe_log
            .truncate_active_log(FILE_MAGIC_HEADER.len() + VERSION.len() + s_content.len())
            .is_err());

        // read next file
        let mut header: Vec<u8> = vec![];
        header.extend(FILE_MAGIC_HEADER);
        header.extend(VERSION);
        let content = pipe_log.read_next_file().unwrap().unwrap();
        assert_eq!(header, content);
        assert!(pipe_log.read_next_file().unwrap().is_none());

        pipe_log.close().unwrap();

        // reopen
        let pipe_log = PipeLog::open(path, bytes_per_sync, rotate_size).unwrap();
        assert_eq!(pipe_log.active_file_num(), 3);
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
    }
}
