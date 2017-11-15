
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, ErrorKind, Read, Write};
use std::u64;
use std::cmp;

use super::Result;
use super::log_batch::LogBatch;
use super::metrics::*;

const LOG_SUFFIX: &'static str = ".log";
const LOG_SUFFIX_LEN: usize = 4;
const FILE_NUM_LEN: usize = 10;
const FILE_NAME_LEN: usize = FILE_NUM_LEN + LOG_SUFFIX_LEN;
pub const FILE_MAGIC_HEADER: &'static [u8] = b"RAFT-LOG-FILE-HEADER";
pub const VERSION: &'static [u8] = b"v1.0.0";
const INIT_FILE_NUM: u64 = 1;

pub struct PipeLog {
    first_file_num: u64,
    active_file_num: u64,

    active_log: Option<File>,
    active_log_size: usize,
    rotate_size: usize,

    dir: String,

    bytes_per_sync: usize,
    last_sync_size: usize,

    // Use to read.
    current_read_file_num: u64,
}

impl PipeLog {
    pub fn new(dir: &str, bytes_per_sync: usize, rotate_size: usize) -> PipeLog {
        PipeLog {
            first_file_num: INIT_FILE_NUM,
            active_file_num: INIT_FILE_NUM,
            active_log: None,
            active_log_size: 0,
            rotate_size: rotate_size,
            dir: dir.to_string(),
            bytes_per_sync: bytes_per_sync,
            last_sync_size: 0,
            current_read_file_num: 0,
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
            let file = pipe_log.new_log_file(pipe_log.active_file_num);
            pipe_log.active_log = Some(file);
            pipe_log.active_log_size = FILE_MAGIC_HEADER.len() + VERSION.len();
            return Ok(pipe_log);
        }

        log_files.sort();
        log_files.dedup();
        if log_files.len() as u64 != max_file_num - min_file_num + 1 {
            return Err(box_err!("Corruption occurs"));
        }

        pipe_log.first_file_num = min_file_num;
        pipe_log.active_file_num = max_file_num;
        pipe_log.open_active_log()?;
        Ok(pipe_log)
    }

    pub fn close(&mut self) -> Result<()> {
        self.active_log.as_mut().unwrap().sync_all()?;
        self.active_log.take();
        Ok(())
    }

    pub fn append(&mut self, content: &[u8], sync: bool) -> Result<u64> {
        let file_num = self.active_file_num;

        PipeLog::write_all(self.active_log.as_mut().unwrap(), content)?;
        self.active_log_size += content.len();
        if sync ||
            self.bytes_per_sync > 0 &&
                self.active_log_size - self.last_sync_size >= self.bytes_per_sync
        {
            self.active_log.as_mut().unwrap().sync_data()?;
            self.last_sync_size = self.active_log_size;
        }

        // Rotate if needed.
        if self.active_log_size >= self.rotate_size {
            self.rotate_log();
        }

        Ok(file_num)
    }

    fn write_all(f: &mut File, data: &[u8]) -> io::Result<()> {
        while let Err(e) = f.write(data) {
            if e.kind() == ErrorKind::Interrupted {
                warn!("Write is interrupted, retry");
                continue;
            } else {
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn append_log_batch(&mut self, batch: &mut LogBatch, sync: bool) -> Result<u64> {
        match batch.encode_to_bytes() {
            Some(content) => self.append(&content, sync),
            None => Ok(0),
        }
    }

    pub fn purge_to(&mut self, file_num: u64) -> Result<()> {
        PIPE_FILES_COUNT_GAUGE.set((self.active_file_num - self.first_file_num + 1) as f64);
        if self.first_file_num >= file_num {
            debug!("Purge nothing.");
            EXPIRED_FILES_PURGED_HISTOGRAM.observe(0.0);
            return Ok(());
        }

        if file_num > self.active_file_num {
            return Err(box_err!("Can't purge active log."));
        }

        let old_first_file_num = self.first_file_num;
        loop {
            if self.first_file_num >= file_num {
                break;
            }
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(self.first_file_num));
            fs::remove_file(path)?;

            self.first_file_num += 1;
        }
        debug!(
            "purge {} expired files",
            self.first_file_num - old_first_file_num
        );
        EXPIRED_FILES_PURGED_HISTOGRAM.observe((self.first_file_num - old_first_file_num) as f64);
        Ok(())
    }

    pub fn truncate_active_log(&mut self, offset: u64) -> Result<()> {
        if offset > self.active_log_size as u64 {
            return Err(box_err!(
                "Offset {} is larger than file size {} when call truncate",
                offset,
                self.active_log_size
            ));
        }
        if let Err(e) = self.active_log.as_ref().unwrap().set_len(offset) {
            return Err(e.into());
        }
        if let Err(e) = self.active_log.as_ref().unwrap().sync_all() {
            return Err(e.into());
        }
        self.active_log_size = offset as usize;
        self.last_sync_size = self.active_log_size;

        Ok(())
    }

    pub fn sync_data(&mut self) -> Result<()> {
        self.active_log.as_mut().unwrap().sync_data()?;
        Ok(())
    }

    fn rotate_log(&mut self) {
        // Synchronize.
        self.active_log
            .as_mut()
            .unwrap()
            .sync_all()
            .unwrap_or_else(|e| panic!("Fail to sync log, error: {:?}", e));

        // New log file.
        let next_file_num = self.active_file_num + 1;
        let new_log_file = self.new_log_file(next_file_num);
        self.active_log = Some(new_log_file);
        self.active_log_size = FILE_MAGIC_HEADER.len() + VERSION.len();
        self.last_sync_size = self.active_log_size;
        self.active_file_num = next_file_num;
    }

    fn open_active_log(&mut self) -> Result<()> {
        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(self.active_file_num));

        // Get active log size.
        let meta = fs::metadata(&path)?;
        self.active_log_size = meta.len() as usize;
        self.last_sync_size = self.active_log_size;

        // Open file in append mode.
        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap_or_else(|e| panic!("Create file failed, error: {:?}", e));
        self.active_log = Some(file);
        Ok(())
    }

    fn new_log_file(&self, file_num: u64) -> File {
        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(file_num));

        // Create new file in append mode.
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.clone())
            .unwrap_or_else(|e| panic!("Create file failed, error: {:?}", e));

        // Write HEADER.
        let mut header = Vec::with_capacity(FILE_MAGIC_HEADER.len() + VERSION.len());
        header.extend_from_slice(FILE_MAGIC_HEADER);
        header.extend_from_slice(VERSION);
        match PipeLog::write_all(&mut file, header.as_slice()) {
            Err(e) => {
                fs::remove_file(path).unwrap();
                panic!("Write HEADER failed, error: {:?}", e)
            }
            Ok(()) => {
                file.sync_all().unwrap();
                file
            }
        }
    }

    pub fn read_next_file(&mut self) -> Result<Option<Vec<u8>>> {
        if self.current_read_file_num == 0 {
            self.current_read_file_num = self.first_file_num;
        }

        if self.current_read_file_num > self.active_file_num {
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

    pub fn active_log_size(&self) -> usize {
        self.active_log_size
    }

    pub fn active_file_num(&self) -> u64 {
        self.active_file_num
    }

    pub fn first_file_num(&self) -> u64 {
        self.first_file_num
    }

    pub fn total_size(&self) -> usize {
        (self.active_file_num - self.first_file_num) as usize * self.rotate_size +
            self.active_log_size
    }

    pub fn files_should_evict(&self, size_limit: usize) -> u64 {
        let cur_size = self.total_size();
        if cur_size > size_limit {
            let count = (cur_size - size_limit) / self.rotate_size;
            self.first_file_num + count as u64
        } else {
            self.first_file_num
        }
    }
}

fn generate_file_name(file_num: u64) -> String {
    format!("{:010}{}", file_num, LOG_SUFFIX)
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

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        assert_eq!(pipe_log.append(content.as_slice(), false).unwrap(), 1);
        assert_eq!(pipe_log.active_file_num(), 2);
        assert_eq!(pipe_log.append(content.as_slice(), false).unwrap(), 2);
        assert_eq!(pipe_log.active_file_num(), 3);

        // purge file 1
        pipe_log.purge_to(2).unwrap();
        assert_eq!(pipe_log.first_file_num(), 2);

        // purge file 2
        pipe_log.purge_to(3).unwrap();
        assert_eq!(pipe_log.first_file_num(), 3);

        // cannot purge active file
        assert!(pipe_log.purge_to(4).is_err());

        // truncate file
        let s_content = b"short content";
        assert_eq!(pipe_log.append(s_content.as_ref(), false).unwrap(), 3);
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len() + s_content.len()
        );
        pipe_log
            .truncate_active_log((FILE_MAGIC_HEADER.len() + VERSION.len()) as u64)
            .unwrap();
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        assert!(
            pipe_log
                .truncate_active_log(
                    (FILE_MAGIC_HEADER.len() + VERSION.len() + s_content.len()) as u64
                )
                .is_err()
        );

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
