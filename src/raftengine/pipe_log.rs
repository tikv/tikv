
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{Read, Write};
use std::u64;
use std::cmp;

use super::Result;
use super::log_batch::LogBatch;

const LOG_SUFFIX: &'static str = ".log";
const LOG_SUFFIX_LEN: usize = 4;
const FILE_NUM_LEN: usize = 10;
const FILE_NAME_LEN: usize = FILE_NUM_LEN + LOG_SUFFIX_LEN;
pub const FILE_MAGIC_HEADER: &'static [u8] = b"RAFT-LOG-FILE-HEADER";
const INIT_FILE_NUM: u64 = 1;
const MB: usize = 1024 * 1024;
const LOG_MAX_SIZE: usize = 128 * MB;

pub struct PipeLog {
    first_file_num: u64,
    active_file_num: u64,

    active_log: Option<File>,
    active_log_size: usize,
    rotate_size: usize,

    dir: String,

    // Use to read.
    current_read_file_num: u64,
}

impl PipeLog {
    pub fn new(dir: &str) -> PipeLog {
        PipeLog {
            first_file_num: INIT_FILE_NUM,
            active_file_num: INIT_FILE_NUM,
            active_log: None,
            active_log_size: 0,
            rotate_size: LOG_MAX_SIZE,
            dir: dir.to_string(),
            current_read_file_num: 0,
        }
    }

    pub fn open(dir: &str) -> Result<PipeLog> {
        let path = Path::new(dir);
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
        let mut pipe_log = PipeLog::new(dir);
        if log_files.is_empty() {
            let file = pipe_log.new_log_file(pipe_log.active_file_num);
            pipe_log.active_log = Some(file);
            pipe_log.active_log_size = FILE_MAGIC_HEADER.len();
            return Ok(pipe_log);
        }

        log_files.sort();
        log_files.dedup();
        if log_files.len() as u64 != max_file_num - min_file_num + 1 {
            return Err(box_err!("Corruption occurs"));
        }

        pipe_log.open_active_log()?;
        Ok(pipe_log)
    }

    pub fn append(&mut self, content: &[u8], sync: bool) -> Result<u64> {
        let file_num = self.active_file_num;

        self.active_log.as_mut().unwrap().write_all(content)?;
        if sync {
            self.active_log.as_mut().unwrap().sync_data()?;
        }
        self.active_log_size += content.len();

        // Rotate if needed.
        if self.active_log_size >= self.rotate_size {
            self.rotate_log();
        }

        Ok(file_num)
    }

    pub fn append_log_batch(&mut self, batch: &LogBatch, sync: bool) -> Result<u64> {
        match batch.to_vec() {
            Some(content) => self.append(&content, sync),
            None => Ok(0),
        }
    }

    pub fn purge_to(&mut self, file_num: u64) -> Result<()> {
        if self.first_file_num >= file_num {
            debug!("Purge nothing.");
            return Ok(());
        }

        if file_num > self.active_file_num {
            return Err(box_err!("Can't purge active log."));
        }

        loop {
            if self.first_file_num >= file_num {
                break;
            }
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(self.first_file_num));
            fs::remove_file(path)?;

            self.first_file_num += 1;
        }
        Ok(())
    }

    pub fn truncate_active_log(&self, offset: u64) -> Result<()> {
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
        self.active_log_size = FILE_MAGIC_HEADER.len();
        self.active_file_num = next_file_num;
    }

    fn open_active_log(&mut self) -> Result<()> {
        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(self.active_file_num));

        // Get active log size.
        let meta = fs::metadata(&path)?;
        self.active_log_size = meta.len() as usize;

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
            .open(path)
            .unwrap_or_else(|e| panic!("Create file failed, error: {:?}", e));

        // Write HEADER.
        file.write_all(FILE_MAGIC_HEADER)
            .unwrap_or_else(|e| panic!("Write file_magic_header failed, error: {:?}", e));
        file.sync_all().unwrap();
        file
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

    pub fn active_file_num(&self) -> u64 {
        self.active_file_num
    }

    pub fn first_file_num(&self) -> u64 {
        self.first_file_num
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
