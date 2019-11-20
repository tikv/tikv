use futures::Future;
use grpcio::{RpcContext, UnarySink};
use kvproto::diagnosticspb::{
    Diagnostics, SearchLogRequest, SearchLogResponse, ServerInfoRequest, ServerInfoResponse,
};

use crate::server::Error;

/// Service handles the RPC messages for the `Diagnostics` service.
#[derive(Clone)]
pub struct Service {
    log_file: String,
}

impl Service {
    pub fn new(log_file: String) -> Self {
        Service { log_file }
    }
}

impl Diagnostics for Service {
    fn search_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: SearchLogRequest,
        sink: UnarySink<SearchLogResponse>,
    ) {
        let future = log::search(&self.log_file, req)
            .map_err(|err| Error::Other(box_err!("Search log error: {:?}", err)))
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map_err(|e| debug!("Diagnostics rpc failed"; "err" => ?e));
        ctx.spawn(future);
    }

    fn server_info(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: ServerInfoRequest,
        _sink: UnarySink<ServerInfoResponse>,
    ) {
        unimplemented!()
    }
}

mod log {
    use std::convert::From;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Seek, SeekFrom};
    use std::path::Path;

    use chrono::DateTime;
    use nom::bytes::complete::{tag, take, take_until};
    use nom::character::complete::{alpha1, space0, space1};
    use nom::sequence::tuple;
    use nom::*;
    use rev_lines;
    use walkdir::WalkDir;

    use futures::future::{err, ok};
    use futures::Future;
    use kvproto::diagnosticspb::{LogLevel, LogMessage, SearchLogRequest, SearchLogResponse};

    const INVALID_TIMESTAMP: i64 = -1;
    const TIMESTAMP_LENGTH: usize = 30;

    pub struct LogIterator {
        search_files: Vec<(i64, File)>,
        currrent_lines: Option<std::io::Lines<BufReader<File>>>,

        // filter conditions
        begin_time: i64,
        end_time: i64,
        level: Option<LogLevel>,
        filter: String,
    }

    #[derive(Debug)]
    pub enum Error {
        InvalidRequest(String),
        ParseError,
        SearchError(String),
        IOError(std::io::Error),
    }

    impl From<std::io::Error> for Error {
        fn from(err: std::io::Error) -> Self {
            Error::IOError(err)
        }
    }

    impl From<walkdir::Error> for Error {
        fn from(e: walkdir::Error) -> Self {
            Error::SearchError(format!("walk directory: {:?}", e))
        }
    }

    impl LogIterator {
        pub fn new(
            log_file: &str,
            begin_time: i64,
            end_time: i64,
            level: Option<LogLevel>,
            filter: String,
        ) -> Result<Self, Error> {
            let log_path: &Path = log_file.as_ref();
            let log_name = match log_path.file_name() {
                Some(file_name) => match file_name.to_str() {
                    Some(file_name) => file_name,
                    None => return Err(Error::SearchError(format!("Invalid utf8: {}", log_file))),
                },
                None => {
                    return Err(Error::SearchError(format!(
                        "Illegal file name: {}",
                        log_file
                    )))
                }
            };
            let log_dir = match log_path.parent() {
                Some(dir) => dir,
                None => {
                    return Err(Error::SearchError(format!(
                        "Illegal parent dir: {}",
                        log_file
                    )))
                }
            };

            let mut search_files = vec![];
            for entry in WalkDir::new(log_dir) {
                let entry = entry?;
                if !entry.path().is_file() {
                    continue;
                }
                let file_name = match entry.file_name().to_str() {
                    Some(file_name) => file_name,
                    None => continue,
                };
                // Rorated file name have the same prefix with the original file name
                if !file_name.starts_with(log_name) {
                    continue;
                }
                // Open the file
                let mut file = match File::open(entry.path()) {
                    Ok(file) => file,
                    Err(_) => continue,
                };

                let (file_start_time, file_end_time) = match parse_time_range(&file) {
                    Ok((file_start_time, file_end_time)) => (file_start_time, file_end_time),
                    Err(_) => continue,
                };

                if (begin_time < file_start_time && end_time > file_start_time)
                    || (end_time > file_end_time && begin_time < file_end_time)
                {
                    if let Err(err) = file.seek(SeekFrom::Start(0)) {
                        warn!("seek file failed: {}, err: {}", file_name, err);
                        continue;
                    }
                    search_files.push((file_start_time, file));
                }
            }
            search_files.sort_by(|a, b| b.0.cmp(&a.0));
            let current_reader = search_files.pop().map(|file| BufReader::new(file.1));
            Ok(Self {
                search_files,
                currrent_lines: current_reader.map(|reader| reader.lines()),
                begin_time,
                end_time,
                level,
                filter,
            })
        }
    }

    impl Iterator for LogIterator {
        type Item = LogMessage;
        fn next(&mut self) -> Option<Self::Item> {
            loop {
                match &mut self.currrent_lines {
                    Some(lines) => {
                        loop {
                            let line = match lines.next() {
                                Some(line) => line,
                                None => {
                                    self.currrent_lines = self
                                        .search_files
                                        .pop()
                                        .map(|file| BufReader::new(file.1))
                                        .map(|reader| reader.lines());
                                    break;
                                }
                            };
                            let input = match line {
                                Ok(input) => input,
                                Err(err) => {
                                    warn!("read line failed: {:?}", err);
                                    continue;
                                }
                            };
                            if input.len() < TIMESTAMP_LENGTH {
                                continue;
                            }
                            match parse(&input) {
                                Ok((content, meta)) => {
                                    // The remain content timestamp more the end time or this file contains inrecognation formation
                                    if meta.time == INVALID_TIMESTAMP && meta.time > self.end_time {
                                        break;
                                    }
                                    if meta.time < self.begin_time {
                                        continue;
                                    }
                                    if self.level.is_some() && meta.level != self.level {
                                        continue;
                                    }
                                    if self.filter.len() > 0 && !content.contains(&self.filter) {
                                        continue;
                                    }
                                    let mut item = LogMessage::new();
                                    item.set_time(meta.time);
                                    // FIXME
                                    item.set_level(meta.level.unwrap());
                                    item.set_message(content.to_owned());
                                    return Some(item);
                                }
                                Err(err) => {
                                    warn!("parse line failed: {:?}", err);
                                    continue;
                                }
                            }
                        }
                    }
                    None => break,
                }
            }
            None
        }
    }

    struct Meta {
        time: i64,
        level: Option<LogLevel>,
    }

    fn parse_time(input: &str) -> IResult<&str, &str> {
        let (input, (_, _, time, _)) =
            tuple((space0, tag("["), take(TIMESTAMP_LENGTH), tag("]")))(input)?;
        Ok((input, time))
    }

    fn parse_level(input: &str) -> IResult<&str, &str> {
        let (input, (_, _, level, _)) = tuple((space0, tag("["), alpha1, tag("]")))(input)?;
        Ok((input, level))
    }

    #[allow(dead_code)]
    fn parse_file(input: &str) -> IResult<&str, &str> {
        let (input, (_, _, level, _)) =
            tuple((space0, tag("["), take_until("]"), tag("]")))(input)?;
        Ok((input, level))
    }

    /// Parses the single log line and retrieve the log meta and log body.
    fn parse(input: &str) -> IResult<&str, Meta> {
        let (content, (time, level, _)) = tuple((parse_time, parse_level, space1))(input)?;
        Ok((
            content,
            Meta {
                time: match DateTime::parse_from_str(time, "%Y/%m/%d %H:%M:%S%.3f %z") {
                    Ok(t) => t.timestamp_millis(),
                    Err(_) => -1,
                },
                level: match level {
                    "trace" | "TRACE" => Some(LogLevel::Trace),
                    "debug" | "DEBUG" => Some(LogLevel::Debug),
                    "info" | "INFO" => Some(LogLevel::Info),
                    "warn" | "WARN" | "warning" | "WARNING" => Some(LogLevel::Warn),
                    "error" | "ERROR" => Some(LogLevel::Error),
                    "critical" | "CRITICAL" => Some(LogLevel::Critical),
                    _ => None,
                },
            },
        ))
    }

    /// Parses the start time and end time of a log file and return the maximal and minimal
    /// timestamp in unix milliseconds.
    fn parse_time_range(file: &std::fs::File) -> Result<(i64, i64), Error> {
        let buffer = BufReader::new(file);
        let file_start_time = match buffer.lines().nth(0) {
            Some(Ok(line)) => {
                let (_, meta) = parse(&line).map_err(|_| Error::ParseError)?;
                meta.time
            }
            Some(Err(err)) => {
                return Err(err.into());
            }
            None => INVALID_TIMESTAMP,
        };

        let buffer = BufReader::new(file);
        let mut rev_lines = rev_lines::RevLines::with_capacity(512, buffer)?;
        let file_end_time = match rev_lines.nth(0) {
            Some(line) => {
                let (_, meta) = parse(&line).map_err(|_| Error::ParseError)?;
                meta.time
            }
            None => INVALID_TIMESTAMP,
        };
        Ok((file_start_time, file_end_time))
    }

    pub fn search(
        log_file: &str,
        mut req: SearchLogRequest,
    ) -> impl Future<Item = SearchLogResponse, Error = Error> {
        if log_file.is_empty() {
            return err(Error::InvalidRequest("Empty `log_file` path".to_owned()));
        }
        let begin_time = req.get_start_time();
        let end_time = req.get_end_time();
        let level = req.get_level();
        let filter = req.take_pattern();
        let limit = req.get_limit();

        // FIXME: the level can be None
        let iter = match LogIterator::new(log_file, begin_time, end_time, Some(level), filter) {
            Ok(iter) => iter,
            Err(e) => return err(e),
        };

        // FIXME: refactor to use future::Stream
        let mut counter = 0;
        let mut results = vec![];
        for line in iter {
            results.push(line);
            counter += 1;
            if counter >= limit {
                break;
            }
        }

        let mut resp = SearchLogResponse::new();
        resp.set_messages(results.into());
        return ok(resp);
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse_time() {
            assert_eq!(
                parse_time("[2019/08/23 18:09:52.387 +08:00]"),
                Ok(("", "2019/08/23 18:09:52.387 +08:00"))
            );
            assert_eq!(
                parse_time(" [2019/08/23 18:09:52.387 +08:00] ["),
                Ok((" [", "2019/08/23 18:09:52.387 +08:00"))
            );
        }

        #[test]
        fn test_parse_level() {
            assert_eq!(parse_level("[INFO]"), Ok(("", "INFO")));
            assert_eq!(parse_level(" [WARN] ["), Ok((" [", "WARN")));
        }

        #[test]
        fn test_parse_file() {
            assert_eq!(parse_file("[foo.rs:100]"), Ok(("", "foo.rs:100")));
            assert_eq!(parse_file(" [bar.rs:200] ["), Ok((" [", "bar.rs:200")));
        }

        #[test]
        fn test_parse() {
            let cs: Vec<(&str, &str, &str, Option<LogLevel>)> = vec![
                ("[2019/08/23 18:09:52.387 +08:00] [INFO] [foo.rs:100] [some message] [key=val]", "foo.rs:100", "[some message] [key=val]", Some(LogLevel::Info)),
                ("[2019/08/23 18:09:52.387 +08:00]    [INFO] [foo.rs:100] [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Info)),
                ("[2019/08/23 18:09:52.387 +08:00]    [INFO]     [foo.rs:100] [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Info)),
                ("   [2019/08/23 18:09:52.387 +08:00]    [INFO]     [foo.rs:100]    [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Info)),
                ("   [2019/08/23 18:09:52.387 +08:00]    [info]     [foo.rs:100]    [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Info)),
                ("   [2019/08/23 18:09:52.387 +08:00]    [warning]     [foo.rs:100]    [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Warn)),
                ("   [2019/08/23 18:09:52.387 +08:00]    [warn]     [foo.rs:100]    [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",Some(LogLevel::Warn)),
                ("   [2019/08/23 18:09:52.387 +08:00]    [xxx]     [foo.rs:100]    [some message] [key=val]", "foo.rs:100", "[some message] [key=val]",None),
            ];
            for (input, file, content, level) in cs.into_iter() {
                let r = parse(input);
                assert!(r.is_ok());
                let log = r.unwrap();
                assert_eq!(log.0, content);
                assert_eq!(log.1.level, level);
                assert_eq!(log.1.file, file);
            }
        }
    }
}
