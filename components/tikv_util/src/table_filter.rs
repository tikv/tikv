// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::LazyLock,
};

use regex::Regex;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct TableFilterError {
    message: String,
}

pub type Result<T> = std::result::Result<T, TableFilterError>;

impl TableFilterError {
    fn new(message: String) -> Self {
        TableFilterError { message }
    }
}

#[derive(Clone, Debug)]
pub struct TableFilter {
    rules: Vec<TableRule>,
}

impl TableFilter {
    pub fn parse(filters: &[String]) -> Result<Self> {
        let mut parser = TableRulesParser::new();
        for filter in filters {
            parser.parse_line(filter, true)?;
        }
        parser.rules.reverse();
        Ok(TableFilter { rules: parser.rules })
    }

    pub fn case_insensitive(self) -> Result<Self> {
        let mut lowered = Vec::with_capacity(self.rules.len());
        for rule in self.rules {
            lowered.push(TableRule {
                schema: rule.schema.to_lower()?,
                table: rule.table.to_lower()?,
                positive: rule.positive,
            });
        }
        Ok(TableFilter { rules: lowered })
    }

    pub fn matches_table(&self, schema: &str, table: &str) -> bool {
        for rule in &self.rules {
            if rule.schema.matches(schema) && rule.table.matches(table) {
                return rule.positive;
            }
        }
        false
    }

    pub fn matches_schema(&self, schema: &str) -> bool {
        for rule in &self.rules {
            if rule.schema.matches(schema) && (rule.positive || rule.table.matches_all()) {
                return rule.positive;
            }
        }
        false
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Clone, Debug)]
struct TableRule {
    schema: Matcher,
    table: Matcher,
    positive: bool,
}

#[derive(Clone, Debug)]
enum Matcher {
    String(String),
    Regex { pattern: String, regex: Regex },
    True,
}

impl Matcher {
    fn matches(&self, name: &str) -> bool {
        match self {
            Matcher::String(value) => value == name,
            Matcher::Regex { regex, .. } => regex.is_match(name),
            Matcher::True => true,
        }
    }

    fn matches_all(&self) -> bool {
        matches!(self, Matcher::True)
    }

    fn to_lower(&self) -> Result<Self> {
        match self {
            Matcher::String(value) => Ok(Matcher::String(value.to_lowercase())),
            Matcher::Regex { pattern, .. } => {
                let pat = format!("(?i){}", pattern);
                Matcher::new_regex(&pat)
                    .map_err(|err| TableFilterError::new(format!("invalid pattern: {}", err)))
            }
            Matcher::True => Ok(Matcher::True),
        }
    }

    fn new_regex(pattern: &str) -> std::result::Result<Self, regex::Error> {
        if pattern == "(?s)^.*$" {
            return Ok(Matcher::True);
        }
        let regex = Regex::new(pattern)?;
        Ok(Matcher::Regex {
            pattern: pattern.to_string(),
            regex,
        })
    }
}

struct TableRulesParser {
    rules: Vec<TableRule>,
    matcher: MatcherParser,
}

impl TableRulesParser {
    fn new() -> Self {
        TableRulesParser {
            rules: Vec::new(),
            matcher: MatcherParser {
                file_name: "<cmdline>".to_string(),
                line_num: 1,
            },
        }
    }

    fn parse_line(&mut self, line: &str, can_import: bool) -> Result<()> {
        let line = line.trim_matches(|c| c == ' ' || c == '\t');
        if line.is_empty() {
            return Ok(());
        }
        let mut positive = true;
        let mut line = line;
        match line.chars().next().unwrap() {
            '#' => return Ok(()),
            '!' => {
                positive = false;
                line = &line[1..];
            }
            '@' => {
                if !can_import {
                    return Err(self
                        .matcher
                        .error("importing filter files recursively is not allowed"));
                }
                return self.import_file(&line[1..]);
            }
            _ => {}
        }

        let (schema, rest) = self.matcher.parse_pattern(line, true)?;
        if rest.is_empty() {
            return Err(self.matcher.error("wrong table pattern"));
        }
        if !rest.starts_with('.') {
            return Err(self
                .matcher
                .error("syntax error: missing '.' between schema and table patterns"));
        }
        let (table, rest) = self.matcher.parse_pattern(&rest[1..], true)?;
        if !rest.is_empty() {
            return Err(self
                .matcher
                .error("syntax error: stray characters after table pattern"));
        }
        self.rules.push(TableRule {
            schema,
            table,
            positive,
        });
        Ok(())
    }

    fn import_file(&mut self, file_name: &str) -> Result<()> {
        let file = File::open(file_name)
            .map_err(|err| self.matcher.annotate(err, "cannot open filter file"))?;
        let reader = BufReader::new(file);
        let old_file_name = self.matcher.file_name.clone();
        let old_line_num = self.matcher.line_num;
        self.matcher.file_name = file_name.to_string();
        self.matcher.line_num = 1;

        for line in reader.lines() {
            let line = line
                .map_err(|err| self.matcher.annotate(err, "cannot read filter file"))?;
            self.parse_line(&line, false)?;
            self.matcher.line_num += 1;
        }

        self.matcher.file_name = old_file_name;
        self.matcher.line_num = old_line_num;
        Ok(())
    }
}

struct MatcherParser {
    file_name: String,
    line_num: i64,
}

impl MatcherParser {
    fn error(&self, message: &str) -> TableFilterError {
        TableFilterError::new(format!(
            "at {}:{}: {}",
            self.file_name, self.line_num, message
        ))
    }

    fn annotate(&self, err: impl std::fmt::Display, message: &str) -> TableFilterError {
        TableFilterError::new(format!(
            "at {}:{}: {}: {}",
            self.file_name, self.line_num, message, err
        ))
    }

    fn parse_pattern<'a>(
        &self,
        line: &'a str,
        needs_dot_separator: bool,
    ) -> Result<(Matcher, &'a str)> {
        if line.is_empty() {
            return Err(self.error("syntax error: missing pattern"));
        }
        match line.as_bytes()[0] {
            b'/' => self.parse_regex_pattern(line),
            b'"' => self.parse_quoted_pattern(line, '"', "\"\""),
            b'`' => self.parse_quoted_pattern(line, '`', "``"),
            _ => self.parse_wildcard_pattern(line, needs_dot_separator),
        }
    }

    fn parse_regex_pattern<'a>(&self, line: &'a str) -> Result<(Matcher, &'a str)> {
        let end = find_regex_end(line)
            .ok_or_else(|| self.error("syntax error: incomplete regexp"))?;
        if end <= 2 {
            return Err(self.error("syntax error: incomplete regexp"));
        }
        let pattern = &line[1..end - 1];
        let matcher = Matcher::new_regex(pattern)
            .map_err(|err| self.annotate(err, "invalid pattern"))?;
        Ok((matcher, &line[end..]))
    }

    fn parse_quoted_pattern<'a>(
        &self,
        line: &'a str,
        quote: char,
        escape_pair: &str,
    ) -> Result<(Matcher, &'a str)> {
        let mut out = String::new();
        let mut idx = 1;
        let mut has_content = false;
        while idx < line.len() {
            let ch = line[idx..].chars().next().unwrap();
            if ch == quote {
                if line[idx..].starts_with(escape_pair) {
                    out.push(quote);
                    idx += escape_pair.len();
                    has_content = true;
                    continue;
                }
                if !has_content {
                    return Err(self.error("syntax error: incomplete quoted identifier"));
                }
                let rest = &line[idx + ch.len_utf8()..];
                return Ok((Matcher::String(out), rest));
            }
            out.push(ch);
            has_content = true;
            idx += ch.len_utf8();
        }
        Err(self.error("syntax error: incomplete quoted identifier"))
    }

    fn parse_wildcard_pattern<'a>(
        &self,
        line: &'a str,
        needs_dot_separator: bool,
    ) -> Result<(Matcher, &'a str)> {
        let mut literal = String::new();
        let mut pattern = String::from("(?s)^");
        let mut is_literal = true;
        let mut idx = 0;
        while idx < line.len() {
            let ch = line[idx..].chars().next().unwrap();
            match ch {
                '\\' => {
                    if idx + 1 >= line.len() {
                        return Err(self.error("syntax error: cannot place \\ at end of line"));
                    }
                    let next = line[idx + 1..].chars().next().unwrap();
                    if next.is_ascii_alphanumeric() {
                        return Err(self.error(&format!(
                            "cannot escape a letter or number (\\{}), it is reserved for future extension",
                            next
                        )));
                    }
                    if is_literal {
                        literal.push(next);
                    }
                    if next.is_ascii() {
                        pattern.push('\\');
                    }
                    pattern.push(next);
                    idx += 1 + next.len_utf8();
                }
                '.' => {
                    if needs_dot_separator {
                        break;
                    }
                    return Err(self.error("unexpected special character '.'"));
                }
                '*' => {
                    is_literal = false;
                    pattern.push_str(".*");
                    idx += ch.len_utf8();
                }
                '?' => {
                    is_literal = false;
                    pattern.push('.');
                    idx += ch.len_utf8();
                }
                '[' => {
                    let slice = &line[idx..];
                    let range = WILDCARD_RANGE_RE.find(slice).ok_or_else(|| {
                        self.error("syntax error: failed to parse character class")
                    })?;
                    let end = idx + range.end();
                    let raw = &line[idx..end];
                    let bytes = raw.as_bytes();
                    is_literal = false;
                    if bytes.get(1) == Some(&b'!') {
                        pattern.push_str("[^");
                        pattern.push_str(&raw[2..]);
                    } else if bytes.get(1) == Some(&b'^') {
                        pattern.push_str("[\\^");
                        pattern.push_str(&raw[2..]);
                    } else {
                        pattern.push_str(raw);
                    }
                    idx = end;
                }
                _ => {
                    if ch.is_ascii() {
                        if ch == '$' || ch == '_' || ch.is_ascii_alphanumeric() {
                            if is_literal {
                                literal.push(ch);
                            }
                            pattern.push(ch);
                            idx += ch.len_utf8();
                        } else {
                            return Err(self.error(&format!(
                                "unexpected special character '{}'",
                                ch
                            )));
                        }
                    } else {
                        if is_literal {
                            literal.push(ch);
                        }
                        pattern.push(ch);
                        idx += ch.len_utf8();
                    }
                }
            }
        }

        let rest = &line[idx..];
        if is_literal {
            return Ok((Matcher::String(literal), rest));
        }
        pattern.push('$');
        let matcher = Matcher::new_regex(&pattern)
            .map_err(|err| self.annotate(err, "invalid pattern"))?;
        Ok((matcher, rest))
    }
}

static WILDCARD_RANGE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\[!?(?:\\[^0-9a-zA-Z]|[^\\\]])+\]").expect("valid wildcard range regex")
});

fn find_regex_end(line: &str) -> Option<usize> {
    let bytes = line.as_bytes();
    if bytes.first()? != &b'/' {
        return None;
    }
    let mut idx = 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\\' => {
                idx += 1;
                if idx >= bytes.len() {
                    return None;
                }
                idx += 1;
            }
            b'/' => return Some(idx + 1),
            _ => idx += 1,
        }
    }
    None
}
