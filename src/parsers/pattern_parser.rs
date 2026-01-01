use crate::models::*;
use crate::error::ParseError;
use crate::parse_result::ParseResult;
use crate::parsers::LogParser;
use chrono::{DateTime, Utc};
use regex::Regex;
use std::time::Instant;

/// Pattern parser for timestamp and level pattern matching
#[derive(Clone)]
pub struct PatternParser {
    bracketed_pattern: Regex,
    space_pattern: Regex,
    android_logcat_pattern: Regex,
    syslog_pattern: Regex,
    #[allow(dead_code)]
    iso8601_pattern: Regex,
    #[allow(dead_code)]
    rfc3339_pattern: Regex,
    #[allow(dead_code)]
    common_log_pattern: Regex,
}

impl PatternParser {
    pub fn new() -> Self {
        Self {
            bracketed_pattern: Regex::new(
                r"^\[([^\]]+)\]\s*\[([^\]]+)\]\s*(.*)$"
            ).unwrap(),
            space_pattern: Regex::new(
                r"^(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(\w+)\s+(.*)$"
            ).unwrap(),
            // Android logcat: "03-17 16:13:38.811  1702  2395 D WindowManager: message"
            android_logcat_pattern: Regex::new(
                r"^(\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)\s+(\d+)\s+(\d+)\s+([VDIWEFA])\s+([^:]+):\s*(.*)$"
            ).unwrap(),
            // Linux syslog: "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: message"
            syslog_pattern: Regex::new(
                r"^([A-Za-z]{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\S+)\s+([^:]+):\s*(.*)$"
            ).unwrap(),
            iso8601_pattern: Regex::new(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
            ).unwrap(),
            rfc3339_pattern: Regex::new(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
            ).unwrap(),
            common_log_pattern: Regex::new(
                r"^\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4}$"
            ).unwrap(),
        }
    }
    
    /// Parse timestamp from string using multiple format attempts
    fn parse_timestamp(&self, timestamp_str: &str) -> Result<DateTime<Utc>, ParseError> {
        let mut attempted_formats = Vec::new();
        
        // Try RFC3339 format first
        attempted_formats.push("RFC3339".to_string());
        if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
            return Ok(dt.with_timezone(&Utc));
        }
        
        // Try ISO8601 without timezone
        attempted_formats.push("ISO8601 without timezone".to_string());
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try ISO8601 with milliseconds
        attempted_formats.push("ISO8601 with milliseconds".to_string());
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.3f") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try space-separated format
        attempted_formats.push("Space-separated format".to_string());
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try common log format with timezone
        attempted_formats.push("Common log format".to_string());
        if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%d/%b/%Y:%H:%M:%S %z") {
            return Ok(dt.with_timezone(&Utc));
        }
        
        // Try Apache/Syslog style: "Sun Dec 04 04:47:44 2005"
        attempted_formats.push("Apache/Syslog format".to_string());
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%a %b %d %H:%M:%S %Y") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try variant without day name: "Dec 04 04:47:44 2005"
        attempted_formats.push("Syslog variant".to_string());
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%b %d %H:%M:%S %Y") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try syslog without year: "Dec  4 04:47:44" (assumes current year)
        attempted_formats.push("Syslog without year".to_string());
        let current_year = chrono::Utc::now().format("%Y").to_string();
        let with_year = format!("{} {}", timestamp_str, current_year);
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&with_year, "%b %d %H:%M:%S %Y") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        // Handle single-digit day with double space: "Dec  4"
        let normalized = timestamp_str.split_whitespace().collect::<Vec<_>>().join(" ");
        let with_year = format!("{} {}", normalized, current_year);
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&with_year, "%b %d %H:%M:%S %Y") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        // Try Android logcat format: "03-17 16:13:38.811" (assumes current year)
        attempted_formats.push("Android logcat format".to_string());
        let android_with_year = format!("{}-{}", current_year, timestamp_str);
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&android_with_year, "%Y-%m-%d %H:%M:%S%.3f") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        // Try without milliseconds
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&android_with_year, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
        }
        
        Err(ParseError::TimestampParseError {
            input: timestamp_str.to_string(),
            attempted_formats,
        })
    }
    
    /// Parse log level from string with normalization
    fn parse_level(&self, level_str: &str) -> Result<LogLevel, ParseError> {
        let normalized = level_str.to_uppercase();
        
        let valid_levels = vec![
            "ERROR".to_string(), "WARN".to_string(), "WARNING".to_string(),
            "INFO".to_string(), "DEBUG".to_string(), "TRACE".to_string(),
            "FATAL".to_string(), "CRITICAL".to_string(), "NOTICE".to_string(),
            "EMERG".to_string(), "ALERT".to_string(), "SEVERE".to_string(),
        ];
        
        match LogLevel::from_str(&normalized) {
            Some(level) => Ok(level),
            None => Err(ParseError::LevelParseError {
                input: level_str.to_string(),
                valid_levels,
            }),
        }
    }
    
    /// Try parsing with bracketed pattern: [timestamp] [level] message
    fn try_bracketed_pattern(&self, line: &str) -> Result<(DateTime<Utc>, LogLevel, String), ParseError> {
        if let Some(captures) = self.bracketed_pattern.captures(line) {
            let timestamp_str = captures.get(1).unwrap().as_str();
            let level_str = captures.get(2).unwrap().as_str();
            let message = captures.get(3).unwrap().as_str().to_string();
            
            let timestamp = self.parse_timestamp(timestamp_str)?;
            let level = self.parse_level(level_str)?;
            
            return Ok((timestamp, level, message));
        }
        
        Err(ParseError::PatternMatchError {
            input: line.to_string(),
            attempted_patterns: vec!["bracketed pattern".to_string()],
        })
    }
    
    /// Try parsing with space-separated pattern: timestamp level message
    fn try_space_pattern(&self, line: &str) -> Result<(DateTime<Utc>, LogLevel, String), ParseError> {
        if let Some(captures) = self.space_pattern.captures(line) {
            let timestamp_str = captures.get(1).unwrap().as_str();
            let level_str = captures.get(2).unwrap().as_str();
            let message = captures.get(3).unwrap().as_str().to_string();
            
            let timestamp = self.parse_timestamp(timestamp_str)?;
            let level = self.parse_level(level_str)?;
            
            return Ok((timestamp, level, message));
        }
        
        Err(ParseError::PatternMatchError {
            input: line.to_string(),
            attempted_patterns: vec!["space-separated pattern".to_string()],
        })
    }
    
    /// Try parsing Android logcat format: "03-17 16:13:38.811  1702  2395 D WindowManager: message"
    fn try_android_logcat_pattern(&self, line: &str) -> Result<(DateTime<Utc>, LogLevel, String, std::collections::HashMap<String, serde_json::Value>), ParseError> {
        if let Some(captures) = self.android_logcat_pattern.captures(line) {
            let timestamp_str = captures.get(1).unwrap().as_str();
            let pid = captures.get(2).unwrap().as_str();
            let tid = captures.get(3).unwrap().as_str();
            let level_char = captures.get(4).unwrap().as_str();
            let tag = captures.get(5).unwrap().as_str().trim();
            let message = captures.get(6).unwrap().as_str().to_string();
            
            let timestamp = self.parse_timestamp(timestamp_str)?;
            
            // Map single-letter Android log levels
            let level = match level_char {
                "V" => LogLevel::Trace,
                "D" => LogLevel::Debug,
                "I" => LogLevel::Info,
                "W" => LogLevel::Warn,
                "E" => LogLevel::Error,
                "F" | "A" => LogLevel::Fatal,
                _ => return Err(ParseError::LevelParseError {
                    input: level_char.to_string(),
                    valid_levels: vec!["V".to_string(), "D".to_string(), "I".to_string(), 
                                       "W".to_string(), "E".to_string(), "F".to_string()],
                }),
            };
            
            // Build fields map with Android-specific metadata
            let mut fields = std::collections::HashMap::new();
            fields.insert("pid".to_string(), serde_json::Value::Number(pid.parse::<i64>().unwrap_or(0).into()));
            fields.insert("tid".to_string(), serde_json::Value::Number(tid.parse::<i64>().unwrap_or(0).into()));
            fields.insert("tag".to_string(), serde_json::Value::String(tag.to_string()));
            
            return Ok((timestamp, level, message, fields));
        }
        
        Err(ParseError::PatternMatchError {
            input: line.to_string(),
            attempted_patterns: vec!["android logcat pattern".to_string()],
        })
    }
    
    /// Try parsing Linux syslog format: "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: message"
    fn try_syslog_pattern(&self, line: &str) -> Result<(DateTime<Utc>, String, std::collections::HashMap<String, serde_json::Value>), ParseError> {
        if let Some(captures) = self.syslog_pattern.captures(line) {
            let month = captures.get(1).unwrap().as_str();
            let day = captures.get(2).unwrap().as_str();
            let time = captures.get(3).unwrap().as_str();
            let hostname = captures.get(4).unwrap().as_str();
            let process = captures.get(5).unwrap().as_str();
            let message = captures.get(6).unwrap().as_str().to_string();
            
            // Build timestamp string for parsing (assume current year)
            let current_year = chrono::Utc::now().format("%Y").to_string();
            let timestamp_str = format!("{} {} {} {}", month, day, time, current_year);
            let timestamp = self.parse_timestamp(&timestamp_str)?;
            
            // Build fields map with syslog-specific metadata
            let mut fields = std::collections::HashMap::new();
            fields.insert("hostname".to_string(), serde_json::Value::String(hostname.to_string()));
            fields.insert("process".to_string(), serde_json::Value::String(process.to_string()));
            
            // Try to extract PID if present in process field (e.g., "sshd[1234]" or "sshd(pam_unix)[1234]")
            if let Some(pid_match) = regex::Regex::new(r"\[(\d+)\]").unwrap().captures(process) {
                if let Ok(pid) = pid_match.get(1).unwrap().as_str().parse::<i64>() {
                    fields.insert("pid".to_string(), serde_json::Value::Number(pid.into()));
                }
            }
            
            return Ok((timestamp, message, fields));
        }
        
        Err(ParseError::PatternMatchError {
            input: line.to_string(),
            attempted_patterns: vec!["syslog pattern".to_string()],
        })
    }
}

impl LogParser for PatternParser {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        let mut attempted_patterns = Vec::new();
        
        // Try Android logcat pattern first (most specific)
        match self.try_android_logcat_pattern(line) {
            Ok((timestamp, level, message, fields)) => {
                let mut event = CanonicalEvent::new(
                    message,
                    line.to_string(),
                    FormatType::Pattern,
                );
                event.set_timestamp(timestamp);
                event.set_level(level);
                for (key, value) in fields {
                    event.add_field(key, value);
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                return ParseResult::success_with_timing(event, 0.90, processing_time);
            }
            Err(_e) => {
                attempted_patterns.push("android logcat pattern".to_string());
                // Continue to next pattern
            }
        }
        
        // Try bracketed pattern
        match self.try_bracketed_pattern(line) {
            Ok((timestamp, level, message)) => {
                let mut event = CanonicalEvent::new(
                    message,
                    line.to_string(),
                    FormatType::Pattern,
                );
                event.set_timestamp(timestamp);
                event.set_level(level);
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                return ParseResult::success_with_timing(event, 0.85, processing_time);
            }
            Err(_e) => {
                attempted_patterns.push("bracketed pattern".to_string());
                // Continue to next pattern
            }
        }
        
        // Try space-separated pattern
        match self.try_space_pattern(line) {
            Ok((timestamp, level, message)) => {
                let mut event = CanonicalEvent::new(
                    message,
                    line.to_string(),
                    FormatType::Pattern,
                );
                event.set_timestamp(timestamp);
                event.set_level(level);
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                return ParseResult::success_with_timing(event, 0.80, processing_time);
            }
            Err(_e) => {
                attempted_patterns.push("space-separated pattern".to_string());
                // Continue to next pattern
            }
        }
        
        // Try syslog pattern (no log level in standard syslog)
        match self.try_syslog_pattern(line) {
            Ok((timestamp, message, fields)) => {
                let mut event = CanonicalEvent::new(
                    message,
                    line.to_string(),
                    FormatType::Pattern,
                );
                event.set_timestamp(timestamp);
                // Syslog doesn't have explicit log levels - leave as None
                for (key, value) in fields {
                    event.add_field(key, value);
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                return ParseResult::success_with_timing(event, 0.75, processing_time);
            }
            Err(_e) => {
                attempted_patterns.push("syslog pattern".to_string());
                // Continue to failure
            }
        }
        
        // No patterns matched
        let error = ParseError::PatternMatchError {
            input: line.to_string(),
            attempted_patterns,
        };
        
        let processing_time = start_time.elapsed().as_micros() as u64;
        ParseResult::failure_with_context(
            line.to_string(),
            error,
            None,
            Some(processing_time),
        )
    }
    
    fn can_parse(&self, line: &str) -> bool {
        // Quick heuristic checks
        self.android_logcat_pattern.is_match(line) || 
        self.bracketed_pattern.is_match(line) || 
        self.space_pattern.is_match(line) ||
        self.syslog_pattern.is_match(line)
    }
    
    fn get_format_type(&self) -> FormatType {
        FormatType::Pattern
    }
}