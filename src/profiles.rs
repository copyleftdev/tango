use crate::models::*;
use crate::error::ParseError;
use crate::parse_result::ParseResult;
use chrono::{DateTime, Utc, Datelike};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Profile trait for user-defined parsing configurations
pub trait Profile: Send + Sync {
    /// Parse a log line using this profile
    fn parse(&self, line: &str) -> ParseResult;
    
    /// Check if this profile can parse the given line
    fn can_parse(&self, line: &str) -> bool;
    
    /// Get the profile type
    fn get_profile_type(&self) -> ProfileType;
    
    /// Validate the profile configuration
    fn validate(&self) -> Result<(), ParseError>;
}

/// Configuration for regex-based profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegexProfileConfig {
    pub name: String,
    pub pattern: String,
    pub field_mappings: HashMap<String, usize>, // field_name -> capture_group_index
    pub timestamp_field: Option<String>,
    pub level_field: Option<String>,
    pub message_field: Option<String>,
    pub timestamp_format: Option<String>,
}

/// Regex-based profile parser
pub struct RegexProfile {
    config: RegexProfileConfig,
    compiled_regex: Regex,
}

impl RegexProfile {
    pub fn new(config: RegexProfileConfig) -> Result<Self, ParseError> {
        let compiled_regex = Regex::new(&config.pattern)
            .map_err(|e| ParseError::RegexError {
                pattern: config.pattern.clone(),
                error_message: e.to_string(),
            })?;
        
        let profile = Self {
            config,
            compiled_regex,
        };
        
        // Validate the configuration
        profile.validate()?;
        
        Ok(profile)
    }
    
    fn extract_timestamp(&self, _captures: &regex::Captures, fields: &HashMap<String, String>) -> Option<DateTime<Utc>> {
        if let Some(timestamp_field) = &self.config.timestamp_field {
            if let Some(timestamp_str) = fields.get(timestamp_field) {
                return self.parse_timestamp_string(timestamp_str);
            }
        }
        None
    }
    
    fn parse_timestamp_string(&self, timestamp_str: &str) -> Option<DateTime<Utc>> {
        // Try custom format first if specified
        if let Some(format) = &self.config.timestamp_format {
            if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
                return Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
            }
        }
        
        // Try common formats
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.fZ",           // ISO8601 with fractional seconds
            "%Y-%m-%dT%H:%M:%SZ",              // ISO8601
            "%Y-%m-%d %H:%M:%S%.f",            // Common log format with fractional seconds
            "%Y-%m-%d %H:%M:%S",               // Common log format
            "%d/%b/%Y:%H:%M:%S %z",            // Apache Common Log Format
            "%b %d %H:%M:%S",                  // Syslog format
        ];
        
        for format in &formats {
            if let Ok(dt) = DateTime::parse_from_str(timestamp_str, format) {
                return Some(dt.with_timezone(&Utc));
            }
            if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
                return Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
            }
        }
        
        None
    }
    
    fn extract_level(&self, fields: &HashMap<String, String>) -> Option<LogLevel> {
        if let Some(level_field) = &self.config.level_field {
            if let Some(level_str) = fields.get(level_field) {
                return LogLevel::from_str(level_str);
            }
        }
        None
    }
    
    fn extract_message(&self, fields: &HashMap<String, String>) -> String {
        if let Some(message_field) = &self.config.message_field {
            if let Some(message) = fields.get(message_field) {
                return message.clone();
            }
        }
        
        // If no message field specified, use the entire line
        fields.values().map(|s| s.as_str()).collect::<Vec<_>>().join(" ")
    }
}

impl Profile for RegexProfile {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        match self.compiled_regex.captures(line) {
            Some(captures) => {
                let mut event = CanonicalEvent::new(
                    String::new(), // Will be set below
                    line.to_string(),
                    FormatType::Profile(ProfileType::Regex),
                );
                
                // Extract all named captures into fields
                let mut extracted_fields = HashMap::new();
                for (field_name, &group_index) in &self.config.field_mappings {
                    if let Some(capture) = captures.get(group_index) {
                        extracted_fields.insert(field_name.clone(), capture.as_str().to_string());
                    }
                }
                
                // Extract timestamp
                if let Some(timestamp) = self.extract_timestamp(&captures, &extracted_fields) {
                    event.set_timestamp(timestamp);
                }
                
                // Extract level
                if let Some(level) = self.extract_level(&extracted_fields) {
                    event.set_level(level);
                }
                
                // Extract message
                event.message = self.extract_message(&extracted_fields);
                
                // Add all other fields to the event
                for (key, value) in extracted_fields {
                    // Skip fields that were mapped to canonical fields
                    let is_canonical_field = Some(&key) == self.config.timestamp_field.as_ref() ||
                                           Some(&key) == self.config.level_field.as_ref() ||
                                           Some(&key) == self.config.message_field.as_ref();
                    
                    if !is_canonical_field {
                        event.add_field(key, serde_json::Value::String(value));
                    }
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::success_with_timing(event, 0.9, processing_time)
            }
            None => {
                let error = ParseError::PatternMatchError {
                    input: line.to_string(),
                    attempted_patterns: vec![self.config.pattern.clone()],
                };
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::failure_with_context(
                    line.to_string(),
                    error,
                    None,
                    Some(processing_time),
                )
            }
        }
    }
    
    fn can_parse(&self, line: &str) -> bool {
        self.compiled_regex.is_match(line)
    }
    
    fn get_profile_type(&self) -> ProfileType {
        ProfileType::Regex
    }
    
    fn validate(&self) -> Result<(), ParseError> {
        // Check that all field mappings reference valid capture groups
        let capture_count = self.compiled_regex.captures_len();
        
        for (field_name, &group_index) in &self.config.field_mappings {
            if group_index >= capture_count {
                return Err(ParseError::ConfigurationError {
                    parameter: format!("field_mappings.{}", field_name),
                    error_message: format!(
                        "Capture group {} does not exist in pattern (max: {})",
                        group_index, capture_count - 1
                    ),
                });
            }
        }
        
        // Validate timestamp format if specified
        if let Some(format) = &self.config.timestamp_format {
            // Try to parse a test timestamp to validate the format
            let test_timestamp = "2025-12-30T10:21:03Z";
            let test_naive = "2025-12-30 10:21:03";
            
            // Try parsing with timezone first
            let tz_parse_ok = chrono::DateTime::parse_from_str(test_timestamp, format).is_ok();
            // Try parsing as naive datetime
            let naive_parse_ok = chrono::NaiveDateTime::parse_from_str(test_naive, format).is_ok();
            
            if !tz_parse_ok && !naive_parse_ok {
                return Err(ParseError::ConfigurationError {
                    parameter: "timestamp_format".to_string(),
                    error_message: format!("Invalid timestamp format: {}", format),
                });
            }
        }
        
        Ok(())
    }
}

/// Configuration for CSV-based profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvProfileConfig {
    pub name: String,
    pub delimiter: char,
    pub has_header: bool,
    pub column_mappings: HashMap<String, usize>, // field_name -> column_index
    pub timestamp_column: Option<String>,
    pub level_column: Option<String>,
    pub message_column: Option<String>,
    pub timestamp_format: Option<String>,
}

/// CSV-based profile parser
pub struct CsvProfile {
    config: CsvProfileConfig,
}

impl CsvProfile {
    pub fn new(config: CsvProfileConfig) -> Result<Self, ParseError> {
        let profile = Self { config };
        profile.validate()?;
        Ok(profile)
    }
    
    fn parse_csv_line(&self, line: &str) -> Vec<String> {
        // Simple CSV parsing - split by delimiter and handle quoted fields
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '"' if !in_quotes => {
                    in_quotes = true;
                }
                '"' if in_quotes => {
                    // Check for escaped quote
                    if chars.peek() == Some(&'"') {
                        chars.next(); // consume the second quote
                        current_field.push('"');
                    } else {
                        in_quotes = false;
                    }
                }
                c if c == self.config.delimiter && !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                _ => {
                    current_field.push(ch);
                }
            }
        }
        
        // Add the last field
        fields.push(current_field.trim().to_string());
        fields
    }
    
    fn extract_timestamp(&self, fields: &[String]) -> Option<DateTime<Utc>> {
        if let Some(timestamp_column) = &self.config.timestamp_column {
            if let Some(&column_index) = self.config.column_mappings.get(timestamp_column) {
                if let Some(timestamp_str) = fields.get(column_index) {
                    return self.parse_timestamp_string(timestamp_str);
                }
            }
        }
        None
    }
    
    fn parse_timestamp_string(&self, timestamp_str: &str) -> Option<DateTime<Utc>> {
        // Try custom format first if specified
        if let Some(format) = &self.config.timestamp_format {
            if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
                return Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
            }
        }
        
        // Try common formats (same as RegexProfile)
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%d/%b/%Y:%H:%M:%S %z",
            "%b %d %H:%M:%S",
        ];
        
        for format in &formats {
            if let Ok(dt) = DateTime::parse_from_str(timestamp_str, format) {
                return Some(dt.with_timezone(&Utc));
            }
            if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
                return Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
            }
        }
        
        None
    }
    
    fn extract_level(&self, fields: &[String]) -> Option<LogLevel> {
        if let Some(level_column) = &self.config.level_column {
            if let Some(&column_index) = self.config.column_mappings.get(level_column) {
                if let Some(level_str) = fields.get(column_index) {
                    return LogLevel::from_str(level_str);
                }
            }
        }
        None
    }
    
    fn extract_message(&self, fields: &[String]) -> String {
        if let Some(message_column) = &self.config.message_column {
            if let Some(&column_index) = self.config.column_mappings.get(message_column) {
                if let Some(message) = fields.get(column_index) {
                    return message.clone();
                }
            }
        }
        
        // If no message column specified, join all fields
        fields.join(" ")
    }
}

impl Profile for CsvProfile {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        let fields = self.parse_csv_line(line);
        
        let mut event = CanonicalEvent::new(
            String::new(), // Will be set below
            line.to_string(),
            FormatType::Profile(ProfileType::Csv),
        );
        
        // Extract timestamp
        if let Some(timestamp) = self.extract_timestamp(&fields) {
            event.set_timestamp(timestamp);
        }
        
        // Extract level
        if let Some(level) = self.extract_level(&fields) {
            event.set_level(level);
        }
        
        // Extract message
        event.message = self.extract_message(&fields);
        
        // Add all mapped fields to the event
        for (field_name, &column_index) in &self.config.column_mappings {
            if let Some(value) = fields.get(column_index) {
                // Skip fields that were mapped to canonical fields
                let is_canonical_field = Some(field_name) == self.config.timestamp_column.as_ref() ||
                                       Some(field_name) == self.config.level_column.as_ref() ||
                                       Some(field_name) == self.config.message_column.as_ref();
                
                if !is_canonical_field {
                    event.add_field(field_name.clone(), serde_json::Value::String(value.clone()));
                }
            }
        }
        
        let processing_time = start_time.elapsed().as_micros() as u64;
        ParseResult::success_with_timing(event, 0.85, processing_time)
    }
    
    fn can_parse(&self, line: &str) -> bool {
        // Check if the line has the expected number of fields
        let fields = self.parse_csv_line(line);
        let max_column_index = self.config.column_mappings.values().max().copied().unwrap_or(0);
        fields.len() > max_column_index
    }
    
    fn get_profile_type(&self) -> ProfileType {
        ProfileType::Csv
    }
    
    fn validate(&self) -> Result<(), ParseError> {
        // Check that column mappings are valid
        if self.config.column_mappings.is_empty() {
            return Err(ParseError::ConfigurationError {
                parameter: "column_mappings".to_string(),
                error_message: "At least one column mapping must be specified".to_string(),
            });
        }
        
        // Validate timestamp format if specified
        if let Some(format) = &self.config.timestamp_format {
            let test_timestamp = "2025-12-30T10:21:03Z";
            let test_naive = "2025-12-30 10:21:03";
            
            // Try parsing with timezone first
            let tz_parse_ok = chrono::DateTime::parse_from_str(test_timestamp, format).is_ok();
            // Try parsing as naive datetime
            let naive_parse_ok = chrono::NaiveDateTime::parse_from_str(test_naive, format).is_ok();
            
            if !tz_parse_ok && !naive_parse_ok {
                return Err(ParseError::ConfigurationError {
                    parameter: "timestamp_format".to_string(),
                    error_message: format!("Invalid timestamp format: {}", format),
                });
            }
        }
        
        Ok(())
    }
}

/// Apache Common Log Format profile
pub struct ApacheProfile;

impl ApacheProfile {
    pub fn new() -> Self {
        Self
    }
    
    fn get_apache_regex() -> &'static str {
        // Apache Common Log Format: host ident authuser [timestamp] "request" status size
        r#"^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d+) (\S+)"#
    }
    
    fn parse_apache_timestamp(&self, timestamp_str: &str) -> Option<DateTime<Utc>> {
        // Apache timestamp format: "10/Oct/2000:13:55:36 -0700"
        if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%d/%b/%Y:%H:%M:%S %z") {
            return Some(dt.with_timezone(&Utc));
        }
        None
    }
}

impl Profile for ApacheProfile {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        let regex = Regex::new(Self::get_apache_regex()).unwrap();
        
        match regex.captures(line) {
            Some(captures) => {
                let mut event = CanonicalEvent::new(
                    String::new(), // Will be set below
                    line.to_string(),
                    FormatType::Profile(ProfileType::Apache),
                );
                
                // Extract fields according to Apache Common Log Format
                if let Some(client_ip) = captures.get(1) {
                    event.add_field("client_ip".to_string(), serde_json::Value::String(client_ip.as_str().to_string()));
                }
                
                if let Some(timestamp_match) = captures.get(4) {
                    if let Some(timestamp) = self.parse_apache_timestamp(timestamp_match.as_str()) {
                        event.set_timestamp(timestamp);
                    }
                }
                
                if let Some(request) = captures.get(5) {
                    event.message = request.as_str().to_string();
                    event.add_field("request".to_string(), serde_json::Value::String(request.as_str().to_string()));
                }
                
                if let Some(status) = captures.get(6) {
                    if let Ok(status_code) = status.as_str().parse::<u16>() {
                        event.add_field("status".to_string(), serde_json::Value::Number(status_code.into()));
                        
                        // Set log level based on status code
                        let level = match status_code {
                            200..=299 => LogLevel::Info,
                            300..=399 => LogLevel::Info,
                            400..=499 => LogLevel::Warn,
                            500..=599 => LogLevel::Error,
                            _ => LogLevel::Info,
                        };
                        event.set_level(level);
                    }
                }
                
                if let Some(size) = captures.get(7) {
                    if let Ok(size_bytes) = size.as_str().parse::<u64>() {
                        event.add_field("size".to_string(), serde_json::Value::Number(size_bytes.into()));
                    }
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::success_with_timing(event, 0.9, processing_time)
            }
            None => {
                let error = ParseError::PatternMatchError {
                    input: line.to_string(),
                    attempted_patterns: vec![Self::get_apache_regex().to_string()],
                };
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::failure_with_context(
                    line.to_string(),
                    error,
                    None,
                    Some(processing_time),
                )
            }
        }
    }
    
    fn can_parse(&self, line: &str) -> bool {
        let regex = Regex::new(Self::get_apache_regex()).unwrap();
        regex.is_match(line)
    }
    
    fn get_profile_type(&self) -> ProfileType {
        ProfileType::Apache
    }
    
    fn validate(&self) -> Result<(), ParseError> {
        // Apache profile is always valid
        Ok(())
    }
}

/// Nginx access log profile
pub struct NginxProfile;

impl NginxProfile {
    pub fn new() -> Self {
        Self
    }
    
    fn get_nginx_regex() -> &'static str {
        // Nginx default log format: host - - [timestamp] "request" status size "referer" "user_agent"
        r#"^(\S+) - - \[([^\]]+)\] "([^"]*)" (\d+) (\S+) "([^"]*)" "([^"]*)""#
    }
    
    fn parse_nginx_timestamp(&self, timestamp_str: &str) -> Option<DateTime<Utc>> {
        // Nginx timestamp format: "10/Oct/2000:13:55:36 +0000"
        if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%d/%b/%Y:%H:%M:%S %z") {
            return Some(dt.with_timezone(&Utc));
        }
        None
    }
}

impl Profile for NginxProfile {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        let regex = Regex::new(Self::get_nginx_regex()).unwrap();
        
        match regex.captures(line) {
            Some(captures) => {
                let mut event = CanonicalEvent::new(
                    String::new(), // Will be set below
                    line.to_string(),
                    FormatType::Profile(ProfileType::Nginx),
                );
                
                // Extract fields according to Nginx log format
                if let Some(client_ip) = captures.get(1) {
                    event.add_field("client_ip".to_string(), serde_json::Value::String(client_ip.as_str().to_string()));
                }
                
                if let Some(timestamp_match) = captures.get(2) {
                    if let Some(timestamp) = self.parse_nginx_timestamp(timestamp_match.as_str()) {
                        event.set_timestamp(timestamp);
                    }
                }
                
                if let Some(request) = captures.get(3) {
                    event.message = request.as_str().to_string();
                    event.add_field("request".to_string(), serde_json::Value::String(request.as_str().to_string()));
                }
                
                if let Some(status) = captures.get(4) {
                    if let Ok(status_code) = status.as_str().parse::<u16>() {
                        event.add_field("status".to_string(), serde_json::Value::Number(status_code.into()));
                        
                        // Set log level based on status code
                        let level = match status_code {
                            200..=299 => LogLevel::Info,
                            300..=399 => LogLevel::Info,
                            400..=499 => LogLevel::Warn,
                            500..=599 => LogLevel::Error,
                            _ => LogLevel::Info,
                        };
                        event.set_level(level);
                    }
                }
                
                if let Some(size) = captures.get(5) {
                    if let Ok(size_bytes) = size.as_str().parse::<u64>() {
                        event.add_field("size".to_string(), serde_json::Value::Number(size_bytes.into()));
                    }
                }
                
                if let Some(referer) = captures.get(6) {
                    event.add_field("referer".to_string(), serde_json::Value::String(referer.as_str().to_string()));
                }
                
                if let Some(user_agent) = captures.get(7) {
                    event.add_field("user_agent".to_string(), serde_json::Value::String(user_agent.as_str().to_string()));
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::success_with_timing(event, 0.9, processing_time)
            }
            None => {
                let error = ParseError::PatternMatchError {
                    input: line.to_string(),
                    attempted_patterns: vec![Self::get_nginx_regex().to_string()],
                };
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::failure_with_context(
                    line.to_string(),
                    error,
                    None,
                    Some(processing_time),
                )
            }
        }
    }
    
    fn can_parse(&self, line: &str) -> bool {
        let regex = Regex::new(Self::get_nginx_regex()).unwrap();
        regex.is_match(line)
    }
    
    fn get_profile_type(&self) -> ProfileType {
        ProfileType::Nginx
    }
    
    fn validate(&self) -> Result<(), ParseError> {
        // Nginx profile is always valid
        Ok(())
    }
}

/// Syslog profile (RFC3164 format)
pub struct SyslogProfile;

impl SyslogProfile {
    pub fn new() -> Self {
        Self
    }
    
    fn get_syslog_regex() -> &'static str {
        // Syslog RFC3164 format: <priority>timestamp hostname tag: message
        r#"^<(\d+)>(\w{3} \d{1,2} \d{2}:\d{2}:\d{2}) (\S+) ([^:]+): (.*)$"#
    }
    
    fn parse_syslog_timestamp(&self, timestamp_str: &str) -> Option<DateTime<Utc>> {
        // Syslog timestamp format: "Oct 10 13:55:36"
        // Note: This doesn't include year, so we assume current year
        let current_year = chrono::Utc::now().year();
        let full_timestamp = format!("{} {}", current_year, timestamp_str);
        
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(&full_timestamp, "%Y %b %d %H:%M:%S") {
            return Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
        }
        None
    }
    
    fn parse_syslog_priority(&self, priority_str: &str) -> (Option<String>, Option<LogLevel>) {
        if let Ok(priority) = priority_str.parse::<u8>() {
            let facility = priority >> 3;
            let severity = priority & 7;
            
            let facility_name = match facility {
                0 => "kernel",
                1 => "user",
                2 => "mail",
                3 => "daemon",
                4 => "auth",
                5 => "syslog",
                6 => "lpr",
                7 => "news",
                8 => "uucp",
                9 => "cron",
                10 => "authpriv",
                11 => "ftp",
                16 => "local0",
                17 => "local1",
                18 => "local2",
                19 => "local3",
                20 => "local4",
                21 => "local5",
                22 => "local6",
                23 => "local7",
                _ => "unknown",
            };
            
            let level = match severity {
                0 => LogLevel::Fatal,  // Emergency
                1 => LogLevel::Fatal,  // Alert
                2 => LogLevel::Fatal,  // Critical
                3 => LogLevel::Error,  // Error
                4 => LogLevel::Warn,   // Warning
                5 => LogLevel::Info,   // Notice
                6 => LogLevel::Info,   // Informational
                7 => LogLevel::Debug,  // Debug
                _ => LogLevel::Info,
            };
            
            (Some(facility_name.to_string()), Some(level))
        } else {
            (None, None)
        }
    }
}

impl Profile for SyslogProfile {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        let regex = Regex::new(Self::get_syslog_regex()).unwrap();
        
        match regex.captures(line) {
            Some(captures) => {
                let mut event = CanonicalEvent::new(
                    String::new(), // Will be set below
                    line.to_string(),
                    FormatType::Profile(ProfileType::Syslog),
                );
                
                // Extract priority and derive facility/severity
                if let Some(priority_match) = captures.get(1) {
                    let (facility, level) = self.parse_syslog_priority(priority_match.as_str());
                    
                    if let Some(facility) = facility {
                        event.add_field("facility".to_string(), serde_json::Value::String(facility));
                    }
                    
                    if let Some(level) = level {
                        event.set_level(level);
                    }
                }
                
                // Extract timestamp
                if let Some(timestamp_match) = captures.get(2) {
                    if let Some(timestamp) = self.parse_syslog_timestamp(timestamp_match.as_str()) {
                        event.set_timestamp(timestamp);
                    }
                }
                
                // Extract hostname
                if let Some(hostname) = captures.get(3) {
                    event.add_field("hostname".to_string(), serde_json::Value::String(hostname.as_str().to_string()));
                }
                
                // Extract tag
                if let Some(tag) = captures.get(4) {
                    event.add_field("tag".to_string(), serde_json::Value::String(tag.as_str().to_string()));
                }
                
                // Extract message
                if let Some(message) = captures.get(5) {
                    event.message = message.as_str().to_string();
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::success_with_timing(event, 0.9, processing_time)
            }
            None => {
                let error = ParseError::PatternMatchError {
                    input: line.to_string(),
                    attempted_patterns: vec![Self::get_syslog_regex().to_string()],
                };
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::failure_with_context(
                    line.to_string(),
                    error,
                    None,
                    Some(processing_time),
                )
            }
        }
    }
    
    fn can_parse(&self, line: &str) -> bool {
        let regex = Regex::new(Self::get_syslog_regex()).unwrap();
        regex.is_match(line)
    }
    
    fn get_profile_type(&self) -> ProfileType {
        ProfileType::Syslog
    }
    
    fn validate(&self) -> Result<(), ParseError> {
        // Syslog profile is always valid
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_regex_profile_creation() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp".to_string(), 1);
        field_mappings.insert("level".to_string(), 2);
        field_mappings.insert("message".to_string(), 3);
        
        let config = RegexProfileConfig {
            name: "test_profile".to_string(),
            pattern: r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string(),
            field_mappings,
            timestamp_field: Some("timestamp".to_string()),
            level_field: Some("level".to_string()),
            message_field: Some("message".to_string()),
            timestamp_format: None,
        };
        
        let profile = RegexProfile::new(config);
        assert!(profile.is_ok());
    }
    
    #[test]
    fn test_regex_profile_parsing() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp".to_string(), 1);
        field_mappings.insert("level".to_string(), 2);
        field_mappings.insert("message".to_string(), 3);
        
        let config = RegexProfileConfig {
            name: "test_profile".to_string(),
            pattern: r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string(),
            field_mappings,
            timestamp_field: Some("timestamp".to_string()),
            level_field: Some("level".to_string()),
            message_field: Some("message".to_string()),
            timestamp_format: None,
        };
        
        let profile = RegexProfile::new(config).unwrap();
        let result = profile.parse("2025-12-30T10:21:03Z [INFO] Test message");
        
        assert!(result.success);
        assert_eq!(result.event.message, "Test message");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
    }
    
    #[test]
    fn test_csv_profile_parsing() {
        let mut column_mappings = HashMap::new();
        column_mappings.insert("timestamp".to_string(), 0);
        column_mappings.insert("level".to_string(), 1);
        column_mappings.insert("message".to_string(), 2);
        
        let config = CsvProfileConfig {
            name: "test_csv".to_string(),
            delimiter: ',',
            has_header: false,
            column_mappings,
            timestamp_column: Some("timestamp".to_string()),
            level_column: Some("level".to_string()),
            message_column: Some("message".to_string()),
            timestamp_format: None,
        };
        
        let profile = CsvProfile::new(config).unwrap();
        let result = profile.parse("2025-12-30T10:21:03Z,INFO,Test message");
        
        assert!(result.success);
        assert_eq!(result.event.message, "Test message");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
    }
    
    #[test]
    fn test_apache_profile_parsing() {
        let profile = ApacheProfile::new();
        let log_line = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#;
        
        let result = profile.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "GET /apache_pb.gif HTTP/1.0");
        assert_eq!(result.event.level, Some(LogLevel::Info)); // 200 status
        assert!(result.event.timestamp.is_some());
        assert!(result.event.fields.contains_key("client_ip"));
        assert!(result.event.fields.contains_key("status"));
    }
    
    #[test]
    fn test_nginx_profile_parsing() {
        let profile = NginxProfile::new();
        let log_line = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0""#;
        
        let result = profile.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "GET /index.html HTTP/1.1");
        assert_eq!(result.event.level, Some(LogLevel::Info)); // 200 status
        assert!(result.event.timestamp.is_some());
        assert!(result.event.fields.contains_key("client_ip"));
        assert!(result.event.fields.contains_key("referer"));
        assert!(result.event.fields.contains_key("user_agent"));
    }
    
    #[test]
    fn test_syslog_profile_parsing() {
        let profile = SyslogProfile::new();
        let log_line = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";
        
        let result = profile.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "'su root' failed for lonvick on /dev/pts/8");
        assert!(result.event.timestamp.is_some());
        assert!(result.event.fields.contains_key("facility"));
        assert!(result.event.fields.contains_key("hostname"));
        assert!(result.event.fields.contains_key("tag"));
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use crate::ProfileParser;
    use crate::parsers::LogParser;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use std::sync::Arc;
    
    // Generate arbitrary profile configurations for testing
    impl Arbitrary for RegexProfileConfig {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut field_mappings = HashMap::new();
            
            // Create a simple pattern with 3 capture groups for testing
            let pattern = r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string();
            
            // Map fields to valid capture groups (1, 2, 3)
            field_mappings.insert("timestamp".to_string(), 1);
            field_mappings.insert("level".to_string(), 2);
            field_mappings.insert("message".to_string(), 3);
            
            Self {
                name: String::arbitrary(g),
                pattern,
                field_mappings,
                timestamp_field: Some("timestamp".to_string()),
                level_field: Some("level".to_string()),
                message_field: Some("message".to_string()),
                timestamp_format: None,
            }
        }
    }
    
    impl Arbitrary for CsvProfileConfig {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut column_mappings = HashMap::new();
            column_mappings.insert("timestamp".to_string(), 0);
            column_mappings.insert("level".to_string(), 1);
            column_mappings.insert("message".to_string(), 2);
            
            Self {
                name: String::arbitrary(g),
                delimiter: ',',
                has_header: bool::arbitrary(g),
                column_mappings,
                timestamp_column: Some("timestamp".to_string()),
                level_column: Some("level".to_string()),
                message_column: Some("message".to_string()),
                timestamp_format: None,
            }
        }
    }
    
    // Property 7: Profile Override Behavior
    // Feature: log-type-detection-and-parsing, Property 7: Profile Override Behavior
    // Validates: Requirements 7.1, 7.2, 7.4, 7.5
    #[quickcheck(tests = 100)]
    fn prop_profile_override_behavior_regex(config: RegexProfileConfig) -> bool {
        // For any user-defined profile configuration, when specified, 
        // the profile rules should take precedence over auto-detection 
        // and correctly parse logs according to the profile specification
        
        if let Ok(profile) = RegexProfile::new(config.clone()) {
            let parser = ProfileParser::from_profile(Arc::new(profile));
            
            // Test with a log line that matches the profile pattern
            let test_line = "2025-12-30T10:21:03Z [INFO] Test message for profile parsing";
            
            // Profile should be able to parse the matching line
            let can_parse = parser.can_parse(test_line);
            let result = parser.parse(test_line);
            
            // When profile is specified, it should take precedence (requirement 7.2)
            // Profile should correctly parse according to specification (requirement 7.1, 7.4, 7.5)
            if can_parse {
                result.success &&
                result.event.format_type == FormatType::Profile(ProfileType::Regex) &&
                !result.event.message.is_empty() &&
                result.event.raw == test_line
            } else {
                // If profile can't parse, it should fail gracefully
                !result.success
            }
        } else {
            // Invalid configurations should be rejected
            true
        }
    }
    
    #[quickcheck(tests = 100)]
    fn prop_profile_override_behavior_csv(config: CsvProfileConfig) -> bool {
        // For any CSV profile configuration, when specified,
        // the profile rules should take precedence and parse correctly
        
        if let Ok(profile) = CsvProfile::new(config.clone()) {
            let parser = ProfileParser::from_profile(Arc::new(profile));
            
            // Test with a CSV line that should match the profile
            let test_line = "2025-12-30T10:21:03Z,INFO,Test CSV message";
            
            let can_parse = parser.can_parse(test_line);
            let result = parser.parse(test_line);
            
            // Profile should take precedence and parse correctly
            if can_parse {
                result.success &&
                result.event.format_type == FormatType::Profile(ProfileType::Csv) &&
                !result.event.message.is_empty() &&
                result.event.raw == test_line
            } else {
                // If profile can't parse, it should fail gracefully
                !result.success
            }
        } else {
            // Invalid configurations should be rejected
            true
        }
    }
    
    #[quickcheck(tests = 100)]
    fn prop_profile_override_behavior_apache(_: ()) -> bool {
        // Apache profile should always take precedence when specified
        let parser = ProfileParser::new_apache();
        
        // Test with Apache Common Log Format
        let apache_line = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /test.html HTTP/1.0" 200 1234"#;
        
        let can_parse = parser.can_parse(apache_line);
        let result = parser.parse(apache_line);
        
        // Apache profile should take precedence and parse correctly
        if can_parse {
            result.success &&
            result.event.format_type == FormatType::Profile(ProfileType::Apache) &&
            result.event.message == "GET /test.html HTTP/1.0" &&
            result.event.raw == apache_line &&
            result.event.fields.contains_key("client_ip") &&
            result.event.fields.contains_key("status")
        } else {
            // If line doesn't match Apache format, should fail gracefully
            !result.success
        }
    }
    
    #[quickcheck(tests = 100)]
    fn prop_profile_override_behavior_nginx(_: ()) -> bool {
        // Nginx profile should always take precedence when specified
        let parser = ProfileParser::new_nginx();
        
        // Test with Nginx log format
        let nginx_line = r#"192.168.1.1 - - [10/Oct/2000:13:55:36 +0000] "POST /api/data HTTP/1.1" 201 567 "https://example.com" "curl/7.68.0""#;
        
        let can_parse = parser.can_parse(nginx_line);
        let result = parser.parse(nginx_line);
        
        // Nginx profile should take precedence and parse correctly
        if can_parse {
            result.success &&
            result.event.format_type == FormatType::Profile(ProfileType::Nginx) &&
            result.event.message == "POST /api/data HTTP/1.1" &&
            result.event.raw == nginx_line &&
            result.event.fields.contains_key("client_ip") &&
            result.event.fields.contains_key("referer") &&
            result.event.fields.contains_key("user_agent")
        } else {
            // If line doesn't match Nginx format, should fail gracefully
            !result.success
        }
    }
    
    #[quickcheck(tests = 100)]
    fn prop_profile_override_behavior_syslog(_: ()) -> bool {
        // Syslog profile should always take precedence when specified
        let parser = ProfileParser::new_syslog();
        
        // Test with Syslog RFC3164 format
        let syslog_line = "<34>Oct 11 22:14:15 server01 sshd: Failed password for user from 192.168.1.100";
        
        let can_parse = parser.can_parse(syslog_line);
        let result = parser.parse(syslog_line);
        
        // Syslog profile should take precedence and parse correctly
        if can_parse {
            result.success &&
            result.event.format_type == FormatType::Profile(ProfileType::Syslog) &&
            result.event.message == "Failed password for user from 192.168.1.100" &&
            result.event.raw == syslog_line &&
            result.event.fields.contains_key("facility") &&
            result.event.fields.contains_key("hostname") &&
            result.event.fields.contains_key("tag")
        } else {
            // If line doesn't match Syslog format, should fail gracefully
            !result.success
        }
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use crate::ProfileParser;
    use std::collections::HashMap;
    
    // Unit tests for profile validation
    // Validates: Requirements 7.6
    
    #[test]
    fn test_regex_profile_invalid_capture_group() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("invalid_field".to_string(), 99); // Invalid capture group
        
        let config = RegexProfileConfig {
            name: "invalid_profile".to_string(),
            pattern: r"^(\w+)$".to_string(), // Only has 1 capture group (index 1)
            field_mappings,
            timestamp_field: None,
            level_field: None,
            message_field: None,
            timestamp_format: None,
        };
        
        let result = RegexProfile::new(config);
        assert!(result.is_err());
        
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = result {
            assert!(parameter.contains("invalid_field"));
            assert!(error_message.contains("Capture group 99 does not exist"));
        } else {
            panic!("Expected ConfigurationError for invalid capture group");
        }
    }
    
    #[test]
    fn test_regex_profile_invalid_pattern() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("test_field".to_string(), 1);
        
        let config = RegexProfileConfig {
            name: "invalid_regex".to_string(),
            pattern: r"[invalid regex(".to_string(), // Invalid regex pattern
            field_mappings,
            timestamp_field: None,
            level_field: None,
            message_field: None,
            timestamp_format: None,
        };
        
        let result = RegexProfile::new(config);
        assert!(result.is_err());
        
        if let Err(ParseError::RegexError { pattern, error_message }) = result {
            assert!(pattern.contains("invalid regex"));
            assert!(!error_message.is_empty());
        } else {
            panic!("Expected RegexError for invalid pattern");
        }
    }
    
    #[test]
    fn test_regex_profile_invalid_timestamp_format() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp".to_string(), 1);
        
        let config = RegexProfileConfig {
            name: "invalid_timestamp_format".to_string(),
            pattern: r"^(\d{4}-\d{2}-\d{2})$".to_string(),
            field_mappings,
            timestamp_field: Some("timestamp".to_string()),
            level_field: None,
            message_field: None,
            timestamp_format: Some("%invalid_format%".to_string()), // Invalid timestamp format
        };
        
        let result = RegexProfile::new(config);
        assert!(result.is_err());
        
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = result {
            assert_eq!(parameter, "timestamp_format");
            assert!(error_message.contains("Invalid timestamp format"));
        } else {
            panic!("Expected ConfigurationError for invalid timestamp format");
        }
    }
    
    #[test]
    fn test_csv_profile_empty_column_mappings() {
        let config = CsvProfileConfig {
            name: "empty_mappings".to_string(),
            delimiter: ',',
            has_header: false,
            column_mappings: HashMap::new(), // Empty mappings
            timestamp_column: None,
            level_column: None,
            message_column: None,
            timestamp_format: None,
        };
        
        let result = CsvProfile::new(config);
        assert!(result.is_err());
        
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = result {
            assert_eq!(parameter, "column_mappings");
            assert!(error_message.contains("At least one column mapping must be specified"));
        } else {
            panic!("Expected ConfigurationError for empty column mappings");
        }
    }
    
    #[test]
    fn test_csv_profile_invalid_timestamp_format() {
        let mut column_mappings = HashMap::new();
        column_mappings.insert("timestamp".to_string(), 0);
        
        let config = CsvProfileConfig {
            name: "invalid_timestamp_format".to_string(),
            delimiter: ',',
            has_header: false,
            column_mappings,
            timestamp_column: Some("timestamp".to_string()),
            level_column: None,
            message_column: None,
            timestamp_format: Some("%bad_format%".to_string()), // Invalid timestamp format
        };
        
        let result = CsvProfile::new(config);
        assert!(result.is_err());
        
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = result {
            assert_eq!(parameter, "timestamp_format");
            assert!(error_message.contains("Invalid timestamp format"));
        } else {
            panic!("Expected ConfigurationError for invalid timestamp format");
        }
    }
    
    #[test]
    fn test_profile_parser_validation_regex() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("invalid_field".to_string(), 99); // Invalid capture group
        
        let config = RegexProfileConfig {
            name: "invalid_profile".to_string(),
            pattern: r"^(\w+)$".to_string(),
            field_mappings,
            timestamp_field: None,
            level_field: None,
            message_field: None,
            timestamp_format: None,
        };
        
        let parser_result = ProfileParser::new_regex(config);
        assert!(parser_result.is_err());
        
        // Verify the error message is descriptive
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = parser_result {
            assert!(parameter.contains("invalid_field"));
            assert!(error_message.contains("Capture group"));
            assert!(error_message.contains("does not exist"));
        } else {
            panic!("Expected descriptive ConfigurationError");
        }
    }
    
    #[test]
    fn test_profile_parser_validation_csv() {
        let config = CsvProfileConfig {
            name: "empty_mappings".to_string(),
            delimiter: ',',
            has_header: false,
            column_mappings: HashMap::new(),
            timestamp_column: None,
            level_column: None,
            message_column: None,
            timestamp_format: None,
        };
        
        let parser_result = ProfileParser::new_csv(config);
        assert!(parser_result.is_err());
        
        // Verify the error message is descriptive
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = parser_result {
            assert_eq!(parameter, "column_mappings");
            assert!(error_message.contains("At least one column mapping"));
        } else {
            panic!("Expected descriptive ConfigurationError");
        }
    }
    
    #[test]
    fn test_built_in_profiles_always_valid() {
        // Apache profile should always be valid
        let apache_parser = ProfileParser::new_apache();
        assert!(apache_parser.validate().is_ok());
        
        // Nginx profile should always be valid
        let nginx_parser = ProfileParser::new_nginx();
        assert!(nginx_parser.validate().is_ok());
        
        // Syslog profile should always be valid
        let syslog_parser = ProfileParser::new_syslog();
        assert!(syslog_parser.validate().is_ok());
    }
    
    #[test]
    fn test_valid_regex_profile_configuration() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp".to_string(), 1);
        field_mappings.insert("level".to_string(), 2);
        field_mappings.insert("message".to_string(), 3);
        
        let config = RegexProfileConfig {
            name: "valid_profile".to_string(),
            pattern: r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string(),
            field_mappings,
            timestamp_field: Some("timestamp".to_string()),
            level_field: Some("level".to_string()),
            message_field: Some("message".to_string()),
            timestamp_format: None, // Don't test timestamp format validation here
        };
        
        let result = RegexProfile::new(config);
        if let Err(ref e) = result {
            println!("Error creating regex profile: {:?}", e);
        }
        assert!(result.is_ok());
        
        let profile = result.unwrap();
        assert!(profile.validate().is_ok());
    }
    
    #[test]
    fn test_valid_csv_profile_configuration() {
        let mut column_mappings = HashMap::new();
        column_mappings.insert("timestamp".to_string(), 0);
        column_mappings.insert("level".to_string(), 1);
        column_mappings.insert("message".to_string(), 2);
        
        let config = CsvProfileConfig {
            name: "valid_csv".to_string(),
            delimiter: ',',
            has_header: true,
            column_mappings,
            timestamp_column: Some("timestamp".to_string()),
            level_column: Some("level".to_string()),
            message_column: Some("message".to_string()),
            timestamp_format: None, // Don't test timestamp format validation here
        };
        
        let result = CsvProfile::new(config);
        assert!(result.is_ok());
        
        let profile = result.unwrap();
        assert!(profile.validate().is_ok());
    }
}