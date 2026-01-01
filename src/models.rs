use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::ParseError;

/// Parsing statistics for monitoring and debugging
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParsingStatistics {
    /// Total number of lines processed
    pub total_lines: usize,
    /// Number of successfully parsed lines
    pub successful_parses: usize,
    /// Number of lines that failed to parse
    pub failed_parses: usize,
    /// Number of lines processed as plain text fallback
    pub plain_text_fallbacks: usize,
    /// Format distribution
    pub format_distribution: HashMap<FormatType, usize>,
    /// Error distribution by type
    pub error_distribution: HashMap<String, usize>,
    /// Processing time statistics (in microseconds)
    pub processing_time_micros: ProcessingTimeStats,
    /// Memory usage statistics
    pub memory_stats: MemoryStats,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingTimeStats {
    pub total_time: u64,
    pub min_time: u64,
    pub max_time: u64,
    pub avg_time: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryStats {
    pub peak_memory_bytes: usize,
    pub current_memory_bytes: usize,
    pub total_allocations: usize,
}

impl ParsingStatistics {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Record a successful parse
    pub fn record_success(&mut self, format_type: FormatType, processing_time_micros: u64) {
        self.total_lines += 1;
        self.successful_parses += 1;
        *self.format_distribution.entry(format_type).or_insert(0) += 1;
        self.update_processing_time(processing_time_micros);
    }
    
    /// Record a failed parse
    pub fn record_failure(&mut self, error: &ParseError, processing_time_micros: u64) {
        self.total_lines += 1;
        self.failed_parses += 1;
        let error_type = self.error_type_name(error);
        *self.error_distribution.entry(error_type).or_insert(0) += 1;
        self.update_processing_time(processing_time_micros);
    }
    
    /// Record a plain text fallback
    pub fn record_plain_text_fallback(&mut self, processing_time_micros: u64) {
        self.total_lines += 1;
        self.successful_parses += 1;
        self.plain_text_fallbacks += 1;
        *self.format_distribution.entry(FormatType::PlainText).or_insert(0) += 1;
        self.update_processing_time(processing_time_micros);
    }
    
    /// Get success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_lines == 0 {
            0.0
        } else {
            (self.successful_parses as f64 / self.total_lines as f64) * 100.0
        }
    }
    
    /// Get error rate as a percentage
    pub fn error_rate(&self) -> f64 {
        if self.total_lines == 0 {
            0.0
        } else {
            (self.failed_parses as f64 / self.total_lines as f64) * 100.0
        }
    }
    
    /// Get plain text fallback rate as a percentage
    pub fn fallback_rate(&self) -> f64 {
        if self.total_lines == 0 {
            0.0
        } else {
            (self.plain_text_fallbacks as f64 / self.total_lines as f64) * 100.0
        }
    }
    
    fn update_processing_time(&mut self, time_micros: u64) {
        self.processing_time_micros.total_time = self.processing_time_micros.total_time.saturating_add(time_micros);
        
        if self.processing_time_micros.min_time == 0 || time_micros < self.processing_time_micros.min_time {
            self.processing_time_micros.min_time = time_micros;
        }
        
        if time_micros > self.processing_time_micros.max_time {
            self.processing_time_micros.max_time = time_micros;
        }
        
        self.processing_time_micros.avg_time = 
            self.processing_time_micros.total_time as f64 / self.total_lines as f64;
    }
    
    fn error_type_name(&self, error: &ParseError) -> String {
        match error {
            ParseError::JsonSyntaxError { .. } => "JsonSyntaxError".to_string(),
            ParseError::JsonNotObject { .. } => "JsonNotObject".to_string(),
            ParseError::LogfmtInsufficientPairs { .. } => "LogfmtInsufficientPairs".to_string(),
            ParseError::LogfmtMalformedSyntax { .. } => "LogfmtMalformedSyntax".to_string(),
            ParseError::TimestampParseError { .. } => "TimestampParseError".to_string(),
            ParseError::LevelParseError { .. } => "LevelParseError".to_string(),
            ParseError::PatternMatchError { .. } => "PatternMatchError".to_string(),
            ParseError::FieldExtractionError { .. } => "FieldExtractionError".to_string(),
            ParseError::RegexError { .. } => "RegexError".to_string(),
            ParseError::IoError { .. } => "IoError".to_string(),
            ParseError::ResourceExhausted { .. } => "ResourceExhausted".to_string(),
            ParseError::ConfigurationError { .. } => "ConfigurationError".to_string(),
            ParseError::GenericError { .. } => "GenericError".to_string(),
        }
    }
}

/// Normalized log levels in order of severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Fatal = 5,
}

impl LogLevel {
    /// Parse a log level from a string, case-insensitive
    /// Supports standard syslog levels: emerg, alert, crit, error, warn, notice, info, debug
    /// Also supports common aliases used by various logging frameworks
    pub fn from_str(s: &str) -> Option<LogLevel> {
        match s.to_lowercase().as_str() {
            "trace" | "trc" | "verbose" => Some(LogLevel::Trace),
            "debug" | "dbg" | "d" => Some(LogLevel::Debug),
            "info" | "inf" | "i" | "notice" | "note" => Some(LogLevel::Info),
            "warn" | "warning" | "w" => Some(LogLevel::Warn),
            "error" | "err" | "e" | "severe" => Some(LogLevel::Error),
            "fatal" | "crit" | "critical" | "f" | "emerg" | "emergency" | "alert" | "panic" => Some(LogLevel::Fatal),
            _ => None,
        }
    }
}

/// Metadata about the log source
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub file: Option<String>,
    pub stream: Option<String>,
    pub host: Option<String>,
    pub offset: Option<u64>,
}

impl Default for SourceMetadata {
    fn default() -> Self {
        Self {
            file: None,
            stream: None,
            host: None,
            offset: None,
        }
    }
}

/// Detected log format types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FormatType {
    Json,
    Logfmt,
    Pattern,
    TimestampLevel,
    Profile(ProfileType),
    PlainText,
}

/// User-defined profile types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfileType {
    Regex,
    Csv,
    Apache,
    Nginx,
    Syslog,
}

/// Canonical event model - unified representation for all parsed log events
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    /// Parsed timestamp or None if not available/parseable
    pub timestamp: Option<DateTime<Utc>>,
    
    /// Normalized log level or None if not available/parseable
    pub level: Option<LogLevel>,
    
    /// Primary log message (required field)
    pub message: String,
    
    /// Structured key-value data extracted from the log
    pub fields: HashMap<String, serde_json::Value>,
    
    /// Original log line preserved for reference
    pub raw: String,
    
    /// Metadata about the log source
    pub source: SourceMetadata,
    
    /// True if parsing encountered errors but continued
    pub parse_error: Option<bool>,
    
    /// Detected format type for debugging and optimization
    pub format_type: FormatType,
}

impl CanonicalEvent {
    /// Create a new canonical event with required fields
    pub fn new(message: String, raw: String, format_type: FormatType) -> Self {
        Self {
            timestamp: None,
            level: None,
            message,
            fields: HashMap::new(),
            raw,
            source: SourceMetadata::default(),
            parse_error: None,
            format_type,
        }
    }
    
    /// Create a canonical event with parse error marked
    pub fn with_error(raw: String, error_message: String) -> Self {
        Self {
            timestamp: None,
            level: None,
            message: error_message,
            fields: HashMap::new(),
            raw,
            source: SourceMetadata::default(),
            parse_error: Some(true),
            format_type: FormatType::PlainText,
        }
    }
    
    /// Add a field to the structured data
    pub fn add_field<T: Into<serde_json::Value>>(&mut self, key: String, value: T) {
        self.fields.insert(key, value.into());
    }
    
    /// Set the timestamp from various input types
    pub fn set_timestamp(&mut self, timestamp: DateTime<Utc>) {
        self.timestamp = Some(timestamp);
    }
    
    /// Set the log level
    pub fn set_level(&mut self, level: LogLevel) {
        self.level = Some(level);
    }
    
    /// Mark this event as having a parse error
    pub fn mark_parse_error(&mut self) {
        self.parse_error = Some(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    
    // Implement Arbitrary for our types to enable property-based testing
    impl Arbitrary for LogLevel {
        fn arbitrary(g: &mut Gen) -> Self {
            let levels = [
                LogLevel::Trace,
                LogLevel::Debug,
                LogLevel::Info,
                LogLevel::Warn,
                LogLevel::Error,
                LogLevel::Fatal,
            ];
            *g.choose(&levels).unwrap()
        }
    }
    
    impl Arbitrary for FormatType {
        fn arbitrary(g: &mut Gen) -> Self {
            let formats = [
                FormatType::Json,
                FormatType::Logfmt,
                FormatType::TimestampLevel,
                FormatType::PlainText,
            ];
            *g.choose(&formats).unwrap()
        }
    }
    
    impl Arbitrary for SourceMetadata {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                file: if bool::arbitrary(g) { Some(String::arbitrary(g)) } else { None },
                stream: if bool::arbitrary(g) { Some(String::arbitrary(g)) } else { None },
                host: if bool::arbitrary(g) { Some(String::arbitrary(g)) } else { None },
                offset: if bool::arbitrary(g) { Some(u64::arbitrary(g)) } else { None },
            }
        }
    }
    
    // Property 1: Canonical Event Structure Consistency
    // Feature: log-type-detection-and-parsing, Property 1: Canonical Event Structure Consistency
    // Validates: Requirements 1.1, 1.2, 1.5
    #[quickcheck(tests = 5)]
    fn prop_canonical_event_structure_consistency(
        message: String,
        raw: String,
        format_type: FormatType,
    ) -> bool {
        let event = CanonicalEvent::new(message.clone(), raw.clone(), format_type);
        
        // The parsed result should always contain a canonical event with required structure
        // Message field must be populated (requirement 1.2)
        !event.message.is_empty() || message.is_empty() && 
        // Raw field must contain the original line (requirement 1.5)
        event.raw == raw &&
        // Format type must be preserved
        event.format_type == format_type &&
        // Optional fields should be properly initialized
        event.timestamp.is_none() &&
        event.level.is_none() &&
        event.fields.is_empty() &&
        event.parse_error.is_none()
    }
    
    #[quickcheck(tests = 5)]
    fn prop_canonical_event_with_error_structure(raw: String, error_message: String) -> bool {
        let event = CanonicalEvent::with_error(raw.clone(), error_message.clone());
        
        // Error events should have consistent structure
        event.message == error_message &&
        event.raw == raw &&
        event.parse_error == Some(true) &&
        event.format_type == FormatType::PlainText &&
        event.timestamp.is_none() &&
        event.level.is_none() &&
        event.fields.is_empty()
    }
    
    #[quickcheck(tests = 5)]
    fn prop_canonical_event_field_operations(
        mut event: CanonicalEvent,
        key: String,
        value: i32,
        level: LogLevel,
    ) -> bool {
        let original_message = event.message.clone();
        let original_raw = event.raw.clone();
        
        // Test field operations preserve core structure
        event.add_field(key.clone(), value);
        event.set_level(level);
        event.mark_parse_error();
        
        // Core fields should remain unchanged
        event.message == original_message &&
        event.raw == original_raw &&
        // New operations should work correctly
        event.fields.contains_key(&key) &&
        event.level == Some(level) &&
        event.parse_error == Some(true)
    }
    
    impl Arbitrary for CanonicalEvent {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut event = CanonicalEvent::new(
                String::arbitrary(g),
                String::arbitrary(g),
                FormatType::arbitrary(g),
            );
            
            // Randomly populate optional fields
            if bool::arbitrary(g) {
                event.timestamp = Some(chrono::Utc::now());
            }
            if bool::arbitrary(g) {
                event.level = Some(LogLevel::arbitrary(g));
            }
            if bool::arbitrary(g) {
                event.parse_error = Some(bool::arbitrary(g));
            }
            event.source = SourceMetadata::arbitrary(g);
            
            event
        }
    }
    
    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("INFO"), Some(LogLevel::Info));
        assert_eq!(LogLevel::from_str("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::from_str("WARN"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("invalid"), None);
    }
    
    #[test]
    fn test_canonical_event_creation() {
        let event = CanonicalEvent::new(
            "Test message".to_string(),
            "raw log line".to_string(),
            FormatType::Json,
        );
        
        assert_eq!(event.message, "Test message");
        assert_eq!(event.raw, "raw log line");
        assert_eq!(event.format_type, FormatType::Json);
        assert!(event.timestamp.is_none());
        assert!(event.level.is_none());
        assert!(event.parse_error.is_none());
    }
    
    #[test]
    fn test_canonical_event_with_error() {
        let event = CanonicalEvent::with_error(
            "malformed log".to_string(),
            "Parse error occurred".to_string(),
        );
        
        assert_eq!(event.message, "Parse error occurred");
        assert_eq!(event.raw, "malformed log");
        assert_eq!(event.parse_error, Some(true));
        assert_eq!(event.format_type, FormatType::PlainText);
    }
}