use crate::models::*;
use crate::parse_result::ParseResult;
use crate::parsers::LogParser;
use chrono::{DateTime, Utc};
use regex::Regex;
use std::time::Instant;

/// Plain text parser for unrecognized log formats (fallback parser)
#[derive(Clone)]
pub struct PlainTextParser {
    // Optional timestamp inference patterns
    timestamp_inference_regex: Regex,
    // Simple field extraction patterns
    field_extraction_regex: Regex,
}

impl PlainTextParser {
    pub fn new() -> Self {
        Self {
            // Look for timestamp-like patterns anywhere in the line
            timestamp_inference_regex: Regex::new(
                r"(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
            ).unwrap(),
            // Look for key=value or key:value patterns for basic field extraction
            field_extraction_regex: Regex::new(
                r"([a-zA-Z0-9_.-]+)[:=]([^\s,;]+)"
            ).unwrap(),
        }
    }
    
    /// Attempt to infer timestamp from plain text
    fn infer_timestamp(&self, line: &str) -> Option<DateTime<Utc>> {
        if let Some(captures) = self.timestamp_inference_regex.captures(line) {
            let timestamp_str = captures.get(1).unwrap().as_str();
            
            // Try parsing the inferred timestamp
            if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
                return Some(dt.with_timezone(&Utc));
            }
            
            // Try ISO8601 without timezone
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S") {
                return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
            }
            
            // Try space-separated format
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
                return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
            }
        }
        
        None
    }
    
    /// Extract basic fields using regex patterns
    fn extract_fields(&self, line: &str) -> std::collections::HashMap<String, serde_json::Value> {
        let mut fields = std::collections::HashMap::new();
        
        for captures in self.field_extraction_regex.captures_iter(line) {
            let key = captures.get(1).unwrap().as_str();
            let value = captures.get(2).unwrap().as_str();
            
            // Try to infer value type
            let json_value = if let Ok(num) = value.parse::<i64>() {
                serde_json::Value::Number(serde_json::Number::from(num))
            } else if let Ok(float) = value.parse::<f64>() {
                serde_json::Value::Number(serde_json::Number::from_f64(float).unwrap_or(serde_json::Number::from(0)))
            } else if value.eq_ignore_ascii_case("true") {
                serde_json::Value::Bool(true)
            } else if value.eq_ignore_ascii_case("false") {
                serde_json::Value::Bool(false)
            } else {
                serde_json::Value::String(value.to_string())
            };
            
            fields.insert(key.to_string(), json_value);
        }
        
        fields
    }
    
    /// Infer log level from plain text using common keywords
    fn infer_level(&self, line: &str) -> Option<LogLevel> {
        let line_upper = line.to_uppercase();
        
        // Check for level keywords in order of severity
        if line_upper.contains("FATAL") || line_upper.contains("CRITICAL") {
            Some(LogLevel::Error) // Map FATAL/CRITICAL to ERROR
        } else if line_upper.contains("ERROR") || line_upper.contains("ERR") {
            Some(LogLevel::Error)
        } else if line_upper.contains("WARN") || line_upper.contains("WARNING") {
            Some(LogLevel::Warn)
        } else if line_upper.contains("INFO") || line_upper.contains("INFORMATION") {
            Some(LogLevel::Info)
        } else if line_upper.contains("DEBUG") || line_upper.contains("DBG") {
            Some(LogLevel::Debug)
        } else if line_upper.contains("TRACE") {
            Some(LogLevel::Debug) // Map TRACE to DEBUG
        } else {
            None // No level inference possible
        }
    }
}

impl LogParser for PlainTextParser {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        // Plain text parser always succeeds - it's the fallback
        let mut event = CanonicalEvent::new(
            line.to_string(), // Entire line becomes the message
            line.to_string(),
            FormatType::PlainText,
        );
        
        // Try to infer timestamp
        if let Some(timestamp) = self.infer_timestamp(line) {
            event.set_timestamp(timestamp);
        }
        
        // Try to infer log level
        if let Some(level) = self.infer_level(line) {
            event.set_level(level);
        }
        
        // Extract any basic fields we can find
        let fields = self.extract_fields(line);
        for (key, value) in fields {
            event.add_field(key, value);
        }
        
        // Set confidence based on how much we could infer
        let mut confidence = 0.1; // Base confidence for plain text
        if event.timestamp.is_some() {
            confidence += 0.2;
        }
        if event.level.is_some() && event.level != Some(LogLevel::Info) { // If we inferred a different level
            confidence += 0.1;
        }
        if !event.fields.is_empty() {
            confidence += 0.1;
        }
        
        let processing_time = start_time.elapsed().as_micros() as u64;
        ParseResult::success_with_timing(event, confidence, processing_time)
    }
    
    fn can_parse(&self, _line: &str) -> bool {
        // Plain text parser can always parse any line (it's the fallback)
        true
    }
    
    fn get_format_type(&self) -> FormatType {
        FormatType::PlainText
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_plain_text_basic_parsing() {
        let parser = PlainTextParser::new();
        
        let result = parser.parse("This is a plain text log message");
        assert!(result.success);
        assert_eq!(result.event.message, "This is a plain text log message");
        assert_eq!(result.event.format_type, FormatType::PlainText);
    }
    
    #[test]
    fn test_plain_text_timestamp_inference() {
        let parser = PlainTextParser::new();
        
        let result = parser.parse("2025-12-29T10:21:03Z This is a message with timestamp");
        assert!(result.success);
        assert!(result.event.timestamp.is_some());
        assert_eq!(result.event.message, "2025-12-29T10:21:03Z This is a message with timestamp");
    }
    
    #[test]
    fn test_plain_text_level_inference() {
        let parser = PlainTextParser::new();
        
        let result = parser.parse("ERROR: Something went wrong");
        assert!(result.success);
        assert_eq!(result.event.level, Some(LogLevel::Error));
        
        let result = parser.parse("WARNING: This is a warning");
        assert!(result.success);
        assert_eq!(result.event.level, Some(LogLevel::Warn));
        
        let result = parser.parse("INFO: This is information");
        assert!(result.success);
        assert_eq!(result.event.level, Some(LogLevel::Info));
    }
    
    #[test]
    fn test_plain_text_field_extraction() {
        let parser = PlainTextParser::new();
        
        let result = parser.parse("user=admin action=login status=success count=5");
        assert!(result.success);
        assert!(result.event.fields.contains_key("user"));
        assert!(result.event.fields.contains_key("action"));
        assert!(result.event.fields.contains_key("status"));
        assert!(result.event.fields.contains_key("count"));
        
        // Check that count was parsed as a number
        if let Some(serde_json::Value::Number(n)) = result.event.fields.get("count") {
            assert_eq!(n.as_i64(), Some(5));
        } else {
            panic!("count should be parsed as a number");
        }
    }
    
    #[test]
    fn test_plain_text_always_can_parse() {
        let parser = PlainTextParser::new();
        
        assert!(parser.can_parse(""));
        assert!(parser.can_parse("any text"));
        assert!(parser.can_parse("!@#$%^&*()"));
        assert!(parser.can_parse("unicode: 你好世界"));
    }
}