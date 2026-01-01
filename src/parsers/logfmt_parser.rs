use crate::models::*;
use crate::error::ParseError;
use crate::parse_result::ParseResult;
use crate::parsers::LogParser;
use regex::Regex;
use std::collections::HashMap;
use std::time::Instant;

/// Logfmt parser for key=value formatted logs
#[derive(Clone)]
pub struct LogfmtParser {
    key_value_regex: Regex,
}

impl LogfmtParser {
    pub fn new() -> Self {
        Self {
            // Regex to match key=value pairs with optional quotes
            key_value_regex: Regex::new(r#"([a-zA-Z0-9_.-]+)=(?:"((?:[^"\\]|\\.)*)"|([^\s]+))"#).unwrap(),
        }
    }
    
    /// Extract key=value pairs from a logfmt line
    pub fn extract_pairs(&self, line: &str) -> HashMap<String, String> {
        let mut pairs = HashMap::new();
        
        for cap in self.key_value_regex.captures_iter(line) {
            let key = cap.get(1).unwrap().as_str().to_string();
            
            // Handle quoted and unquoted values
            let value = if let Some(quoted_value) = cap.get(2) {
                // Quoted value - handle escaped quotes and other escape sequences
                quoted_value.as_str()
                    .replace(r#"\""#, r#"""#)
                    .replace(r#"\\"#, r#"\"#)
                    .replace(r#"\n"#, "\n")
                    .replace(r#"\t"#, "\t")
            } else if let Some(unquoted_value) = cap.get(3) {
                // Unquoted value
                unquoted_value.as_str().to_string()
            } else {
                // Shouldn't happen with our regex, but handle gracefully
                String::new()
            };
            
            pairs.insert(key, value);
        }
        
        pairs
    }
    
    /// Check if line has minimum threshold of key=value pairs for logfmt detection
    fn meets_logfmt_threshold(&self, line: &str) -> bool {
        self.key_value_regex.find_iter(line).count() >= 3
    }
}

impl LogParser for LogfmtParser {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        let pairs = self.extract_pairs(line);
        
        // Check if we have enough pairs to be confident this is logfmt
        if pairs.len() < 3 {
            let error = ParseError::LogfmtInsufficientPairs {
                found_pairs: pairs.len(),
                required_pairs: 3,
            };
            
            let processing_time = start_time.elapsed().as_micros() as u64;
            return ParseResult::failure_with_context(
                line.to_string(),
                error,
                None,
                Some(processing_time),
            );
        }
        
        let mut event = CanonicalEvent::new(
            line.to_string(), // Use entire line as message for now
            line.to_string(),
            FormatType::Logfmt,
        );
        
        // Store all fields
        for (key, value) in &pairs {
            event.add_field(key.clone(), serde_json::Value::String(value.clone()));
        }
        
        let confidence = if pairs.len() >= 5 { 0.9 } else { 0.7 };
        let processing_time = start_time.elapsed().as_micros() as u64;
        ParseResult::success_with_timing(event, confidence, processing_time)
    }
    
    fn can_parse(&self, line: &str) -> bool {
        self.meets_logfmt_threshold(line)
    }
    
    fn get_format_type(&self) -> FormatType {
        FormatType::Logfmt
    }
}