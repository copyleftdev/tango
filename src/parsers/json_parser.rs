use crate::models::*;
use crate::error::ParseError;
use crate::parse_result::ParseResult;
use crate::parsers::LogParser;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Instant;

/// JSON parser for structured JSON logs
#[derive(Clone)]
pub struct JsonParser;

impl JsonParser {
    pub fn new() -> Self {
        Self
    }
    
    /// Extract timestamp from JSON value using common field names
    fn extract_timestamp(&self, json: &Map<String, Value>) -> Option<DateTime<Utc>> {
        let timestamp_fields = ["ts", "time", "timestamp", "@timestamp"];
        
        for field in &timestamp_fields {
            if let Some(value) = json.get(*field) {
                if let Some(timestamp) = self.parse_timestamp_value(value) {
                    return Some(timestamp);
                }
            }
        }
        None
    }
    
    /// Parse timestamp from various JSON value types
    fn parse_timestamp_value(&self, value: &Value) -> Option<DateTime<Utc>> {
        match value {
            Value::String(s) => {
                // Try parsing ISO8601/RFC3339 formats
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Some(dt.with_timezone(&Utc));
                }
                // Try parsing ISO8601 without timezone
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
                }
                // Try parsing other common formats
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
                }
            }
            Value::Number(n) => {
                // Handle Unix timestamps (seconds or milliseconds)
                if let Some(timestamp) = n.as_i64() {
                    // Try as seconds first
                    if let Some(dt) = DateTime::from_timestamp(timestamp, 0) {
                        return Some(dt);
                    }
                    // Try as milliseconds
                    if let Some(dt) = DateTime::from_timestamp_millis(timestamp) {
                        return Some(dt);
                    }
                }
            }
            _ => {}
        }
        None
    }
    
    /// Extract log level from JSON value using common field names
    fn extract_level(&self, json: &Map<String, Value>) -> Option<LogLevel> {
        let level_fields = ["level", "severity", "lvl", "log.level"];
        
        for field in &level_fields {
            if let Some(value) = json.get(*field) {
                if let Some(level_str) = value.as_str() {
                    if let Some(level) = LogLevel::from_str(level_str) {
                        return Some(level);
                    }
                }
            }
        }
        None
    }
    
    /// Extract message from JSON value using common field names
    fn extract_message(&self, json: &Map<String, Value>) -> Option<String> {
        let message_fields = ["msg", "message", "log.message"];
        
        for field in &message_fields {
            if let Some(value) = json.get(*field) {
                if let Some(msg) = value.as_str() {
                    return Some(msg.to_string());
                }
            }
        }
        None
    }
    
    /// Flatten nested JSON objects using dot notation
    fn flatten_object(&self, obj: &Map<String, Value>, prefix: &str, result: &mut HashMap<String, Value>) {
        for (key, value) in obj {
            let full_key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", prefix, key)
            };
            
            match value {
                Value::Object(nested) => {
                    self.flatten_object(nested, &full_key, result);
                }
                Value::Array(arr) => {
                    // Convert arrays to string representation for simplicity
                    result.insert(full_key, Value::String(format!("{:?}", arr)));
                }
                _ => {
                    result.insert(full_key, value.clone());
                }
            }
        }
    }
    
    /// Extract line and column information from JSON error message
    fn extract_json_error_position(&self, error_msg: &str) -> (Option<usize>, Option<usize>) {
        // Try to extract line and column from serde_json error messages
        // Example: "EOF while parsing a string at line 1 column 15"
        let line_regex = regex::Regex::new(r"line (\d+)").unwrap();
        let column_regex = regex::Regex::new(r"column (\d+)").unwrap();
        
        let line_num = line_regex.captures(error_msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse().ok());
            
        let column = column_regex.captures(error_msg)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse().ok());
            
        (line_num, column)
    }
}

impl LogParser for JsonParser {
    fn parse(&self, line: &str) -> ParseResult {
        let start_time = Instant::now();
        
        // Try to parse as JSON
        match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(json_obj)) => {
                let mut event = CanonicalEvent::new(
                    String::new(), // Will be set below
                    line.to_string(),
                    FormatType::Json,
                );
                
                // Extract timestamp with error handling
                if let Some(timestamp) = self.extract_timestamp(&json_obj) {
                    event.set_timestamp(timestamp);
                }
                
                // Extract level with error handling
                if let Some(level) = self.extract_level(&json_obj) {
                    event.set_level(level);
                }
                
                // Extract message
                let message = self.extract_message(&json_obj)
                    .unwrap_or_else(|| {
                        // If no message field found, use the entire JSON as message
                        line.to_string()
                    });
                event.message = message;
                
                // Flatten and store all other fields
                let mut flattened_fields = HashMap::new();
                self.flatten_object(&json_obj, "", &mut flattened_fields);
                
                // Remove the fields we've already extracted to canonical fields
                let extracted_fields = ["ts", "time", "timestamp", "@timestamp", 
                                      "level", "severity", "lvl", "log.level",
                                      "msg", "message", "log.message"];
                for field in &extracted_fields {
                    flattened_fields.remove(*field);
                }
                
                // Convert HashMap<String, Value> to HashMap<String, serde_json::Value>
                for (key, value) in flattened_fields {
                    event.add_field(key, value);
                }
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::success_with_timing(event, 0.95, processing_time) // High confidence for valid JSON
            }
            Ok(other_value) => {
                // Valid JSON but not an object (e.g., array, primitive)
                let actual_type = match other_value {
                    Value::Array(_) => "array",
                    Value::String(_) => "string", 
                    Value::Number(_) => "number",
                    Value::Bool(_) => "boolean",
                    Value::Null => "null",
                    _ => "unknown",
                };
                
                let error = ParseError::JsonNotObject {
                    actual_type: actual_type.to_string(),
                };
                
                let processing_time = start_time.elapsed().as_micros() as u64;
                ParseResult::failure_with_context(
                    line.to_string(),
                    error,
                    None,
                    Some(processing_time),
                )
            }
            Err(json_error) => {
                // Invalid JSON - extract line and column information if available
                let error_msg = json_error.to_string();
                let (line_num, column) = self.extract_json_error_position(&error_msg);
                
                let error = ParseError::JsonSyntaxError {
                    message: error_msg,
                    line_number: line_num,
                    column,
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
        // Quick heuristic: line should start with '{' and be valid JSON
        line.trim_start().starts_with('{') && serde_json::from_str::<Value>(line).is_ok()
    }
    
    fn get_format_type(&self) -> FormatType {
        FormatType::Json
    }
}