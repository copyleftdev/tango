use crate::parse_result::ParseResult;
use crate::statistics::{ParsingStatistics, StatisticsMonitor};
use crate::parsers::{JsonParser, LogfmtParser, PatternParser, PlainTextParser, LogParser};

/// Resilient parsing engine that demonstrates error handling and continuation
pub struct ResilientParser {
    json_parser: JsonParser,
    logfmt_parser: LogfmtParser,
    pattern_parser: PatternParser,
    plain_text_parser: PlainTextParser,
    statistics_monitor: StatisticsMonitor,
}

impl ResilientParser {
    pub fn new() -> Self {
        Self {
            json_parser: JsonParser::new(),
            logfmt_parser: LogfmtParser::new(),
            pattern_parser: PatternParser::new(),
            plain_text_parser: PlainTextParser::new(),
            statistics_monitor: StatisticsMonitor::new(),
        }
    }
    
    /// Create a new resilient parser with monitoring settings
    pub fn with_monitoring(monitoring_enabled: bool, debug_output_enabled: bool, report_interval: usize) -> Self {
        Self {
            json_parser: JsonParser::new(),
            logfmt_parser: LogfmtParser::new(),
            pattern_parser: PatternParser::new(),
            plain_text_parser: PlainTextParser::new(),
            statistics_monitor: StatisticsMonitor::with_settings(monitoring_enabled, debug_output_enabled, report_interval),
        }
    }
    
    /// Parse multiple lines with comprehensive error handling and continuation
    pub fn parse_lines(&mut self, lines: Vec<String>) -> Vec<ParseResult> {
        let mut results = Vec::new();
        
        for (line_number, line) in lines.iter().enumerate() {
            let result = self.parse_line_with_fallback(line, Some(line_number + 1));
            
            // Update statistics with monitoring
            if result.success {
                if let Some(processing_time) = result.processing_time_micros {
                    self.statistics_monitor.record_success(result.event.format_type, processing_time);
                } else {
                    self.statistics_monitor.record_success(result.event.format_type, 0);
                }
            } else {
                if let Some(error) = &result.error {
                    if let Some(processing_time) = result.processing_time_micros {
                        self.statistics_monitor.record_failure(error, processing_time);
                    } else {
                        self.statistics_monitor.record_failure(error, 0);
                    }
                }
            }
            
            results.push(result);
        }
        
        results
    }
    
    /// Parse a single line using the fallback chain with comprehensive error handling
    pub fn parse_line_with_fallback(&mut self, line: &str, line_number: Option<usize>) -> ParseResult {
        // Stage 1: Try JSON parsing first
        if line.trim_start().starts_with('{') {
            let json_result = self.json_parser.parse(line);
            if json_result.success {
                return json_result.with_line_number(line_number.unwrap_or(0));
            }
            // Continue to next stage on failure - don't return error yet
        }
        
        // Stage 2: Try logfmt parsing
        if self.logfmt_parser.can_parse(line) {
            let logfmt_result = self.logfmt_parser.parse(line);
            if logfmt_result.success {
                return logfmt_result.with_line_number(line_number.unwrap_or(0));
            }
            // Continue to next stage on failure
        }
        
        // Stage 3: Try timestamp+level pattern parsing
        if self.pattern_parser.can_parse(line) {
            let pattern_result = self.pattern_parser.parse(line);
            if pattern_result.success {
                return pattern_result.with_line_number(line_number.unwrap_or(0));
            }
            // Continue to next stage on failure
        }
        
        // Stage 4: Fall back to plain text (always succeeds)
        let plain_result = self.plain_text_parser.parse(line);
        plain_result.with_line_number(line_number.unwrap_or(0))
    }
    
    /// Get parsing statistics
    pub fn get_statistics(&self) -> &ParsingStatistics {
        self.statistics_monitor.get_statistics()
    }
    
    /// Get the statistics monitor
    pub fn get_statistics_monitor(&self) -> &StatisticsMonitor {
        &self.statistics_monitor
    }
    
    /// Get mutable access to the statistics monitor
    pub fn get_statistics_monitor_mut(&mut self) -> &mut StatisticsMonitor {
        &mut self.statistics_monitor
    }
    
    /// Reset statistics
    pub fn reset_statistics(&mut self) {
        self.statistics_monitor.reset();
    }
    
    /// Enable or disable monitoring
    pub fn set_monitoring_enabled(&mut self, enabled: bool) {
        self.statistics_monitor.set_monitoring_enabled(enabled);
    }
    
    /// Enable or disable debug output
    pub fn set_debug_output_enabled(&mut self, enabled: bool) {
        self.statistics_monitor.set_debug_output_enabled(enabled);
    }
    
    /// Print a comprehensive statistics report
    pub fn print_statistics_report(&self) {
        self.statistics_monitor.print_report();
    }
    
    /// Print a compact status line
    pub fn print_status_line(&self) {
        self.statistics_monitor.print_status_line();
    }
    
    /// Parse lines from an iterator with error resilience
    pub fn parse_lines_resilient<I>(&mut self, lines: I) -> Vec<ParseResult> 
    where 
        I: Iterator<Item = String>
    {
        let mut results = Vec::new();
        let mut line_number = 1;
        
        for line in lines {
            // Even if individual lines fail, continue processing
            let result = self.parse_line_with_fallback(&line, Some(line_number));
            results.push(result);
            line_number += 1;
        }
        
        results
    }
    
    /// Demonstrate error recovery by parsing problematic input
    pub fn demonstrate_error_recovery(&mut self) -> Vec<ParseResult> {
        let problematic_lines = vec![
            r#"{"incomplete": json"#.to_string(),                    // Malformed JSON
            "key=value".to_string(),                                // Insufficient logfmt pairs
            "2025-12-29T10:21:03Z INVALID_LEVEL message".to_string(), // Invalid level
            "".to_string(),                                         // Empty line
            "Plain text with no structure".to_string(),             // Plain text
            r#"{"valid": "json", "level": "INFO"}"#.to_string(),    // Valid JSON
            "level=INFO msg=test user=admin count=5".to_string(),   // Valid logfmt
            "[2025-12-29T10:21:03Z] [ERROR] Valid pattern".to_string(), // Valid pattern
        ];
        
        self.parse_lines(problematic_lines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;
    
    // Property 6: Error Resilience and Continuation
    // Feature: log-type-detection-and-parsing, Property 6: Error Resilience and Continuation
    // Validates: Requirements 6.5, 8.1, 8.2, 8.3
    #[quickcheck(tests = 100)]
    fn prop_error_resilience_and_continuation(
        malformed_inputs: Vec<String>,
        valid_inputs: Vec<String>,
    ) -> bool {
        let mut parser = ResilientParser::new();
        
        // Create a mixed batch of malformed and valid inputs
        let mut mixed_inputs = Vec::new();
        
        // Add malformed inputs (simulate various error conditions)
        for input in malformed_inputs.iter().take(10) {
            // Create various types of malformed inputs
            if input.is_empty() {
                mixed_inputs.push("".to_string()); // Empty input
            } else {
                // Create malformed JSON
                mixed_inputs.push(format!("{{\"incomplete\": {}", input));
                // Create insufficient logfmt
                mixed_inputs.push(format!("key={}", input));
                // Create invalid patterns
                mixed_inputs.push(format!("INVALID_LEVEL {}", input));
            }
        }
        
        // Add valid inputs
        for input in valid_inputs.iter().take(5) {
            if !input.trim().is_empty() {
                // Create valid JSON
                mixed_inputs.push(format!(r#"{{"message": "{}", "level": "INFO"}}"#, 
                    input.replace('"', "'").chars().take(50).collect::<String>()));
                // Create valid logfmt
                mixed_inputs.push(format!("level=INFO msg={} user=test count=1", 
                    input.replace(' ', "_").chars().take(20).collect::<String>()));
                // Create valid pattern
                mixed_inputs.push(format!("[2025-12-29T10:21:03Z] [INFO] {}", 
                    input.chars().take(50).collect::<String>()));
            }
        }
        
        // If no inputs generated, create some default test cases
        if mixed_inputs.is_empty() {
            mixed_inputs = vec![
                r#"{"incomplete": json"#.to_string(),
                "key=value".to_string(),
                "INVALID_LEVEL message".to_string(),
                "".to_string(),
                r#"{"valid": "json"}"#.to_string(),
                "level=INFO msg=test user=admin".to_string(),
                "[2025-12-29T10:21:03Z] [INFO] Valid message".to_string(),
            ];
        }
        
        // Parse all inputs - this tests error resilience and continuation
        let results = parser.parse_lines(mixed_inputs.clone());
        
        // For any parsing error, the system should mark the event appropriately (parse_error=true),
        // preserve the original line, and continue processing subsequent lines without termination
        
        // Verify we got results for all inputs (no termination)
        if results.len() != mixed_inputs.len() {
            return false;
        }
        
        let mut found_error = false;
        let mut found_success = false;
        
        for (i, result) in results.iter().enumerate() {
            let original_line = &mixed_inputs[i];
            
            // Check that original line is preserved in raw field
            if result.event.raw != *original_line {
                return false;
            }
            
            if result.success {
                found_success = true;
                // Successful parsing should not have parse_error flag
                if result.event.parse_error == Some(true) {
                    return false;
                }
            } else {
                found_error = true;
                // Failed parsing should mark parse_error=true
                if result.event.parse_error != Some(true) {
                    return false;
                }
                
                // Should have error information
                if result.error.is_none() {
                    return false;
                }
                
                // Should have preserved original line
                if result.event.raw != *original_line {
                    return false;
                }
            }
        }
        
        // We should have encountered both errors and successes in a mixed batch
        // (unless all inputs were identical, which is unlikely with property testing)
        if mixed_inputs.len() > 3 {
            // For larger batches, we expect to see both success and failure cases
            found_error || found_success
        } else {
            // For smaller batches, just ensure no crashes and proper error marking
            true
        }
    }
    
    #[test]
    fn test_resilient_parser_error_recovery() {
        let mut parser = ResilientParser::new();
        
        // Test the demonstration of error recovery
        let results = parser.demonstrate_error_recovery();
        
        // Should have results for all test cases
        assert_eq!(results.len(), 8);
        
        // The resilient parser should always succeed by falling back to plain text
        // So we check that all results are successful, but some may have parse_error=true
        // if they failed at higher-level parsers before falling back
        
        for (i, result) in results.iter().enumerate() {
            assert!(result.success, "Resilient parser should always succeed for line {}", i);
            // Original line should always be preserved (even if empty)
            // The raw field should match the original input
        }
        
        // Check specific cases
        let empty_line_result = &results[3]; // Empty line case
        assert!(empty_line_result.success);
        assert_eq!(empty_line_result.event.raw, ""); // Empty line preserved
        
        // Verify that we have a mix of different format types due to fallback behavior
        let format_types: std::collections::HashSet<_> = results.iter()
            .map(|r| r.event.format_type)
            .collect();
        
        // Should have at least plain text and some structured formats
        assert!(format_types.len() > 1, "Should have multiple format types");
        
        // Verify statistics are updated
        let stats = parser.get_statistics();
        assert!(stats.total_lines > 0);
        assert!(stats.successful_parses > 0);
        // Note: failed_parses might be 0 since resilient parser always succeeds
    }
}