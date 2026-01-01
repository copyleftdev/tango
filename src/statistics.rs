use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::models::FormatType;
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

/// Statistics monitor for real-time monitoring and debugging
#[derive(Debug, Clone)]
pub struct StatisticsMonitor {
    stats: ParsingStatistics,
    monitoring_enabled: bool,
    debug_output_enabled: bool,
    report_interval: usize, // Report every N lines
    last_report_line: usize,
}

impl StatisticsMonitor {
    /// Create a new statistics monitor
    pub fn new() -> Self {
        Self {
            stats: ParsingStatistics::new(),
            monitoring_enabled: false,
            debug_output_enabled: false,
            report_interval: 1000, // Default: report every 1000 lines
            last_report_line: 0,
        }
    }
    
    /// Create a new statistics monitor with custom settings
    pub fn with_settings(monitoring_enabled: bool, debug_output_enabled: bool, report_interval: usize) -> Self {
        Self {
            stats: ParsingStatistics::new(),
            monitoring_enabled,
            debug_output_enabled,
            report_interval,
            last_report_line: 0,
        }
    }
    
    /// Enable or disable monitoring
    pub fn set_monitoring_enabled(&mut self, enabled: bool) {
        self.monitoring_enabled = enabled;
    }
    
    /// Enable or disable debug output
    pub fn set_debug_output_enabled(&mut self, enabled: bool) {
        self.debug_output_enabled = enabled;
    }
    
    /// Set the report interval (number of lines between reports)
    pub fn set_report_interval(&mut self, interval: usize) {
        self.report_interval = interval;
    }
    
    /// Record a successful parse with optional monitoring output
    pub fn record_success(&mut self, format_type: FormatType, processing_time_micros: u64) {
        self.stats.record_success(format_type, processing_time_micros);
        
        if self.debug_output_enabled {
            println!("DEBUG: Successful parse - Format: {:?}, Time: {}μs", format_type, processing_time_micros);
        }
        
        self.check_and_report();
    }
    
    /// Record a failed parse with optional monitoring output
    pub fn record_failure(&mut self, error: &ParseError, processing_time_micros: u64) {
        self.stats.record_failure(error, processing_time_micros);
        
        if self.debug_output_enabled {
            println!("DEBUG: Parse failure - Error: {}, Time: {}μs", error, processing_time_micros);
        }
        
        self.check_and_report();
    }
    
    /// Record a plain text fallback with optional monitoring output
    pub fn record_plain_text_fallback(&mut self, processing_time_micros: u64) {
        self.stats.record_plain_text_fallback(processing_time_micros);
        
        if self.debug_output_enabled {
            println!("DEBUG: Plain text fallback - Time: {}μs", processing_time_micros);
        }
        
        self.check_and_report();
    }
    
    /// Get the current statistics
    pub fn get_statistics(&self) -> &ParsingStatistics {
        &self.stats
    }
    
    /// Get mutable access to statistics
    pub fn get_statistics_mut(&mut self) -> &mut ParsingStatistics {
        &mut self.stats
    }
    
    /// Reset all statistics
    pub fn reset(&mut self) {
        self.stats = ParsingStatistics::new();
        self.last_report_line = 0;
    }
    
    /// Generate a comprehensive monitoring report
    pub fn generate_report(&self) -> String {
        let stats = &self.stats;
        let mut report = String::new();
        
        report.push_str("=== Parsing Statistics Report ===\n");
        report.push_str(&format!("Total lines processed: {}\n", stats.total_lines));
        report.push_str(&format!("Successful parses: {} ({:.2}%)\n", stats.successful_parses, stats.success_rate()));
        report.push_str(&format!("Failed parses: {} ({:.2}%)\n", stats.failed_parses, stats.error_rate()));
        report.push_str(&format!("Plain text fallbacks: {} ({:.2}%)\n", stats.plain_text_fallbacks, stats.fallback_rate()));
        
        report.push_str("\n--- Format Distribution ---\n");
        for (format_type, count) in &stats.format_distribution {
            let percentage = (*count as f64 / stats.total_lines as f64) * 100.0;
            report.push_str(&format!("{:?}: {} ({:.2}%)\n", format_type, count, percentage));
        }
        
        report.push_str("\n--- Error Distribution ---\n");
        for (error_type, count) in &stats.error_distribution {
            let percentage = (*count as f64 / stats.failed_parses as f64) * 100.0;
            report.push_str(&format!("{}: {} ({:.2}%)\n", error_type, count, percentage));
        }
        
        report.push_str("\n--- Performance Metrics ---\n");
        report.push_str(&format!("Total processing time: {}μs\n", stats.processing_time_micros.total_time));
        report.push_str(&format!("Average processing time: {:.2}μs\n", stats.processing_time_micros.avg_time));
        report.push_str(&format!("Min processing time: {}μs\n", stats.processing_time_micros.min_time));
        report.push_str(&format!("Max processing time: {}μs\n", stats.processing_time_micros.max_time));
        
        if stats.total_lines > 0 {
            let throughput = stats.total_lines as f64 / (stats.processing_time_micros.total_time as f64 / 1_000_000.0);
            report.push_str(&format!("Throughput: {:.2} lines/second\n", throughput));
        }
        
        report.push_str("\n--- Memory Usage ---\n");
        report.push_str(&format!("Peak memory: {} bytes\n", stats.memory_stats.peak_memory_bytes));
        report.push_str(&format!("Current memory: {} bytes\n", stats.memory_stats.current_memory_bytes));
        report.push_str(&format!("Total allocations: {}\n", stats.memory_stats.total_allocations));
        
        report
    }
    
    /// Print a monitoring report to stdout
    pub fn print_report(&self) {
        println!("{}", self.generate_report());
    }
    
    /// Generate a compact status line for continuous monitoring
    pub fn generate_status_line(&self) -> String {
        let stats = &self.stats;
        format!(
            "Lines: {} | Success: {:.1}% | Errors: {:.1}% | Fallbacks: {:.1}% | Avg Time: {:.1}μs",
            stats.total_lines,
            stats.success_rate(),
            stats.error_rate(),
            stats.fallback_rate(),
            stats.processing_time_micros.avg_time
        )
    }
    
    /// Print a compact status line
    pub fn print_status_line(&self) {
        println!("{}", self.generate_status_line());
    }
    
    /// Check if it's time to report and generate a report if monitoring is enabled
    fn check_and_report(&mut self) {
        if !self.monitoring_enabled {
            return;
        }
        
        let lines_since_last_report = self.stats.total_lines - self.last_report_line;
        
        if lines_since_last_report >= self.report_interval {
            println!("MONITOR: {}", self.generate_status_line());
            self.last_report_line = self.stats.total_lines;
        }
    }
    
    /// Update memory statistics (to be called by memory tracking systems)
    pub fn update_memory_stats(&mut self, current_bytes: usize, peak_bytes: usize, allocations: usize) {
        self.stats.memory_stats.current_memory_bytes = current_bytes;
        if peak_bytes > self.stats.memory_stats.peak_memory_bytes {
            self.stats.memory_stats.peak_memory_bytes = peak_bytes;
        }
        self.stats.memory_stats.total_allocations = allocations;
    }
    
    /// Get performance summary for alerting/monitoring systems
    pub fn get_performance_summary(&self) -> PerformanceSummary {
        let stats = &self.stats;
        PerformanceSummary {
            total_lines: stats.total_lines,
            success_rate: stats.success_rate(),
            error_rate: stats.error_rate(),
            fallback_rate: stats.fallback_rate(),
            avg_processing_time_micros: stats.processing_time_micros.avg_time,
            throughput_lines_per_second: if stats.processing_time_micros.total_time > 0 {
                stats.total_lines as f64 / (stats.processing_time_micros.total_time as f64 / 1_000_000.0)
            } else {
                0.0
            },
            peak_memory_bytes: stats.memory_stats.peak_memory_bytes,
            most_common_format: self.get_most_common_format(),
            most_common_error: self.get_most_common_error(),
        }
    }
    
    /// Get the most commonly detected format
    fn get_most_common_format(&self) -> Option<FormatType> {
        self.stats.format_distribution
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&format_type, _)| format_type)
    }
    
    /// Get the most common error type
    fn get_most_common_error(&self) -> Option<String> {
        self.stats.error_distribution
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(error_type, _)| error_type.clone())
    }
}

impl Default for StatisticsMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance summary for monitoring systems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub total_lines: usize,
    pub success_rate: f64,
    pub error_rate: f64,
    pub fallback_rate: f64,
    pub avg_processing_time_micros: f64,
    pub throughput_lines_per_second: f64,
    pub peak_memory_bytes: usize,
    pub most_common_format: Option<FormatType>,
    pub most_common_error: Option<String>,
}

impl PerformanceSummary {
    /// Check if performance metrics indicate potential issues
    pub fn has_performance_issues(&self) -> bool {
        self.error_rate > 10.0 || // More than 10% errors
        self.avg_processing_time_micros > 10000.0 || // More than 10ms per line
        self.throughput_lines_per_second < 100.0 // Less than 100 lines/second
    }
    
    /// Get performance status as a string
    pub fn get_status(&self) -> &'static str {
        if self.has_performance_issues() {
            "WARNING"
        } else if self.error_rate > 5.0 || self.avg_processing_time_micros > 5000.0 {
            "CAUTION"
        } else {
            "HEALTHY"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::FormatType;
    use crate::error::ParseError;
    
    #[test]
    fn test_statistics_monitor_creation() {
        let monitor = StatisticsMonitor::new();
        assert_eq!(monitor.get_statistics().total_lines, 0);
        assert!(!monitor.monitoring_enabled);
        assert!(!monitor.debug_output_enabled);
        assert_eq!(monitor.report_interval, 1000);
    }
    
    #[test]
    fn test_statistics_monitor_with_settings() {
        let monitor = StatisticsMonitor::with_settings(true, true, 500);
        assert!(monitor.monitoring_enabled);
        assert!(monitor.debug_output_enabled);
        assert_eq!(monitor.report_interval, 500);
    }
    
    #[test]
    fn test_record_success() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_success(FormatType::Json, 1000);
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.total_lines, 1);
        assert_eq!(stats.successful_parses, 1);
        assert_eq!(stats.failed_parses, 0);
        assert_eq!(stats.format_distribution[&FormatType::Json], 1);
    }
    
    #[test]
    fn test_record_failure() {
        let mut monitor = StatisticsMonitor::new();
        let error = ParseError::JsonSyntaxError {
            message: "test error".to_string(),
            line_number: Some(1),
            column: Some(5),
        };
        monitor.record_failure(&error, 2000);
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.total_lines, 1);
        assert_eq!(stats.successful_parses, 0);
        assert_eq!(stats.failed_parses, 1);
        assert_eq!(stats.error_distribution["JsonSyntaxError"], 1);
    }
    
    #[test]
    fn test_record_plain_text_fallback() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_plain_text_fallback(500);
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.total_lines, 1);
        assert_eq!(stats.successful_parses, 1);
        assert_eq!(stats.plain_text_fallbacks, 1);
        assert_eq!(stats.format_distribution[&FormatType::PlainText], 1);
    }
    
    #[test]
    fn test_generate_report() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_success(FormatType::Json, 1000);
        monitor.record_success(FormatType::Logfmt, 1500);
        
        let report = monitor.generate_report();
        assert!(report.contains("Total lines processed: 2"));
        assert!(report.contains("Successful parses: 2"));
        assert!(report.contains("Format Distribution"));
        assert!(report.contains("Performance Metrics"));
    }
    
    #[test]
    fn test_generate_status_line() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_success(FormatType::Json, 1000);
        
        let status = monitor.generate_status_line();
        assert!(status.contains("Lines: 1"));
        assert!(status.contains("Success: 100.0%"));
        assert!(status.contains("Errors: 0.0%"));
    }
    
    #[test]
    fn test_performance_summary() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_success(FormatType::Json, 1000);
        monitor.record_success(FormatType::Logfmt, 2000);
        
        let summary = monitor.get_performance_summary();
        assert_eq!(summary.total_lines, 2);
        assert_eq!(summary.success_rate, 100.0);
        assert_eq!(summary.error_rate, 0.0);
        assert_eq!(summary.most_common_format, Some(FormatType::Json)); // First one recorded
        assert_eq!(summary.get_status(), "HEALTHY");
    }
    
    #[test]
    fn test_performance_issues_detection() {
        let summary = PerformanceSummary {
            total_lines: 100,
            success_rate: 80.0,
            error_rate: 15.0, // High error rate
            fallback_rate: 5.0,
            avg_processing_time_micros: 1000.0,
            throughput_lines_per_second: 500.0,
            peak_memory_bytes: 1024,
            most_common_format: Some(FormatType::Json),
            most_common_error: Some("JsonSyntaxError".to_string()),
        };
        
        assert!(summary.has_performance_issues());
        assert_eq!(summary.get_status(), "WARNING");
    }
    
    #[test]
    fn test_reset_statistics() {
        let mut monitor = StatisticsMonitor::new();
        monitor.record_success(FormatType::Json, 1000);
        assert_eq!(monitor.get_statistics().total_lines, 1);
        
        monitor.reset();
        assert_eq!(monitor.get_statistics().total_lines, 0);
        assert_eq!(monitor.last_report_line, 0);
    }
    
    #[test]
    fn test_memory_stats_update() {
        let mut monitor = StatisticsMonitor::new();
        monitor.update_memory_stats(1024, 2048, 10);
        
        let stats = monitor.get_statistics();
        assert_eq!(stats.memory_stats.current_memory_bytes, 1024);
        assert_eq!(stats.memory_stats.peak_memory_bytes, 2048);
        assert_eq!(stats.memory_stats.total_allocations, 10);
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use crate::models::FormatType;
    use crate::error::ParseError;
    
    // Property 10: Statistics and Monitoring Completeness
    // Feature: log-type-detection-and-parsing, Property 10: Statistics and Monitoring Completeness
    // Validates: Requirements 6.6, 8.5
    #[quickcheck(tests = 100)]
    fn prop_statistics_completeness(
        success_events: Vec<(FormatType, u64)>, // (format_type, processing_time)
        error_events: Vec<String>, // error messages
        fallback_events: Vec<u64>, // processing times for fallbacks
    ) -> bool {
        // For any parsing session, the system should maintain comprehensive statistics
        // including success rates, error counts, and format distribution for monitoring
        // and debugging purposes
        
        let mut monitor = StatisticsMonitor::new();
        
        // Track expected totals for verification
        let mut expected_total_lines = 0;
        let mut expected_successful_parses = 0;
        let mut expected_failed_parses = 0;
        let mut expected_fallbacks = 0;
        let mut expected_format_distribution: HashMap<FormatType, usize> = HashMap::new();
        let mut expected_error_distribution: HashMap<String, usize> = HashMap::new();
        
        // Record success events
        for (format_type, processing_time) in success_events.iter().take(50) { // Limit to prevent excessive test time
            monitor.record_success(*format_type, *processing_time);
            expected_total_lines += 1;
            expected_successful_parses += 1;
            *expected_format_distribution.entry(*format_type).or_insert(0) += 1;
        }
        
        // Record error events
        for error_msg in error_events.iter().take(20) { // Limit to prevent excessive test time
            let error = ParseError::GenericError {
                message: error_msg.clone(),
                context: HashMap::new(),
            };
            monitor.record_failure(&error, 1000);
            expected_total_lines += 1;
            expected_failed_parses += 1;
            *expected_error_distribution.entry("GenericError".to_string()).or_insert(0) += 1;
        }
        
        // Record fallback events
        for processing_time in fallback_events.iter().take(30) { // Limit to prevent excessive test time
            monitor.record_plain_text_fallback(*processing_time);
            expected_total_lines += 1;
            expected_successful_parses += 1;
            expected_fallbacks += 1;
            *expected_format_distribution.entry(FormatType::PlainText).or_insert(0) += 1;
        }
        
        let stats = monitor.get_statistics();
        
        // Verify comprehensive statistics are maintained
        
        // 1. Total line count should match all recorded events
        if stats.total_lines != expected_total_lines {
            return false;
        }
        
        // 2. Success and failure counts should be accurate
        if stats.successful_parses != expected_successful_parses {
            return false;
        }
        
        if stats.failed_parses != expected_failed_parses {
            return false;
        }
        
        if stats.plain_text_fallbacks != expected_fallbacks {
            return false;
        }
        
        // 3. Format distribution should be complete and accurate
        for (format_type, expected_count) in &expected_format_distribution {
            if stats.format_distribution.get(format_type).unwrap_or(&0) != expected_count {
                return false;
            }
        }
        
        // 4. Error distribution should be complete and accurate
        for (error_type, expected_count) in &expected_error_distribution {
            if stats.error_distribution.get(error_type).unwrap_or(&0) != expected_count {
                return false;
            }
        }
        
        // 5. Success rate calculation should be correct
        let expected_success_rate = if expected_total_lines == 0 {
            0.0
        } else {
            (expected_successful_parses as f64 / expected_total_lines as f64) * 100.0
        };
        
        if (stats.success_rate() - expected_success_rate).abs() > 0.01 {
            return false;
        }
        
        // 6. Error rate calculation should be correct
        let expected_error_rate = if expected_total_lines == 0 {
            0.0
        } else {
            (expected_failed_parses as f64 / expected_total_lines as f64) * 100.0
        };
        
        if (stats.error_rate() - expected_error_rate).abs() > 0.01 {
            return false;
        }
        
        // 7. Fallback rate calculation should be correct
        let expected_fallback_rate = if expected_total_lines == 0 {
            0.0
        } else {
            (expected_fallbacks as f64 / expected_total_lines as f64) * 100.0
        };
        
        if (stats.fallback_rate() - expected_fallback_rate).abs() > 0.01 {
            return false;
        }
        
        // 8. Processing time statistics should be maintained
        if expected_total_lines > 0 {
            // Should have non-zero total time (unless all processing times were 0)
            if stats.processing_time_micros.total_time == 0 && 
               (success_events.iter().any(|(_, time)| *time > 0) || 
                fallback_events.iter().any(|time| *time > 0)) {
                return false;
            }
            
            // Average time should be calculated correctly
            let expected_avg = stats.processing_time_micros.total_time as f64 / expected_total_lines as f64;
            if (stats.processing_time_micros.avg_time - expected_avg).abs() > 0.01 {
                return false;
            }
        }
        
        // 9. Performance summary should be available and consistent
        let summary = monitor.get_performance_summary();
        if summary.total_lines != expected_total_lines {
            return false;
        }
        
        if (summary.success_rate - expected_success_rate).abs() > 0.01 {
            return false;
        }
        
        if (summary.error_rate - expected_error_rate).abs() > 0.01 {
            return false;
        }
        
        // 10. Monitoring reports should be generatable without errors
        let report = monitor.generate_report();
        if report.is_empty() {
            return false;
        }
        
        let status_line = monitor.generate_status_line();
        if status_line.is_empty() {
            return false;
        }
        
        // All statistics should be comprehensive and accurate
        true
    }
    
    #[quickcheck(tests = 50)]
    fn prop_statistics_reset_completeness(_: ()) -> bool {
        // Statistics reset should completely clear all tracked data
        let mut monitor = StatisticsMonitor::new();
        
        // Add some data
        monitor.record_success(FormatType::Json, 1000);
        monitor.record_failure(&ParseError::GenericError {
            message: "test".to_string(),
            context: HashMap::new(),
        }, 2000);
        monitor.record_plain_text_fallback(500);
        
        // Verify data exists
        let stats_before = monitor.get_statistics();
        if stats_before.total_lines == 0 {
            return false;
        }
        
        // Reset
        monitor.reset();
        
        // Verify complete reset
        let stats_after = monitor.get_statistics();
        stats_after.total_lines == 0 &&
        stats_after.successful_parses == 0 &&
        stats_after.failed_parses == 0 &&
        stats_after.plain_text_fallbacks == 0 &&
        stats_after.format_distribution.is_empty() &&
        stats_after.error_distribution.is_empty() &&
        stats_after.processing_time_micros.total_time == 0 &&
        stats_after.processing_time_micros.min_time == 0 &&
        stats_after.processing_time_micros.max_time == 0 &&
        stats_after.processing_time_micros.avg_time == 0.0
    }
    
    #[quickcheck(tests = 50)]
    fn prop_statistics_memory_tracking(_: ()) -> bool {
        // Memory statistics should be trackable and updateable
        let mut monitor = StatisticsMonitor::new();
        
        // Initial memory stats should be zero
        let initial_stats = monitor.get_statistics();
        if initial_stats.memory_stats.current_memory_bytes != 0 ||
           initial_stats.memory_stats.peak_memory_bytes != 0 ||
           initial_stats.memory_stats.total_allocations != 0 {
            return false;
        }
        
        // Update memory stats
        monitor.update_memory_stats(1024, 2048, 10);
        
        let updated_stats = monitor.get_statistics();
        updated_stats.memory_stats.current_memory_bytes == 1024 &&
        updated_stats.memory_stats.peak_memory_bytes == 2048 &&
        updated_stats.memory_stats.total_allocations == 10
    }
}