use crate::models::*;
use crate::parse_result::ParseResult;
use crate::parsers::{LogParser, JsonParser, LogfmtParser, PatternParser, PlainTextParser};
use crate::classifier::{TangoFormatClassifier, FormatClassifier, FormatCache};
use crate::statistics::{ParsingStatistics, StatisticsMonitor};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::io::{BufRead, BufReader, Read};
use rayon::prelude::*;
use parking_lot::RwLock;
use crossbeam_channel::{bounded, Receiver, Sender};
use serde::{Deserialize, Serialize};

/// Configuration for parallel processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelConfig {
    /// Number of worker threads to use (0 = auto-detect)
    pub num_threads: usize,
    /// Batch size for processing chunks
    pub batch_size: usize,
    /// Buffer size for reading from streams
    pub buffer_size: usize,
    /// Enable shared format cache across threads
    pub enable_shared_cache: bool,
    /// Maximum number of items in work queue
    pub queue_capacity: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            num_threads: 0, // Auto-detect
            batch_size: 1000,
            buffer_size: 64 * 1024, // 64KB
            enable_shared_cache: true,
            queue_capacity: 10000,
        }
    }
}

/// Thread-safe parsing structures for parallel processing
#[derive(Clone)]
pub struct ThreadSafeParsingStructures {
    /// Shared format cache (thread-safe)
    shared_cache: Arc<RwLock<FormatCache>>,
    /// Thread-local parser instances (not shared)
    json_parser: JsonParser,
    logfmt_parser: LogfmtParser,
    pattern_parser: PatternParser,
    plain_text_parser: PlainTextParser,
    /// Thread-local classifier (uses shared cache)
    classifier: TangoFormatClassifier,
}

impl ThreadSafeParsingStructures {
    pub fn new(shared_cache: Arc<RwLock<FormatCache>>) -> Self {
        Self {
            shared_cache,
            json_parser: JsonParser::new(),
            logfmt_parser: LogfmtParser::new(),
            pattern_parser: PatternParser::new(),
            plain_text_parser: PlainTextParser::new(),
            classifier: TangoFormatClassifier::new(),
        }
    }
    
    /// Get the appropriate parser for a format type
    pub fn get_parser(&self, format_type: FormatType) -> &dyn LogParser {
        match format_type {
            FormatType::Json => &self.json_parser,
            FormatType::Logfmt => &self.logfmt_parser,
            FormatType::TimestampLevel | FormatType::Pattern => &self.pattern_parser,
            FormatType::PlainText => &self.plain_text_parser,
            FormatType::Profile(_) => &self.plain_text_parser, // Fallback for profiles
        }
    }
    
    /// Detect format with shared caching
    pub fn detect_format_with_shared_cache(&self, line: &str, source: &str) -> FormatType {
        // Check shared cache first (read lock)
        {
            let mut cache = self.shared_cache.write();
            if let Some(entry) = cache.get(source) {
                return entry.format_type;
            }
        }
        
        // Perform detection if not cached
        let format_type = self.classifier.detect_format(line, source);
        
        // Update shared cache (write lock)
        {
            let mut cache = self.shared_cache.write();
            cache.put(
                source.to_string(),
                format_type,
                0.8,
                None,
                HashMap::new(),
            );
        }
        
        format_type
    }
}

/// Work item for parallel processing
#[derive(Debug, Clone)]
pub struct WorkItem {
    pub line: String,
    pub source: String,
    pub line_number: usize,
}

/// Result from parallel processing
#[derive(Debug)]
pub struct ParallelResult {
    pub results: Vec<ParseResult>,
    pub statistics: ParsingStatistics,
}

/// High-performance parallel log parser
pub struct ParallelParser {
    config: ParallelConfig,
    shared_cache: Arc<RwLock<FormatCache>>,
    global_statistics: Arc<Mutex<StatisticsMonitor>>,
}

impl ParallelParser {
    /// Create a new parallel parser with default configuration
    pub fn new() -> Self {
        Self::with_config(ParallelConfig::default())
    }
    
    /// Create a new parallel parser with custom configuration
    pub fn with_config(config: ParallelConfig) -> Self {
        // Set up thread pool if specified (only if not already initialized)
        if config.num_threads > 0 {
            let _ = rayon::ThreadPoolBuilder::new()
                .num_threads(config.num_threads)
                .build_global(); // Ignore error if already initialized
        }
        
        Self {
            shared_cache: Arc::new(RwLock::new(FormatCache::new())),
            global_statistics: Arc::new(Mutex::new(StatisticsMonitor::new())),
            config,
        }
    }
    
    /// Parse multiple independent log streams in parallel
    pub fn parse_streams_parallel<R: Read + Send + 'static>(
        &self,
        streams: Vec<(R, String)>, // (reader, source_name) pairs
    ) -> Result<Vec<ParallelResult>, std::io::Error> {
        let results: Result<Vec<_>, _> = streams
            .into_par_iter()
            .map(|(reader, source)| {
                self.parse_single_stream(reader, &source)
            })
            .collect();
        
        results
    }
    
    /// Parse a single stream (used internally by parallel processing)
    fn parse_single_stream<R: Read>(
        &self,
        reader: R,
        source: &str,
    ) -> Result<ParallelResult, std::io::Error> {
        let mut buf_reader = BufReader::with_capacity(self.config.buffer_size, reader);
        let mut results = Vec::new();
        let mut line_number = 1;
        let mut local_stats = StatisticsMonitor::new();
        
        // Create thread-safe parsing structures
        let parsing_structures = ThreadSafeParsingStructures::new(self.shared_cache.clone());
        
        loop {
            let mut line = String::new();
            let bytes_read = buf_reader.read_line(&mut line)?;
            
            if bytes_read == 0 {
                break; // End of stream
            }
            
            // Remove trailing newline
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }
            
            // Parse the line
            let result = self.parse_line_with_structures(&parsing_structures, &line, source, line_number);
            
            // Update local statistics
            if result.success {
                if let Some(processing_time) = result.processing_time_micros {
                    local_stats.record_success(result.event.format_type, processing_time);
                } else {
                    local_stats.record_success(result.event.format_type, 0);
                }
            } else {
                if let Some(error) = &result.error {
                    if let Some(processing_time) = result.processing_time_micros {
                        local_stats.record_failure(error, processing_time);
                    } else {
                        local_stats.record_failure(error, 0);
                    }
                }
            }
            
            results.push(result);
            line_number += 1;
        }
        
        Ok(ParallelResult {
            results,
            statistics: local_stats.get_statistics().clone(),
        })
    }
    
    /// Parse lines in parallel using work-stealing
    pub fn parse_lines_parallel(
        &self,
        lines: Vec<String>,
        source: &str,
    ) -> ParallelResult {
        // Create work items
        let work_items: Vec<WorkItem> = lines
            .into_iter()
            .enumerate()
            .map(|(i, line)| WorkItem {
                line,
                source: source.to_string(),
                line_number: i + 1,
            })
            .collect();
        
        // Process in parallel using rayon
        let results: Vec<ParseResult> = work_items
            .into_par_iter()
            .map(|work_item| {
                let parsing_structures = ThreadSafeParsingStructures::new(self.shared_cache.clone());
                self.parse_line_with_structures(
                    &parsing_structures,
                    &work_item.line,
                    &work_item.source,
                    work_item.line_number,
                )
            })
            .collect();
        
        // Aggregate statistics
        let mut aggregated_stats = StatisticsMonitor::new();
        for result in &results {
            if result.success {
                if let Some(processing_time) = result.processing_time_micros {
                    aggregated_stats.record_success(result.event.format_type, processing_time);
                } else {
                    aggregated_stats.record_success(result.event.format_type, 0);
                }
            } else {
                if let Some(error) = &result.error {
                    if let Some(processing_time) = result.processing_time_micros {
                        aggregated_stats.record_failure(error, processing_time);
                    } else {
                        aggregated_stats.record_failure(error, 0);
                    }
                }
            }
        }
        
        ParallelResult {
            results,
            statistics: aggregated_stats.get_statistics().clone(),
        }
    }
    
    /// Parse lines using producer-consumer pattern with bounded queue
    pub fn parse_lines_producer_consumer(
        &self,
        lines: Vec<String>,
        source: &str,
    ) -> Result<ParallelResult, crossbeam_channel::RecvError> {
        let (work_sender, work_receiver): (Sender<WorkItem>, Receiver<WorkItem>) = 
            bounded(self.config.queue_capacity);
        let (result_sender, result_receiver): (Sender<ParseResult>, Receiver<ParseResult>) = 
            bounded(self.config.queue_capacity);
        
        let num_workers = if self.config.num_threads > 0 {
            self.config.num_threads
        } else {
            num_cpus::get()
        };
        
        // Producer thread
        let producer_sender = work_sender.clone();
        let producer_source = source.to_string();
        let producer_handle = thread::spawn(move || {
            for (i, line) in lines.into_iter().enumerate() {
                let work_item = WorkItem {
                    line,
                    source: producer_source.clone(),
                    line_number: i + 1,
                };
                
                if producer_sender.send(work_item).is_err() {
                    break; // Receiver dropped
                }
            }
            // Drop sender to signal end of work
        });
        
        // Worker threads
        let mut worker_handles = Vec::new();
        for _ in 0..num_workers {
            let work_recv = work_receiver.clone();
            let result_send = result_sender.clone();
            let shared_cache = self.shared_cache.clone();
            
            let handle = thread::spawn(move || {
                let parsing_structures = ThreadSafeParsingStructures::new(shared_cache);
                
                while let Ok(work_item) = work_recv.recv() {
                    let result = Self::parse_line_with_structures_static(
                        &parsing_structures,
                        &work_item.line,
                        &work_item.source,
                        work_item.line_number,
                    );
                    
                    if result_send.send(result).is_err() {
                        break; // Receiver dropped
                    }
                }
            });
            
            worker_handles.push(handle);
        }
        
        // Drop the original senders so workers know when to stop
        drop(work_sender);
        drop(result_sender);
        
        // Collector thread
        let mut results = Vec::new();
        let mut aggregated_stats = StatisticsMonitor::new();
        
        while let Ok(result) = result_receiver.recv() {
            // Update statistics
            if result.success {
                if let Some(processing_time) = result.processing_time_micros {
                    aggregated_stats.record_success(result.event.format_type, processing_time);
                } else {
                    aggregated_stats.record_success(result.event.format_type, 0);
                }
            } else {
                if let Some(error) = &result.error {
                    if let Some(processing_time) = result.processing_time_micros {
                        aggregated_stats.record_failure(error, processing_time);
                    } else {
                        aggregated_stats.record_failure(error, 0);
                    }
                }
            }
            
            results.push(result);
        }
        
        // Wait for all threads to complete
        producer_handle.join().expect("Producer thread panicked");
        for handle in worker_handles {
            handle.join().expect("Worker thread panicked");
        }
        
        Ok(ParallelResult {
            results,
            statistics: aggregated_stats.get_statistics().clone(),
        })
    }
    
    /// Parse a single line with given parsing structures (instance method)
    fn parse_line_with_structures(
        &self,
        parsing_structures: &ThreadSafeParsingStructures,
        line: &str,
        source: &str,
        line_number: usize,
    ) -> ParseResult {
        Self::parse_line_with_structures_static(parsing_structures, line, source, line_number)
    }
    
    /// Parse a single line with given parsing structures (static method for thread safety)
    fn parse_line_with_structures_static(
        parsing_structures: &ThreadSafeParsingStructures,
        line: &str,
        source: &str,
        line_number: usize,
    ) -> ParseResult {
        let start_time = std::time::Instant::now();
        
        // Check shared cache first
        {
            let mut cache = parsing_structures.shared_cache.write();
            if let Some(entry) = cache.get(source) {
                // Use cached format type to get the appropriate parser
                let parser = parsing_structures.get_parser(entry.format_type);
                let cached_result = parser.parse(line);
                if cached_result.success {
                    let mut result = cached_result.with_line_number(line_number);
                    let processing_time = start_time.elapsed().as_micros() as u64;
                    result.processing_time_micros = Some(processing_time);
                    return result;
                }
                // If cached parser fails, continue with fallback chain
            }
        }
        
        // Implement the fallback chain: JSON → logfmt → timestamp patterns → plain text
        
        // Stage 1: Try JSON parsing first
        if line.trim_start().starts_with('{') {
            let json_result = parsing_structures.json_parser.parse(line);
            if json_result.success {
                // Update shared cache with successful detection
                {
                    let mut cache = parsing_structures.shared_cache.write();
                    cache.put(
                        source.to_string(),
                        FormatType::Json,
                        json_result.confidence,
                        None,
                        HashMap::new(),
                    );
                }
                
                let mut result = json_result.with_line_number(line_number);
                let processing_time = start_time.elapsed().as_micros() as u64;
                result.processing_time_micros = Some(processing_time);
                return result;
            }
        }
        
        // Stage 2: Try logfmt parsing
        if parsing_structures.logfmt_parser.can_parse(line) {
            let logfmt_result = parsing_structures.logfmt_parser.parse(line);
            if logfmt_result.success {
                // Update shared cache with successful detection
                {
                    let mut cache = parsing_structures.shared_cache.write();
                    cache.put(
                        source.to_string(),
                        FormatType::Logfmt,
                        logfmt_result.confidence,
                        None,
                        HashMap::new(),
                    );
                }
                
                let mut result = logfmt_result.with_line_number(line_number);
                let processing_time = start_time.elapsed().as_micros() as u64;
                result.processing_time_micros = Some(processing_time);
                return result;
            }
        }
        
        // Stage 3: Try timestamp+level pattern parsing
        if parsing_structures.pattern_parser.can_parse(line) {
            let pattern_result = parsing_structures.pattern_parser.parse(line);
            if pattern_result.success {
                // Update shared cache with successful detection
                {
                    let mut cache = parsing_structures.shared_cache.write();
                    cache.put(
                        source.to_string(),
                        FormatType::TimestampLevel,
                        pattern_result.confidence,
                        None,
                        HashMap::new(),
                    );
                }
                
                let mut result = pattern_result.with_line_number(line_number);
                let processing_time = start_time.elapsed().as_micros() as u64;
                result.processing_time_micros = Some(processing_time);
                return result;
            }
        }
        
        // Stage 4: Fall back to plain text (always succeeds)
        let plain_result = parsing_structures.plain_text_parser.parse(line);
        
        // Update shared cache with plain text fallback
        {
            let mut cache = parsing_structures.shared_cache.write();
            cache.put(
                source.to_string(),
                FormatType::PlainText,
                plain_result.confidence,
                None,
                HashMap::new(),
            );
        }
        
        let mut result = plain_result.with_line_number(line_number);
        let processing_time = start_time.elapsed().as_micros() as u64;
        result.processing_time_micros = Some(processing_time);
        result
    }
    
    /// Get shared cache statistics
    pub fn get_cache_stats(&self) -> crate::classifier::CacheStats {
        let cache = self.shared_cache.read();
        cache.stats()
    }
    
    /// Get global statistics (aggregated from all threads)
    pub fn get_global_statistics(&self) -> ParsingStatistics {
        let stats_monitor = self.global_statistics.lock().unwrap();
        stats_monitor.get_statistics().clone()
    }
    
    /// Clear shared cache
    pub fn clear_shared_cache(&self) {
        let mut cache = self.shared_cache.write();
        cache.clear();
    }
    
    /// Get current configuration
    pub fn get_config(&self) -> &ParallelConfig {
        &self.config
    }
    
    /// Update configuration (note: thread pool changes require restart)
    pub fn update_config(&mut self, config: ParallelConfig) {
        self.config = config;
    }
}

impl Default for ParallelParser {
    fn default() -> Self {
        Self::new()
    }
}

// Add num_cpus as a dependency helper (we'll add this to Cargo.toml)
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4) // Fallback to 4 threads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_parallel_parser_creation() {
        let parser = ParallelParser::new();
        let config = parser.get_config();
        
        assert_eq!(config.num_threads, 0); // Auto-detect
        assert_eq!(config.batch_size, 1000);
        assert!(config.enable_shared_cache);
    }
    
    #[test]
    fn test_parallel_parser_with_custom_config() {
        let config = ParallelConfig {
            num_threads: 2,
            batch_size: 500,
            buffer_size: 32 * 1024,
            enable_shared_cache: false,
            queue_capacity: 5000,
        };
        
        let parser = ParallelParser::with_config(config.clone());
        let parser_config = parser.get_config();
        
        assert_eq!(parser_config.num_threads, 2);
        assert_eq!(parser_config.batch_size, 500);
        assert_eq!(parser_config.buffer_size, 32 * 1024);
        assert!(!parser_config.enable_shared_cache);
        assert_eq!(parser_config.queue_capacity, 5000);
    }
    
    #[test]
    fn test_parallel_lines_processing() {
        let parser = ParallelParser::new();
        
        let lines = vec![
            r#"{"message": "First log", "level": "INFO"}"#.to_string(),
            "level=WARN msg=Second user=admin".to_string(),
            "[2025-12-29T10:21:03Z] [ERROR] Third log message".to_string(),
            "Plain text fourth log".to_string(),
            r#"{"message": "Fifth log", "level": "DEBUG"}"#.to_string(),
        ];
        
        let result = parser.parse_lines_parallel(lines.clone(), "test.log");
        
        assert_eq!(result.results.len(), 5);
        
        // Verify all parsing succeeded
        for (i, parse_result) in result.results.iter().enumerate() {
            assert!(parse_result.success, "Parsing failed on line {}: {:?}", i + 1, parse_result.error);
            assert_eq!(parse_result.line_number, Some(i + 1));
        }
        
        // Verify format detection
        assert_eq!(result.results[0].event.format_type, FormatType::Json);
        assert_eq!(result.results[1].event.format_type, FormatType::Logfmt);
        assert!(matches!(result.results[2].event.format_type, FormatType::TimestampLevel | FormatType::Pattern));
        assert_eq!(result.results[3].event.format_type, FormatType::PlainText);
        assert_eq!(result.results[4].event.format_type, FormatType::Json);
        
        // Verify statistics
        assert_eq!(result.statistics.total_lines, 5);
        assert_eq!(result.statistics.successful_parses, 5);
        assert_eq!(result.statistics.failed_parses, 0);
    }
    
    #[test]
    fn test_parallel_streams_processing() {
        let parser = ParallelParser::new();
        
        // Create multiple streams
        let stream1_data = r#"{"message": "Stream 1 log 1", "level": "INFO"}
{"message": "Stream 1 log 2", "level": "ERROR"}"#;
        
        let stream2_data = r#"level=INFO msg="Stream 2 log 1" user=alice
level=WARN msg="Stream 2 log 2" user=bob"#;
        
        let streams = vec![
            (Cursor::new(stream1_data), "stream1.log".to_string()),
            (Cursor::new(stream2_data), "stream2.log".to_string()),
        ];
        
        let results = parser.parse_streams_parallel(streams).unwrap();
        
        assert_eq!(results.len(), 2);
        
        // Verify stream 1 results
        let stream1_result = &results[0];
        assert_eq!(stream1_result.results.len(), 2);
        assert!(stream1_result.results.iter().all(|r| r.success));
        assert!(stream1_result.results.iter().all(|r| r.event.format_type == FormatType::Json));
        
        // Verify stream 2 results
        let stream2_result = &results[1];
        assert_eq!(stream2_result.results.len(), 2);
        assert!(stream2_result.results.iter().all(|r| r.success));
        assert!(stream2_result.results.iter().all(|r| r.event.format_type == FormatType::Logfmt));
    }
    
    #[test]
    fn test_producer_consumer_processing() {
        let parser = ParallelParser::with_config(ParallelConfig {
            num_threads: 2,
            queue_capacity: 100,
            ..Default::default()
        });
        
        let lines = vec![
            r#"{"message": "Test 1", "level": "INFO"}"#.to_string(),
            r#"{"message": "Test 2", "level": "ERROR"}"#.to_string(),
            r#"{"message": "Test 3", "level": "WARN"}"#.to_string(),
        ];
        
        let result = parser.parse_lines_producer_consumer(lines.clone(), "producer_test.log").unwrap();
        
        assert_eq!(result.results.len(), 3);
        
        // Results might be in different order due to parallel processing
        // So we just verify all succeeded and have correct content
        assert!(result.results.iter().all(|r| r.success));
        assert!(result.results.iter().all(|r| r.event.format_type == FormatType::Json));
        
        // Verify statistics
        assert_eq!(result.statistics.total_lines, 3);
        assert_eq!(result.statistics.successful_parses, 3);
    }
    
    #[test]
    fn test_thread_safe_parsing_structures() {
        let shared_cache = Arc::new(RwLock::new(FormatCache::new()));
        let structures = ThreadSafeParsingStructures::new(shared_cache.clone());
        
        // Test format detection with caching
        let json_line = r#"{"message": "test", "level": "INFO"}"#;
        let format1 = structures.detect_format_with_shared_cache(json_line, "test.log");
        let format2 = structures.detect_format_with_shared_cache(json_line, "test.log");
        
        assert_eq!(format1, FormatType::Json);
        assert_eq!(format2, FormatType::Json);
        
        // Verify cache was used (should have entry now)
        let cache = shared_cache.read();
        let stats = cache.stats();
        assert!(stats.entries > 0);
    }
    
    #[test]
    fn test_shared_cache_thread_safety() {
        let parser = ParallelParser::new();
        
        // Create multiple lines that will use the same source
        let lines: Vec<String> = (0..100)
            .map(|i| format!(r#"{{"message": "Log {}", "level": "INFO"}}"#, i))
            .collect();
        
        let result = parser.parse_lines_parallel(lines, "cache_test.log");
        
        assert_eq!(result.results.len(), 100);
        assert!(result.results.iter().all(|r| r.success));
        
        // Verify cache was populated
        let cache_stats = parser.get_cache_stats();
        assert!(cache_stats.entries > 0);
        assert!(cache_stats.cache_hits > 0); // Should have cache hits due to same source
    }
    
    #[test]
    fn test_parallel_error_handling() {
        let parser = ParallelParser::new();
        
        let lines = vec![
            r#"{"valid": "json"}"#.to_string(),
            r#"{"invalid": json"#.to_string(), // Malformed JSON
            "valid logfmt key=value msg=test user=admin".to_string(),
            "insufficient=pairs".to_string(), // Insufficient logfmt pairs
            "Plain text line".to_string(),
        ];
        
        let result = parser.parse_lines_parallel(lines, "error_test.log");
        
        assert_eq!(result.results.len(), 5);
        
        // All should succeed due to fallback to plain text
        for (i, parse_result) in result.results.iter().enumerate() {
            assert!(parse_result.success, "Line {} should succeed with fallback: {:?}", i + 1, parse_result.error);
        }
        
        // Verify format types
        assert_eq!(result.results[0].event.format_type, FormatType::Json);
        // Malformed JSON should fall back to plain text
        assert_eq!(result.results[1].event.format_type, FormatType::PlainText);
        assert_eq!(result.results[2].event.format_type, FormatType::Logfmt);
        // Insufficient logfmt should fall back to plain text
        assert_eq!(result.results[3].event.format_type, FormatType::PlainText);
        assert_eq!(result.results[4].event.format_type, FormatType::PlainText);
    }
}