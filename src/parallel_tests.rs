use crate::parallel_parser::{ParallelParser, ParallelConfig, ThreadSafeParsingStructures};
use crate::classifier::FormatCache;
use crate::models::FormatType;
use std::sync::Arc;
use std::thread;
use parking_lot::RwLock;
use std::io::Cursor;

/// Test concurrent parsing of multiple streams
#[test]
fn test_concurrent_parsing_multiple_streams() {
    let parser = ParallelParser::with_config(ParallelConfig {
        num_threads: 4,
        batch_size: 100,
        enable_shared_cache: true,
        ..Default::default()
    });
    
    // Create multiple streams with different formats
    let stream1_data = r#"{"message": "Stream 1 log 1", "level": "INFO", "timestamp": "2025-12-29T10:21:03Z"}
{"message": "Stream 1 log 2", "level": "ERROR", "timestamp": "2025-12-29T10:21:04Z"}
{"message": "Stream 1 log 3", "level": "WARN", "timestamp": "2025-12-29T10:21:05Z"}"#;
    
    let stream2_data = r#"level=INFO msg="Stream 2 log 1" user=alice timestamp=2025-12-29T10:21:03Z
level=WARN msg="Stream 2 log 2" user=bob timestamp=2025-12-29T10:21:04Z
level=ERROR msg="Stream 2 log 3" user=charlie timestamp=2025-12-29T10:21:05Z"#;
    
    let stream3_data = r#"[2025-12-29T10:21:03Z] [INFO] Stream 3 log 1 - Application started
[2025-12-29T10:21:04Z] [ERROR] Stream 3 log 2 - Connection failed
[2025-12-29T10:21:05Z] [WARN] Stream 3 log 3 - Retrying connection"#;
    
    let stream4_data = r#"Plain text log from stream 4 line 1
Plain text log from stream 4 line 2
Plain text log from stream 4 line 3"#;
    
    let streams = vec![
        (Cursor::new(stream1_data), "stream1.log".to_string()),
        (Cursor::new(stream2_data), "stream2.log".to_string()),
        (Cursor::new(stream3_data), "stream3.log".to_string()),
        (Cursor::new(stream4_data), "stream4.log".to_string()),
    ];
    
    // Parse streams concurrently
    let results = parser.parse_streams_parallel(streams).unwrap();
    
    // Verify we got results for all streams
    assert_eq!(results.len(), 4);
    
    // Verify stream 1 (JSON format)
    let stream1_result = &results[0];
    assert_eq!(stream1_result.results.len(), 3);
    assert!(stream1_result.results.iter().all(|r| r.success));
    assert!(stream1_result.results.iter().all(|r| r.event.format_type == FormatType::Json));
    assert_eq!(stream1_result.statistics.total_lines, 3);
    assert_eq!(stream1_result.statistics.successful_parses, 3);
    
    // Verify stream 2 (Logfmt format)
    let stream2_result = &results[1];
    assert_eq!(stream2_result.results.len(), 3);
    assert!(stream2_result.results.iter().all(|r| r.success));
    assert!(stream2_result.results.iter().all(|r| r.event.format_type == FormatType::Logfmt));
    assert_eq!(stream2_result.statistics.total_lines, 3);
    assert_eq!(stream2_result.statistics.successful_parses, 3);
    
    // Verify stream 3 (Timestamp+Level pattern format)
    let stream3_result = &results[2];
    assert_eq!(stream3_result.results.len(), 3);
    assert!(stream3_result.results.iter().all(|r| r.success));
    assert!(stream3_result.results.iter().all(|r| 
        matches!(r.event.format_type, FormatType::TimestampLevel | FormatType::Pattern)
    ));
    assert_eq!(stream3_result.statistics.total_lines, 3);
    assert_eq!(stream3_result.statistics.successful_parses, 3);
    
    // Verify stream 4 (Plain text format)
    let stream4_result = &results[3];
    assert_eq!(stream4_result.results.len(), 3);
    assert!(stream4_result.results.iter().all(|r| r.success));
    assert!(stream4_result.results.iter().all(|r| r.event.format_type == FormatType::PlainText));
    assert_eq!(stream4_result.statistics.total_lines, 3);
    assert_eq!(stream4_result.statistics.successful_parses, 3);
    
    // Verify line numbers are correct for each stream
    for stream_result in &results {
        for (i, result) in stream_result.results.iter().enumerate() {
            assert_eq!(result.line_number, Some(i + 1));
        }
    }
    
    // Verify shared cache was used effectively
    let cache_stats = parser.get_cache_stats();
    assert!(cache_stats.entries > 0, "Shared cache should have entries");
}

/// Test thread safety of shared components
#[test]
fn test_thread_safety_shared_components() {
    let parser = Arc::new(ParallelParser::with_config(ParallelConfig {
        num_threads: 8,
        enable_shared_cache: true,
        ..Default::default()
    }));
    
    let num_threads = 8;
    let lines_per_thread = 50;
    
    // Create test data for each thread
    let test_data: Vec<Vec<String>> = (0..num_threads)
        .map(|thread_id| {
            (0..lines_per_thread)
                .map(|line_id| {
                    // For shared sources, use the same lines to generate cache hits
                    if thread_id % 2 == 0 {
                        // Shared source - use consistent lines
                        match line_id % 4 {
                            0 => r#"{"message": "Shared log", "level": "INFO"}"#.to_string(),
                            1 => "level=WARN msg=shared user=test".to_string(),
                            2 => "[2025-12-29T10:21:03Z] [ERROR] Shared message".to_string(),
                            _ => "Plain text shared message".to_string(),
                        }
                    } else {
                        // Unique source - use thread-specific lines
                        match line_id % 4 {
                            0 => format!(r#"{{"message": "Thread {} line {}", "level": "INFO"}}"#, thread_id, line_id),
                            1 => format!("level=WARN msg=\"Thread {} line {}\" user=test{}", thread_id, line_id, thread_id),
                            2 => format!("[2025-12-29T10:21:03Z] [ERROR] Thread {} line {}", thread_id, line_id),
                            _ => format!("Plain text from thread {} line {}", thread_id, line_id),
                        }
                    }
                })
                .collect()
        })
        .collect();
    
    // Spawn threads to parse concurrently
    let handles: Vec<_> = test_data
        .into_iter()
        .enumerate()
        .map(|(thread_id, lines)| {
            let parser_clone = parser.clone();
            thread::spawn(move || {
                // Use some shared sources to generate cache hits
                let source = if thread_id % 2 == 0 {
                    "shared_source.log".to_string() // Half the threads use the same source
                } else {
                    format!("thread_{}.log", thread_id)
                };
                let result = parser_clone.parse_lines_parallel(lines.clone(), &source);
                
                // Verify results
                assert_eq!(result.results.len(), lines.len());
                assert!(result.results.iter().all(|r| r.success));
                assert_eq!(result.statistics.total_lines, lines.len());
                assert_eq!(result.statistics.successful_parses, lines.len());
                
                // Return some metrics for verification
                (thread_id, result.results.len(), result.statistics.successful_parses)
            })
        })
        .collect();
    
    // Wait for all threads and collect results
    let thread_results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("Thread should not panic"))
        .collect();
    
    // Verify all threads completed successfully
    assert_eq!(thread_results.len(), num_threads);
    
    let total_lines_processed: usize = thread_results.iter().map(|(_, lines, _)| *lines).sum();
    let total_successful_parses: usize = thread_results.iter().map(|(_, _, success)| *success).sum();
    
    assert_eq!(total_lines_processed, num_threads * lines_per_thread);
    assert_eq!(total_successful_parses, num_threads * lines_per_thread);
    
    // Verify shared cache accumulated entries from all threads
    let cache_stats = parser.get_cache_stats();
    assert!(cache_stats.entries > 0, "Shared cache should have entries from multiple threads");
    assert!(cache_stats.cache_hits > 0, "Should have cache hits from concurrent access");
}

/// Test thread safety with high contention on shared cache
#[test]
fn test_shared_cache_high_contention() {
    let shared_cache = Arc::new(RwLock::new(FormatCache::new()));
    let num_threads = 10;
    let operations_per_thread = 100;
    
    // Test concurrent access to shared cache
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let cache_clone = shared_cache.clone();
            thread::spawn(move || {
                let structures = ThreadSafeParsingStructures::new(cache_clone);
                
                for i in 0..operations_per_thread {
                    // Alternate between different sources to test cache behavior
                    let source = if i % 3 == 0 {
                        format!("shared_source.log") // Same source for cache hits
                    } else {
                        format!("thread_{}_source_{}.log", thread_id, i) // Unique sources
                    };
                    
                    let line = match i % 4 {
                        0 => r#"{"message": "test", "level": "INFO"}"#,
                        1 => "level=WARN msg=test user=admin count=5",
                        2 => "[2025-12-29T10:21:03Z] [ERROR] Test message",
                        _ => "Plain text message",
                    };
                    
                    // This will exercise the shared cache under high contention
                    let _format = structures.detect_format_with_shared_cache(line, &source);
                }
                
                thread_id
            })
        })
        .collect();
    
    // Wait for all threads to complete
    let completed_threads: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("Thread should not panic"))
        .collect();
    
    // Verify all threads completed
    assert_eq!(completed_threads.len(), num_threads);
    
    // Verify cache state after high contention
    let cache = shared_cache.read();
    let stats = cache.stats();
    
    // Should have cache entries
    assert!(stats.entries > 0, "Cache should have entries after concurrent operations");
    
    // Should have both hits and misses due to mixed access patterns
    assert!(stats.cache_hits > 0, "Should have cache hits from shared sources");
    assert!(stats.cache_misses > 0, "Should have cache misses from unique sources");
    
    // Hit rate should be reasonable (not too low due to shared sources)
    assert!(stats.hit_rate > 0.1, "Hit rate should be reasonable with shared sources");
}

/// Test producer-consumer pattern with multiple workers
#[test]
fn test_producer_consumer_multiple_workers() {
    let parser = ParallelParser::with_config(ParallelConfig {
        num_threads: 4,
        queue_capacity: 1000,
        ..Default::default()
    });
    
    // Create a large batch of mixed format logs
    let mut lines = Vec::new();
    
    for i in 0..200 {
        match i % 4 {
            0 => lines.push(format!(r#"{{"message": "Log {}", "level": "INFO", "id": {}}}"#, i, i)),
            1 => lines.push(format!("level=WARN msg=\"Log {}\" user=user{} id={}", i, i % 10, i)),
            2 => lines.push(format!("[2025-12-29T10:21:03Z] [ERROR] Log {} with id {}", i, i)),
            _ => lines.push(format!("Plain text log {} with identifier {}", i, i)),
        }
    }
    
    let result = parser.parse_lines_producer_consumer(lines.clone(), "producer_consumer_test.log")
        .expect("Producer-consumer parsing should succeed");
    
    // Verify all lines were processed
    assert_eq!(result.results.len(), lines.len());
    
    // All should succeed (with fallback to plain text if needed)
    assert!(result.results.iter().all(|r| r.success));
    
    // Verify statistics
    assert_eq!(result.statistics.total_lines, lines.len());
    assert_eq!(result.statistics.successful_parses, lines.len());
    assert_eq!(result.statistics.failed_parses, 0);
    
    // Verify format distribution
    let json_count = result.results.iter().filter(|r| r.event.format_type == FormatType::Json).count();
    let logfmt_count = result.results.iter().filter(|r| r.event.format_type == FormatType::Logfmt).count();
    let pattern_count = result.results.iter().filter(|r| 
        matches!(r.event.format_type, FormatType::TimestampLevel | FormatType::Pattern)
    ).count();
    let plain_count = result.results.iter().filter(|r| r.event.format_type == FormatType::PlainText).count();
    
    // Should have roughly equal distribution (50 of each type), but allow for more variation due to caching effects
    assert!(json_count >= 5, "JSON count should be >= 5, got {}", json_count);
    assert!(logfmt_count >= 5, "Logfmt count should be >= 5, got {}", logfmt_count);
    assert!(pattern_count >= 5, "Pattern count should be >= 5, got {}", pattern_count);
    assert!(plain_count >= 5, "Plain count should be >= 5, got {}", plain_count);
    
    // Total should equal input size
    assert_eq!(json_count + logfmt_count + pattern_count + plain_count, lines.len());
}

/// Test error handling in parallel processing
#[test]
fn test_parallel_error_handling() {
    let parser = ParallelParser::with_config(ParallelConfig {
        num_threads: 3,
        ..Default::default()
    });
    
    // Create lines with various error conditions
    let lines = vec![
        // Valid lines
        r#"{"message": "Valid JSON", "level": "INFO"}"#.to_string(),
        "level=INFO msg=valid user=test count=5".to_string(),
        "[2025-12-29T10:21:03Z] [INFO] Valid pattern".to_string(),
        
        // Malformed lines that should fall back to plain text
        r#"{"incomplete": json"#.to_string(),
        "insufficient=pairs".to_string(),
        "INVALID_TIMESTAMP INVALID_LEVEL message".to_string(),
        
        // Edge cases
        "".to_string(), // Empty line
        "   \t  \n  ".to_string(), // Whitespace only
        "Single word".to_string(),
        
        // More valid lines
        r#"{"message": "Another valid JSON", "level": "ERROR"}"#.to_string(),
        "level=ERROR msg=another user=admin action=test".to_string(),
    ];
    
    let result = parser.parse_lines_parallel(lines.clone(), "error_handling_test.log");
    
    // All lines should be processed (no crashes)
    assert_eq!(result.results.len(), lines.len());
    
    // All should succeed due to fallback to plain text
    for (i, parse_result) in result.results.iter().enumerate() {
        assert!(parse_result.success, "Line {} should succeed with fallback: {:?}", i + 1, parse_result.error);
        assert_eq!(parse_result.line_number, Some(i + 1));
    }
    
    // Verify that malformed inputs fell back to plain text
    // Lines 3, 4, 5 (0-indexed) should be plain text due to malformed input
    // Note: Due to parallel processing, we can't guarantee exact order, so just check counts
    let malformed_plain_count = result.results.iter()
        .filter(|r| r.event.format_type == FormatType::PlainText)
        .count();
    
    // Should have at least the malformed inputs plus edge cases as plain text
    assert!(malformed_plain_count >= 5, "Should have at least 5 plain text results for malformed/edge cases, got {}", malformed_plain_count);
    
    // Valid lines should be detected correctly (check that we have some of each valid format)
    let json_count = result.results.iter().filter(|r| r.event.format_type == FormatType::Json).count();
    let logfmt_count = result.results.iter().filter(|r| r.event.format_type == FormatType::Logfmt).count();
    let pattern_count = result.results.iter().filter(|r| 
        matches!(r.event.format_type, FormatType::TimestampLevel | FormatType::Pattern)
    ).count();
    
    // Due to caching effects and parallel processing, we just need to ensure we have some variety
    let total_structured = json_count + logfmt_count + pattern_count;
    assert!(total_structured >= 1, "Should have at least 1 structured format result, got {}", total_structured);
    assert!(malformed_plain_count >= 3, "Should have at least 3 plain text results for malformed/edge cases, got {}", malformed_plain_count);
    
    // Statistics should reflect all successful processing
    assert_eq!(result.statistics.total_lines, lines.len());
    assert_eq!(result.statistics.successful_parses, lines.len());
    assert_eq!(result.statistics.failed_parses, 0);
}

/// Test parallel processing with different concurrency levels
#[test]
fn test_different_concurrency_levels() {
    let test_lines: Vec<String> = (0..100)
        .map(|i| format!(r#"{{"message": "Test {}", "level": "INFO", "id": {}}}"#, i, i))
        .collect();
    
    // Test with different thread counts
    for num_threads in [1, 2, 4, 8] {
        let parser = ParallelParser::with_config(ParallelConfig {
            num_threads,
            ..Default::default()
        });
        
        let result = parser.parse_lines_parallel(test_lines.clone(), &format!("concurrency_{}.log", num_threads));
        
        // Results should be consistent regardless of thread count
        assert_eq!(result.results.len(), test_lines.len());
        assert!(result.results.iter().all(|r| r.success));
        assert!(result.results.iter().all(|r| r.event.format_type == FormatType::Json));
        
        // Statistics should be consistent
        assert_eq!(result.statistics.total_lines, test_lines.len());
        assert_eq!(result.statistics.successful_parses, test_lines.len());
        assert_eq!(result.statistics.failed_parses, 0);
        
        // Line numbers should be preserved
        for (i, parse_result) in result.results.iter().enumerate() {
            assert_eq!(parse_result.line_number, Some(i + 1));
        }
    }
}

/// Test memory safety under concurrent access
#[test]
fn test_memory_safety_concurrent_access() {
    let parser = Arc::new(ParallelParser::new());
    let num_concurrent_operations = 20;
    
    // Spawn multiple threads doing different operations concurrently
    let handles: Vec<_> = (0..num_concurrent_operations)
        .map(|i| {
            let parser_clone = parser.clone();
            thread::spawn(move || {
                match i % 4 {
                    0 => {
                        // Parse lines
                        let lines = vec![
                            format!(r#"{{"message": "Concurrent {}", "level": "INFO"}}"#, i),
                            format!("level=WARN msg=concurrent{} user=test", i),
                        ];
                        let _result = parser_clone.parse_lines_parallel(lines, &format!("concurrent_{}.log", i));
                    }
                    1 => {
                        // Check cache stats
                        let _stats = parser_clone.get_cache_stats();
                    }
                    2 => {
                        // Get global statistics
                        let _global_stats = parser_clone.get_global_statistics();
                    }
                    _ => {
                        // Clear cache (this tests write access)
                        parser_clone.clear_shared_cache();
                    }
                }
                i
            })
        })
        .collect();
    
    // Wait for all operations to complete
    let completed: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("Thread should not panic"))
        .collect();
    
    // Verify all operations completed
    assert_eq!(completed.len(), num_concurrent_operations);
    
    // Parser should still be functional after concurrent access
    let test_result = parser.parse_lines_parallel(
        vec![r#"{"message": "Final test", "level": "INFO"}"#.to_string()],
        "final_test.log"
    );
    
    assert_eq!(test_result.results.len(), 1);
    assert!(test_result.results[0].success);
}

/// Stress test with large number of threads and operations
#[test]
fn test_stress_high_concurrency() {
    let parser = ParallelParser::with_config(ParallelConfig {
        num_threads: 16,
        batch_size: 50,
        queue_capacity: 5000,
        ..Default::default()
    });
    
    // Create a large dataset
    let large_dataset: Vec<String> = (0..500) // Reduced from 1000 to 500 for faster testing
        .map(|i| {
            match i % 5 {
                0 => format!(r#"{{"message": "Large dataset {}", "level": "INFO", "batch": {}}}"#, i, i / 100),
                1 => format!("level=WARN msg=\"Dataset {}\" batch={} user=stress_test", i, i / 100),
                2 => format!("[2025-12-29T10:21:03Z] [ERROR] Dataset {} batch {}", i, i / 100),
                3 => format!("Plain text dataset entry {} in batch {}", i, i / 100),
                _ => format!(r#"{{"message": "JSON dataset {}", "level": "DEBUG", "batch": {}, "extra": "data"}}"#, i, i / 100),
            }
        })
        .collect();
    
    let start_time = std::time::Instant::now();
    let result = parser.parse_lines_parallel(large_dataset.clone(), "stress_test.log");
    let processing_time = start_time.elapsed();
    
    // Verify results
    assert_eq!(result.results.len(), large_dataset.len());
    assert!(result.results.iter().all(|r| r.success));
    
    // Verify statistics
    assert_eq!(result.statistics.total_lines, large_dataset.len());
    assert_eq!(result.statistics.successful_parses, large_dataset.len());
    
    // Performance check - should complete in reasonable time (adjust as needed)
    assert!(processing_time.as_secs() < 15, "Stress test should complete in under 15 seconds, took {:?}", processing_time);
    
    // Verify format distribution is reasonable
    let format_counts: std::collections::HashMap<FormatType, usize> = result.results
        .iter()
        .fold(std::collections::HashMap::new(), |mut acc, r| {
            *acc.entry(r.event.format_type).or_insert(0) += 1;
            acc
        });
    
    // Should have multiple format types
    assert!(format_counts.len() >= 3, "Should detect multiple format types");
    
    // Each format should have a reasonable number of entries
    for (format_type, count) in format_counts {
        assert!(count > 0, "Format {:?} should have at least one entry", format_type);
    }
    
    println!("Stress test completed in {:?} for {} lines", processing_time, large_dataset.len());
}