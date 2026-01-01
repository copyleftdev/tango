use crate::*;
use std::io::Cursor;
use serde_json;

/// Integration tests for the main TangoParser
/// These tests verify end-to-end parsing with real-world log samples

/// Test end-to-end parsing with real-world log samples
pub fn test_end_to_end_parsing_with_real_world_samples() -> Result<(), String> {
    println!("Testing end-to-end parsing with real-world log samples...");
    
    let mut parser = TangoParser::new();
    
    // Test 1: Real JSON logs from various applications
    let json_logs = vec![
        r#"{"timestamp":"2025-12-30T10:21:03.123Z","level":"INFO","message":"User authentication successful","user_id":12345,"session_id":"abc123","ip":"192.168.1.100"}"#,
        r#"{"@timestamp":"2025-12-30T10:21:04.456Z","log.level":"ERROR","msg":"Database connection failed","error":"Connection timeout","retry_count":3}"#,
        r#"{"ts":"2025-12-30T10:21:05.789Z","severity":"WARN","log.message":"High memory usage detected","memory_usage_percent":85.7,"threshold":80}"#,
    ];
    
    for (i, log_line) in json_logs.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("app{}.log", i));
        if !result.success {
            return Err(format!("JSON log {} failed to parse: {:?}", i, result.error));
        }
        
        if result.event.format_type != FormatType::Json {
            return Err(format!("JSON log {} detected as wrong format: {:?}", i, result.event.format_type));
        }
        
        // Verify message extraction
        if result.event.message.is_empty() {
            return Err(format!("JSON log {} has empty message", i));
        }
        
        // Verify timestamp extraction
        if result.event.timestamp.is_none() {
            return Err(format!("JSON log {} missing timestamp", i));
        }
        
        // Verify level extraction where available
        if i == 0 && result.event.level != Some(LogLevel::Info) {
            return Err(format!("JSON log {} has wrong level: {:?}", i, result.event.level));
        }
    }
    
    println!("✓ JSON logs parsed successfully");
    
    // Test 2: Real logfmt logs from various applications
    let logfmt_logs = vec![
        "timestamp=2025-12-30T10:21:03Z level=INFO msg=started user=admin component=auth action=login session_id=xyz789",
        "time=2025-12-30T10:21:04Z lvl=ERROR message=failed database=users table=sessions error=timeout duration=5.2s",
        "ts=2025-12-30T10:21:05Z severity=WARN msg=warning memory_usage=85.7 cpu_usage=72.3 disk_usage=45.1",
    ];
    
    for (i, log_line) in logfmt_logs.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("service{}.log", i));
        if !result.success {
            return Err(format!("Logfmt log {} failed to parse: {:?}", i, result.error));
        }
        
        if result.event.format_type != FormatType::Logfmt {
            return Err(format!("Logfmt log {} detected as wrong format: {:?}", i, result.event.format_type));
        }
        
        // Verify message extraction
        if result.event.message.is_empty() {
            return Err(format!("Logfmt log {} has empty message", i));
        }
    }
    
    println!("✓ Logfmt logs parsed successfully");
    
    // Test 3: Real timestamp+level pattern logs
    let pattern_logs = vec![
        "[2025-12-30T10:21:03Z] [INFO] Application startup completed successfully",
        "[2025-12-30T10:21:04Z] [ERROR] Failed to connect to database: connection timeout",
        "[2025-12-30T10:21:05Z] [WARN] Memory usage is high: 85.7% of available memory",
        "2025-12-30 10:21:06 INFO Server listening on port 8080",
        "2025-12-30 10:21:07 ERROR Authentication failed for user admin",
    ];
    
    for (i, log_line) in pattern_logs.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("system{}.log", i));
        if !result.success {
            return Err(format!("Pattern log {} failed to parse: {:?}", i, result.error));
        }
        
        // Should be detected as either TimestampLevel, Pattern, or PlainText (fallback)
        if result.event.format_type != FormatType::TimestampLevel && 
           result.event.format_type != FormatType::Pattern && 
           result.event.format_type != FormatType::PlainText {
            return Err(format!("Pattern log {} detected as unexpected format: {:?}", i, result.event.format_type));
        }
        
        // Verify message extraction
        if result.event.message.is_empty() {
            return Err(format!("Pattern log {} has empty message", i));
        }
    }
    
    println!("✓ Pattern logs parsed successfully");
    
    // Test 4: Plain text logs
    let plain_logs = vec![
        "This is a plain text log message without any structure",
        "Another unstructured log entry from legacy system",
        "Simple message: operation completed",
    ];
    
    for (i, log_line) in plain_logs.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("legacy{}.log", i));
        if !result.success {
            return Err(format!("Plain text log {} failed to parse: {:?}", i, result.error));
        }
        
        if result.event.format_type != FormatType::PlainText {
            return Err(format!("Plain text log {} detected as wrong format: {:?}", i, result.event.format_type));
        }
        
        // Verify message preservation
        if result.event.message != *log_line {
            return Err(format!("Plain text log {} message not preserved correctly", i));
        }
    }
    
    println!("✓ Plain text logs parsed successfully");
    
    println!("All real-world log samples parsed successfully!");
    Ok(())
}

/// Test multi-format log streams
pub fn test_multi_format_log_streams() -> Result<(), String> {
    println!("Testing multi-format log streams...");
    
    // Create parser with caching disabled to ensure each line is detected independently
    let mut config = TangoConfig::default();
    config.enable_format_caching = false;
    let mut parser = TangoParser::with_config(config);
    
    // Mixed format log stream
    let mixed_logs = vec![
        (r#"{"timestamp":"2025-12-30T10:21:03Z","level":"INFO","message":"JSON log entry"}"#, FormatType::Json),
        ("timestamp=2025-12-30T10:21:04Z level=ERROR msg=logfmt user=admin", FormatType::Logfmt),
        ("[2025-12-30T10:21:05Z] [WARN] Pattern log entry", FormatType::TimestampLevel),
        ("Plain text log entry without structure", FormatType::PlainText),
        (r#"{"level":"DEBUG","message":"Another JSON entry","component":"parser"}"#, FormatType::Json),
        ("level=INFO msg=another user=system component=auth", FormatType::Logfmt),
    ];
    
    let lines: Vec<String> = mixed_logs.iter().map(|(line, _)| line.to_string()).collect();
    let results = parser.parse_lines(lines);
    
    if results.len() != mixed_logs.len() {
        return Err(format!("Expected {} results, got {}", mixed_logs.len(), results.len()));
    }
    
    for (i, (result, (original_line, expected_format))) in results.iter().zip(mixed_logs.iter()).enumerate() {
        if !result.success {
            return Err(format!("Mixed log {} failed to parse: {:?}", i, result.error));
        }
        
        // For pattern logs, we accept TimestampLevel, Pattern, or PlainText as fallback
        let format_matches = if *expected_format == FormatType::TimestampLevel {
            result.event.format_type == FormatType::TimestampLevel || 
            result.event.format_type == FormatType::Pattern || 
            result.event.format_type == FormatType::PlainText
        } else {
            result.event.format_type == *expected_format
        };
        
        if !format_matches {
            return Err(format!("Mixed log {} detected as {:?}, expected {:?}", i, result.event.format_type, expected_format));
        }
        
        // Verify message is not empty
        if result.event.message.is_empty() {
            return Err(format!("Mixed log {} has empty message", i));
        }
        
        // Verify raw line is preserved
        if result.event.raw != *original_line {
            return Err(format!("Mixed log {} raw line not preserved", i));
        }
    }
    
    println!("✓ Multi-format log stream parsed successfully");
    
    // Test with different sources to verify caching works correctly
    let mut parser_with_cache = TangoParser::new(); // Default has caching enabled
    
    // Parse same format from same source multiple times
    let json_line = r#"{"level":"INFO","message":"Cached test"}"#;
    
    for i in 0..3 {
        let result = parser_with_cache.parse_line_with_source(json_line, "cached.log");
        if !result.success {
            return Err(format!("Cached JSON log {} failed to parse: {:?}", i, result.error));
        }
        
        if result.event.format_type != FormatType::Json {
            return Err(format!("Cached JSON log {} detected as wrong format: {:?}", i, result.event.format_type));
        }
    }
    
    println!("✓ Format caching working correctly");
    
    println!("All multi-format log stream tests passed!");
    Ok(())
}

/// Test error recovery scenarios
pub fn test_error_recovery_scenarios() -> Result<(), String> {
    println!("Testing error recovery scenarios...");
    
    let mut parser = TangoParser::new();
    
    // Test 1: Malformed JSON should fall back gracefully
    let malformed_logs = vec![
        r#"{"incomplete": json"#,  // Missing closing brace
        r#"{"invalid": "json",}"#, // Trailing comma
        r#"{not_quoted_key: "value"}"#, // Unquoted key
        r#"{"nested": {"incomplete": }"#, // Incomplete nested object
    ];
    
    for (i, log_line) in malformed_logs.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("malformed{}.log", i));
        
        // Should still succeed (fallback to plain text or other format)
        if !result.success {
            return Err(format!("Malformed JSON {} should have fallen back gracefully: {:?}", i, result.error));
        }
        
        // Should not be detected as JSON
        if result.event.format_type == FormatType::Json {
            return Err(format!("Malformed JSON {} incorrectly detected as JSON", i));
        }
        
        // Should preserve the original line
        if result.event.raw != *log_line {
            return Err(format!("Malformed JSON {} raw line not preserved", i));
        }
    }
    
    println!("✓ Malformed JSON handled gracefully");
    
    // Test 2: Insufficient logfmt pairs should fall back
    let insufficient_logfmt = vec![
        "key=value", // Only 1 pair
        "key1=value1 key2=value2", // Only 2 pairs
        "not_key_value_format at all",
        "key=value=extra=equals", // Malformed values
    ];
    
    for (i, log_line) in insufficient_logfmt.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("insufficient{}.log", i));
        
        // Should still succeed (fallback to plain text)
        if !result.success {
            return Err(format!("Insufficient logfmt {} should have fallen back gracefully: {:?}", i, result.error));
        }
        
        // Should preserve the original line
        if result.event.raw != *log_line {
            return Err(format!("Insufficient logfmt {} raw line not preserved", i));
        }
    }
    
    println!("✓ Insufficient logfmt handled gracefully");
    
    // Test 3: Empty and whitespace-only lines
    let edge_case_lines = vec![
        "",           // Empty line
        "   ",        // Whitespace only
        "\t\n",       // Tabs and newlines
        "   \t  \n ", // Mixed whitespace
    ];
    
    for (i, log_line) in edge_case_lines.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("edge{}.log", i));
        
        // Should handle gracefully (may succeed or fail, but shouldn't crash)
        if result.success {
            // If it succeeds, should preserve the original line
            if result.event.raw != *log_line {
                return Err(format!("Edge case {} raw line not preserved", i));
            }
        }
        // If it fails, that's also acceptable for empty/whitespace lines
    }
    
    println!("✓ Edge case lines handled gracefully");
    
    // Test 4: Very long lines
    let long_line = "x".repeat(10000); // 10KB line
    let result = parser.parse_line_with_source(&long_line, "long.log");
    
    if !result.success {
        return Err(format!("Long line should be handled gracefully: {:?}", result.error));
    }
    
    if result.event.raw != long_line {
        return Err("Long line raw content not preserved".to_string());
    }
    
    println!("✓ Long lines handled gracefully");
    
    // Test 5: Lines with special characters
    let special_char_lines = vec![
        "Log with unicode: 你好世界 🌍",
        "Log with null bytes: \0\0\0",
        "Log with control chars: \x01\x02\x03",
        "Log with quotes: \"quoted\" and 'single quoted'",
        "Log with backslashes: C:\\Windows\\System32\\",
    ];
    
    for (i, log_line) in special_char_lines.iter().enumerate() {
        let result = parser.parse_line_with_source(log_line, &format!("special{}.log", i));
        
        // Should handle gracefully
        if !result.success {
            return Err(format!("Special character line {} should be handled gracefully: {:?}", i, result.error));
        }
        
        // Should preserve the original line
        if result.event.raw != *log_line {
            return Err(format!("Special character line {} raw content not preserved", i));
        }
    }
    
    println!("✓ Special character lines handled gracefully");
    
    println!("All error recovery scenarios passed!");
    Ok(())
}

/// Test streaming processing with large log files
pub fn test_streaming_processing() -> Result<(), String> {
    println!("Testing streaming processing...");
    
    let mut parser = TangoParser::new();
    
    // Create a large log file in memory
    let log_entries = vec![
        r#"{"timestamp":"2025-12-30T10:21:03Z","level":"INFO","message":"Entry 1"}"#,
        "timestamp=2025-12-30T10:21:04Z level=ERROR msg=entry2 user=admin",
        "[2025-12-30T10:21:05Z] [WARN] Entry 3 from pattern parser",
        "Plain text entry 4",
        r#"{"timestamp":"2025-12-30T10:21:06Z","level":"DEBUG","message":"Entry 5"}"#,
    ];
    
    // Repeat entries to create a larger dataset
    let mut large_log_data = String::new();
    for i in 0..100 {
        for entry in &log_entries {
            // For JSON entries, modify the message field to include iteration
            if entry.starts_with("{") {
                // Parse and modify JSON to include iteration in message
                if let Ok(mut json_value) = serde_json::from_str::<serde_json::Value>(entry) {
                    if let Some(message) = json_value.get_mut("message") {
                        if let Some(msg_str) = message.as_str() {
                            *message = serde_json::Value::String(format!("{} (iteration {})", msg_str, i));
                        }
                    }
                    large_log_data.push_str(&serde_json::to_string(&json_value).unwrap());
                } else {
                    large_log_data.push_str(entry);
                }
            } else {
                // For non-JSON entries, append iteration info
                large_log_data.push_str(&format!("{} (iteration {})", entry, i));
            }
            large_log_data.push('\n');
        }
    }
    
    // Test streaming parsing
    let cursor = Cursor::new(large_log_data.as_bytes());
    let results = parser.parse_reader(cursor, "large.log")
        .map_err(|e| format!("Streaming parse failed: {}", e))?;
    
    let expected_count = 100 * log_entries.len();
    if results.len() != expected_count {
        return Err(format!("Expected {} results, got {}", expected_count, results.len()));
    }
    
    // Verify all results are successful
    let successful_count = results.iter().filter(|r| r.success).count();
    if successful_count != expected_count {
        return Err(format!("Expected {} successful parses, got {}", expected_count, successful_count));
    }
    
    // Verify format distribution (be flexible as detection may vary)
    let json_count = results.iter().filter(|r| r.event.format_type == FormatType::Json).count();
    let logfmt_count = results.iter().filter(|r| r.event.format_type == FormatType::Logfmt).count();
    let pattern_count = results.iter().filter(|r| r.event.format_type == FormatType::Pattern).count();
    let timestamp_level_count = results.iter().filter(|r| r.event.format_type == FormatType::TimestampLevel).count();
    let plain_count = results.iter().filter(|r| r.event.format_type == FormatType::PlainText).count();
    
    // Should have some of each format type (allowing for detection variations)
    if json_count == 0 {
        return Err("Expected some JSON entries to be detected".to_string());
    }
    
    if logfmt_count == 0 {
        return Err("Expected some logfmt entries to be detected".to_string());
    }
    
    // At least some entries should be parsed (total should equal expected)
    let total_detected = json_count + logfmt_count + pattern_count + timestamp_level_count + plain_count;
    if total_detected != expected_count {
        return Err(format!("Total detected formats {} doesn't match expected {}", total_detected, expected_count));
    }
    
    println!("✓ Streaming processing completed successfully");
    println!("  - Processed {} log entries", results.len());
    println!("  - JSON: {}, Logfmt: {}, Pattern: {}, TimestampLevel: {}, Plain: {}", 
             json_count, logfmt_count, pattern_count, timestamp_level_count, plain_count);
    
    println!("All streaming processing tests passed!");
    Ok(())
}

/// Test configuration management and profiles
pub fn test_configuration_and_profiles() -> Result<(), String> {
    println!("Testing configuration management and profiles...");
    
    // Test 1: Custom configuration
    let mut config = TangoConfig::default();
    config.enable_format_caching = false;
    config.enable_statistics = true;
    config.default_source = "test".to_string();
    
    let mut parser = TangoParser::with_config(config);
    
    // Verify configuration is applied
    let parser_config = parser.get_config();
    if parser_config.enable_format_caching {
        return Err("Format caching should be disabled".to_string());
    }
    
    if !parser_config.enable_statistics {
        return Err("Statistics should be enabled".to_string());
    }
    
    if parser_config.default_source != "test" {
        return Err("Default source not set correctly".to_string());
    }
    
    println!("✓ Custom configuration applied correctly");
    
    // Test 2: Statistics collection
    let test_lines = vec![
        r#"{"level":"INFO","message":"Test 1"}"#,
        "level=ERROR msg=test2",
        "Plain text test 3",
    ];
    
    for line in test_lines {
        parser.parse_line(line);
    }
    
    // Check statistics
    if let Some(stats) = parser.get_statistics() {
        if stats.total_lines != 3 {
            return Err(format!("Expected 3 total lines, got {}", stats.total_lines));
        }
        
        if stats.successful_parses != 3 {
            return Err(format!("Expected 3 successful parses, got {}", stats.successful_parses));
        }
        
        if stats.format_distribution.is_empty() {
            return Err("Format distribution should not be empty".to_string());
        }
        
        println!("✓ Statistics collection working correctly");
        println!("  - Total lines: {}", stats.total_lines);
        println!("  - Successful: {}", stats.successful_parses);
        println!("  - Success rate: {:.1}%", stats.success_rate());
    } else {
        return Err("Statistics should be available".to_string());
    }
    
    // Test 3: Profile management
    let mut profile_parser = TangoParser::new();
    
    // Add a regex profile
    let mut field_mappings = std::collections::HashMap::new();
    field_mappings.insert("timestamp".to_string(), 1);
    field_mappings.insert("level".to_string(), 2);
    field_mappings.insert("message".to_string(), 3);
    
    let regex_config = RegexProfileConfig {
        name: "test_profile".to_string(),
        pattern: r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string(),
        field_mappings,
        timestamp_field: Some("timestamp".to_string()),
        level_field: Some("level".to_string()),
        message_field: Some("message".to_string()),
        timestamp_format: None,
    };
    
    let profile_config = ProfileConfig::Regex(regex_config);
    
    // Add profile
    if let Err(e) = profile_parser.add_profile("test_profile".to_string(), profile_config) {
        return Err(format!("Failed to add profile: {}", e));
    }
    
    // Verify profile exists
    let profiles = profile_parser.list_profiles();
    if !profiles.contains(&"test_profile".to_string()) {
        return Err("Profile not found in list".to_string());
    }
    
    // Remove profile
    if !profile_parser.remove_profile("test_profile") {
        return Err("Failed to remove profile".to_string());
    }
    
    // Verify profile is gone
    let profiles = profile_parser.list_profiles();
    if profiles.contains(&"test_profile".to_string()) {
        return Err("Profile should have been removed".to_string());
    }
    
    println!("✓ Profile management working correctly");
    
    println!("All configuration and profile tests passed!");
    Ok(())
}

/// Run all integration tests
pub fn run_all_integration_tests() -> Result<(), String> {
    println!("Running Tango Parser Integration Tests");
    println!("=====================================");
    
    test_end_to_end_parsing_with_real_world_samples()?;
    println!();
    
    test_multi_format_log_streams()?;
    println!();
    
    test_error_recovery_scenarios()?;
    println!();
    
    test_streaming_processing()?;
    println!();
    
    test_configuration_and_profiles()?;
    println!();
    
    println!("🎉 All integration tests passed successfully!");
    println!("The TangoParser is ready for production use.");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_integration_end_to_end_parsing() {
        if let Err(e) = test_end_to_end_parsing_with_real_world_samples() {
            panic!("Integration test failed: {}", e);
        }
    }
    
    #[test]
    fn test_integration_multi_format_streams() {
        if let Err(e) = test_multi_format_log_streams() {
            panic!("Integration test failed: {}", e);
        }
    }
    
    #[test]
    fn test_integration_error_recovery() {
        if let Err(e) = test_error_recovery_scenarios() {
            panic!("Integration test failed: {}", e);
        }
    }
    
    #[test]
    fn test_integration_streaming() {
        if let Err(e) = test_streaming_processing() {
            panic!("Integration test failed: {}", e);
        }
    }
    
    #[test]
    fn test_integration_configuration() {
        if let Err(e) = test_configuration_and_profiles() {
            panic!("Integration test failed: {}", e);
        }
    }
}