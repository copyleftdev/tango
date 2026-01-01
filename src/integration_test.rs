use crate::*;

/// Integration test to verify core parsing functionality works correctly
pub fn test_core_parsing_functionality() -> Result<(), String> {
    println!("Testing core parsing functionality...");
    
    // Test 1: JSON Parser
    let json_parser = JsonParser::new();
    let json_line = r#"{"timestamp": "2025-12-29T10:21:03Z", "level": "INFO", "message": "User logged in", "user_id": 123}"#;
    let json_result = json_parser.parse(json_line);
    
    if !json_result.success {
        return Err("JSON parser failed to parse valid JSON".to_string());
    }
    
    if json_result.event.message != "User logged in" {
        return Err("JSON parser failed to extract message correctly".to_string());
    }
    
    if json_result.event.level != Some(LogLevel::Info) {
        return Err("JSON parser failed to extract level correctly".to_string());
    }
    
    if json_result.event.timestamp.is_none() {
        return Err("JSON parser failed to extract timestamp".to_string());
    }
    
    println!("✓ JSON parser working correctly");
    
    // Test 2: Logfmt Parser
    let logfmt_parser = LogfmtParser::new();
    let logfmt_line = "timestamp=2025-12-29T10:21:03Z level=ERROR msg=failed user=admin action=delete count=5";
    let logfmt_result = logfmt_parser.parse(logfmt_line);
    
    if !logfmt_result.success {
        return Err("Logfmt parser failed to parse valid logfmt".to_string());
    }
    
    if logfmt_result.event.message != "failed" {
        return Err("Logfmt parser failed to extract message correctly".to_string());
    }
    
    if logfmt_result.event.level != Some(LogLevel::Error) {
        return Err("Logfmt parser failed to extract level correctly".to_string());
    }
    
    println!("✓ Logfmt parser working correctly");
    
    // Test 3: Pattern Parser
    let pattern_parser = PatternParser::new();
    let pattern_line = "[2025-12-29T10:21:03Z] [WARN] Memory usage is high";
    let pattern_result = pattern_parser.parse(pattern_line);
    
    if !pattern_result.success {
        return Err("Pattern parser failed to parse valid timestamp+level pattern".to_string());
    }
    
    if pattern_result.event.message != "Memory usage is high" {
        return Err("Pattern parser failed to extract message correctly".to_string());
    }
    
    if pattern_result.event.level != Some(LogLevel::Warn) {
        return Err("Pattern parser failed to extract level correctly".to_string());
    }
    
    println!("✓ Pattern parser working correctly");
    
    // Test 4: Plain Text Parser
    let plain_parser = PlainTextParser::new();
    let plain_line = "This is a plain text log message without structure";
    let plain_result = plain_parser.parse(plain_line);
    
    if !plain_result.success {
        return Err("Plain text parser failed to parse plain text".to_string());
    }
    
    if plain_result.event.message != plain_line {
        return Err("Plain text parser failed to preserve message correctly".to_string());
    }
    
    if plain_result.event.format_type != FormatType::PlainText {
        return Err("Plain text parser failed to set correct format type".to_string());
    }
    
    println!("✓ Plain text parser working correctly");
    
    // Test 5: Format Classifier
    let mut classifier = TangoFormatClassifier::new();
    
    // Test JSON detection
    let json_format = classifier.detect_format(json_line, "test.log");
    if json_format != FormatType::Json {
        return Err("Format classifier failed to detect JSON format".to_string());
    }
    
    // Test logfmt detection
    let logfmt_format = classifier.detect_format(logfmt_line, "app.log");
    if logfmt_format != FormatType::Logfmt {
        return Err("Format classifier failed to detect logfmt format".to_string());
    }
    
    // Test timestamp+level detection
    let pattern_format = classifier.detect_format(pattern_line, "system.log");
    if pattern_format != FormatType::TimestampLevel {
        return Err("Format classifier failed to detect timestamp+level format".to_string());
    }
    
    // Test plain text detection
    let plain_format = classifier.detect_format(plain_line, "plain.log");
    if plain_format != FormatType::PlainText {
        return Err("Format classifier failed to detect plain text format".to_string());
    }
    
    println!("✓ Format classifier working correctly");
    
    // Test 6: Format Caching
    classifier.cache_format("cached.log".to_string(), FormatType::Json);
    
    // Verify cache hit
    let cached_format = classifier.detect_format_with_caching(plain_line, "cached.log");
    if cached_format != FormatType::Json {
        return Err("Format caching failed - cached format not returned".to_string());
    }
    
    // Verify cache miss uses detection
    let uncached_format = classifier.detect_format_with_caching(plain_line, "uncached.log");
    if uncached_format != FormatType::PlainText {
        return Err("Format caching failed - detection not used for uncached source".to_string());
    }
    
    // Check cache statistics
    let stats = classifier.cache_stats();
    if stats.entries == 0 {
        return Err("Format cache statistics not working correctly".to_string());
    }
    
    println!("✓ Format caching working correctly");
    
    // Test 7: Confidence Scoring
    let json_confidence = classifier.get_confidence(json_line, FormatType::Json);
    if json_confidence < 0.9 {
        return Err("JSON confidence scoring too low for valid JSON".to_string());
    }
    
    let logfmt_confidence = classifier.get_confidence(logfmt_line, FormatType::Logfmt);
    if logfmt_confidence < 0.7 {
        return Err("Logfmt confidence scoring too low for valid logfmt".to_string());
    }
    
    let plain_confidence = classifier.get_confidence(plain_line, FormatType::PlainText);
    if plain_confidence != 0.1 {
        return Err("Plain text confidence scoring incorrect".to_string());
    }
    
    println!("✓ Confidence scoring working correctly");
    
    println!("All core parsing functionality tests passed!");
    Ok(())
}

/// Test the fallback chain behavior
pub fn test_fallback_chain() -> Result<(), String> {
    println!("Testing fallback chain behavior...");
    
    let classifier = TangoFormatClassifier::new();
    
    // Test 1: Malformed JSON should fall back
    let malformed_json = r#"{"incomplete": json"#;
    let format = classifier.detect_format(malformed_json, "malformed.log");
    if format == FormatType::Json {
        return Err("Malformed JSON should not be detected as JSON".to_string());
    }
    println!("✓ Malformed JSON falls back correctly");
    
    // Test 2: Insufficient logfmt pairs should fall back
    let insufficient_logfmt = "key=value another=pair";
    let format = classifier.detect_format(insufficient_logfmt, "insufficient.log");
    if format == FormatType::Logfmt {
        return Err("Insufficient logfmt pairs should not be detected as logfmt".to_string());
    }
    println!("✓ Insufficient logfmt pairs fall back correctly");
    
    // Test 3: Timestamp without level should fall back
    let timestamp_only = "2025-12-29T10:21:03Z This has timestamp but no level";
    let format = classifier.detect_format(timestamp_only, "timestamp.log");
    if format == FormatType::TimestampLevel {
        return Err("Timestamp without level should not be detected as timestamp+level".to_string());
    }
    println!("✓ Timestamp without level falls back correctly");
    
    // Test 4: Level without timestamp should fall back
    let level_only = "ERROR This has level but no timestamp";
    let format = classifier.detect_format(level_only, "level.log");
    if format == FormatType::TimestampLevel {
        return Err("Level without timestamp should not be detected as timestamp+level".to_string());
    }
    println!("✓ Level without timestamp falls back correctly");
    
    println!("All fallback chain tests passed!");
    Ok(())
}

/// Test error handling and resilience
pub fn test_error_handling() -> Result<(), String> {
    println!("Testing error handling and resilience...");
    
    let json_parser = JsonParser::new();
    let logfmt_parser = LogfmtParser::new();
    let pattern_parser = PatternParser::new();
    let plain_parser = PlainTextParser::new();
    
    // Test 1: Empty input handling
    let empty_result = json_parser.parse("");
    if empty_result.success {
        return Err("JSON parser should fail on empty input".to_string());
    }
    
    let empty_result = logfmt_parser.parse("");
    if empty_result.success {
        return Err("Logfmt parser should fail on empty input".to_string());
    }
    
    let empty_result = pattern_parser.parse("");
    if empty_result.success {
        return Err("Pattern parser should fail on empty input".to_string());
    }
    
    // Plain text parser should always succeed
    let empty_result = plain_parser.parse("");
    if !empty_result.success {
        return Err("Plain text parser should succeed on empty input".to_string());
    }
    
    println!("✓ Empty input handled correctly");
    
    // Test 2: Malformed input handling
    let malformed_inputs = vec![
        r#"{"malformed": json"#,
        "key=value=extra=equals",
        "[malformed] timestamp",
        "random garbage text",
    ];
    
    for input in malformed_inputs {
        // All parsers should handle malformed input gracefully (not crash)
        let _ = json_parser.parse(input);
        let _ = logfmt_parser.parse(input);
        let _ = pattern_parser.parse(input);
        let plain_result = plain_parser.parse(input);
        
        // Plain text should always succeed
        if !plain_result.success {
            return Err(format!("Plain text parser failed on input: {}", input));
        }
    }
    
    println!("✓ Malformed input handled gracefully");
    
    println!("All error handling tests passed!");
    Ok(())
}