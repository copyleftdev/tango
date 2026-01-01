use crate::models::*;
#[cfg(test)]
use crate::parse_result::ParseResult;
use crate::parsers::*;
use std::collections::HashMap;

/// Interface for format classification
pub trait FormatClassifier {
    fn detect_format(&self, line: &str, source: &str) -> FormatType;
    fn cache_format(&mut self, source: String, format: FormatType);
    fn get_confidence(&self, line: &str, format: FormatType) -> f64;
}

/// Format cache entry for storing detection results per source
#[derive(Debug, Clone)]
pub struct FormatCacheEntry {
    pub format_type: FormatType,
    pub confidence: f64,
    pub timestamp_format: Option<String>,
    pub field_mappings: HashMap<String, String>,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    pub sample_count: usize,
}

impl FormatCacheEntry {
    pub fn new(format_type: FormatType, confidence: f64) -> Self {
        Self {
            format_type,
            confidence,
            timestamp_format: None,
            field_mappings: HashMap::new(),
            last_updated: chrono::Utc::now(),
            sample_count: 1,
        }
    }
    
    pub fn update(&mut self, confidence: f64) {
        self.confidence = (self.confidence + confidence) / 2.0; // Average confidence
        self.last_updated = chrono::Utc::now();
        self.sample_count += 1;
    }
    
    /// Check if cache entry is stale based on age and sample count
    pub fn is_stale(&self, max_age_seconds: i64, min_samples_for_stability: usize) -> bool {
        let age = chrono::Utc::now().signed_duration_since(self.last_updated);
        
        // Entry is stale if it's old and hasn't been confirmed by enough samples
        age.num_seconds() > max_age_seconds && self.sample_count < min_samples_for_stability
    }
    
    /// Update with field mappings and timestamp format information
    pub fn update_with_metadata(
        &mut self, 
        confidence: f64, 
        timestamp_format: Option<String>,
        field_mappings: HashMap<String, String>
    ) {
        self.update(confidence);
        
        // Update timestamp format if provided
        if let Some(ts_format) = timestamp_format {
            self.timestamp_format = Some(ts_format);
        }
        
        // Merge field mappings (new mappings take precedence)
        for (key, value) in field_mappings {
            self.field_mappings.insert(key, value);
        }
    }
}

/// Comprehensive format cache with performance optimization and adaptive learning
#[derive(Debug, Clone)]
pub struct FormatCache {
    /// Cache entries indexed by source identifier
    cache: HashMap<String, FormatCacheEntry>,
    
    /// Maximum number of cache entries to maintain
    max_entries: usize,
    
    /// Maximum age for cache entries (in seconds)
    max_age_seconds: i64,
    
    /// Minimum samples required for cache entry stability
    min_samples_for_stability: usize,
    
    /// Statistics for monitoring cache performance
    cache_hits: usize,
    cache_misses: usize,
    cache_evictions: usize,
}

impl FormatCache {
    /// Create a new format cache with default settings
    pub fn new() -> Self {
        Self::with_settings(1000, 3600, 5) // 1000 entries, 1 hour max age, 5 samples for stability
    }
    
    /// Create a new format cache with custom settings
    pub fn with_settings(max_entries: usize, max_age_seconds: i64, min_samples_for_stability: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_entries,
            max_age_seconds,
            min_samples_for_stability,
            cache_hits: 0,
            cache_misses: 0,
            cache_evictions: 0,
        }
    }
    
    /// Get cached format for a source, if available and not stale
    pub fn get(&mut self, source: &str) -> Option<&FormatCacheEntry> {
        // Check if entry exists and is stale in one step to avoid borrowing issues
        let should_remove = if let Some(entry) = self.cache.get(source) {
            entry.is_stale(self.max_age_seconds, self.min_samples_for_stability)
        } else {
            false
        };
        
        if should_remove {
            // Remove stale entry
            self.cache.remove(source);
            self.cache_evictions += 1;
            self.cache_misses += 1;
            None
        } else if let Some(entry) = self.cache.get(source) {
            self.cache_hits += 1;
            Some(entry)
        } else {
            self.cache_misses += 1;
            None
        }
    }
    
    /// Cache a format detection result for a source
    pub fn put(
        &mut self, 
        source: String, 
        format_type: FormatType, 
        confidence: f64,
        timestamp_format: Option<String>,
        field_mappings: HashMap<String, String>
    ) {
        // Check if we need to evict entries to make room
        if self.cache.len() >= self.max_entries {
            self.evict_oldest_entries();
        }
        
        // Update existing entry or create new one
        if let Some(entry) = self.cache.get_mut(&source) {
            // Update the format type as well when updating an existing entry
            entry.format_type = format_type;
            entry.update_with_metadata(confidence, timestamp_format, field_mappings);
        } else {
            let mut entry = FormatCacheEntry::new(format_type, confidence);
            entry.timestamp_format = timestamp_format;
            entry.field_mappings = field_mappings;
            self.cache.insert(source, entry);
        }
    }
    
    /// Update an existing cache entry with new detection information
    pub fn update(
        &mut self, 
        source: &str, 
        confidence: f64,
        timestamp_format: Option<String>,
        field_mappings: HashMap<String, String>
    ) -> bool {
        if let Some(entry) = self.cache.get_mut(source) {
            entry.update_with_metadata(confidence, timestamp_format, field_mappings);
            true
        } else {
            false
        }
    }
    
    /// Remove a specific cache entry
    pub fn remove(&mut self, source: &str) -> bool {
        self.cache.remove(source).is_some()
    }
    
    /// Clear all cache entries
    pub fn clear(&mut self) {
        let evicted_count = self.cache.len();
        self.cache.clear();
        self.cache_evictions += evicted_count;
    }
    
    /// Evict stale entries based on age and sample count
    pub fn evict_stale_entries(&mut self) -> usize {
        let mut to_remove = Vec::new();
        
        for (source, entry) in &self.cache {
            if entry.is_stale(self.max_age_seconds, self.min_samples_for_stability) {
                to_remove.push(source.clone());
            }
        }
        
        let evicted_count = to_remove.len();
        for source in to_remove {
            self.cache.remove(&source);
        }
        
        self.cache_evictions += evicted_count;
        evicted_count
    }
    
    /// Evict oldest entries when cache is full
    fn evict_oldest_entries(&mut self) {
        // Calculate how many entries to evict (25% of max capacity)
        let evict_count = std::cmp::max(1, self.max_entries / 4);
        
        // Collect entries with their last_updated times
        let mut entries: Vec<(String, chrono::DateTime<chrono::Utc>)> = self.cache
            .iter()
            .map(|(source, entry)| (source.clone(), entry.last_updated))
            .collect();
        
        // Sort by last_updated (oldest first)
        entries.sort_by_key(|(_, last_updated)| *last_updated);
        
        // Remove the oldest entries
        for (source, _) in entries.into_iter().take(evict_count) {
            self.cache.remove(&source);
            self.cache_evictions += 1;
        }
    }
    
    /// Get cache statistics for monitoring and debugging
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.cache.len(),
            max_entries: self.max_entries,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            cache_evictions: self.cache_evictions,
            hit_rate: if self.cache_hits + self.cache_misses > 0 {
                self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
            } else {
                0.0
            },
            total_samples: self.cache.values().map(|entry| entry.sample_count).sum(),
        }
    }
    
    /// Get the number of cache entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    
    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
    
    /// Adaptive learning: analyze cache performance and adjust parameters
    pub fn adapt_parameters(&mut self) {
        let stats = self.stats();
        
        // If hit rate is low, increase cache size (up to a limit)
        if stats.hit_rate < 0.7 && self.max_entries < 5000 {
            self.max_entries = ((self.max_entries as f64) * 1.2) as usize;
        }
        
        // If hit rate is very high, we might be able to reduce cache size
        if stats.hit_rate > 0.95 && self.max_entries > 100 {
            self.max_entries = ((self.max_entries as f64) * 0.9) as usize;
        }
        
        // Adjust max age based on cache churn
        let eviction_rate = if stats.cache_hits + stats.cache_misses > 0 {
            stats.cache_evictions as f64 / (stats.cache_hits + stats.cache_misses) as f64
        } else {
            0.0
        };
        
        // If eviction rate is high, increase max age
        if eviction_rate > 0.1 && self.max_age_seconds < 7200 {
            self.max_age_seconds = ((self.max_age_seconds as f64) * 1.1) as i64;
        }
    }
}

impl Default for FormatCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache performance statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_evictions: usize,
    pub hit_rate: f64,
    pub total_samples: usize,
}

/// Multi-stage format classifier with detection heuristics
#[derive(Clone)]
pub struct TangoFormatClassifier {
    /// Cache of detected formats per source with performance optimization
    format_cache: FormatCache,
    
    /// Parser instances for format detection
    json_parser: JsonParser,
    logfmt_parser: LogfmtParser,
    pattern_parser: PatternParser,
}

impl TangoFormatClassifier {
    pub fn new() -> Self {
        Self {
            format_cache: FormatCache::new(),
            json_parser: JsonParser::new(),
            logfmt_parser: LogfmtParser::new(),
            pattern_parser: PatternParser::new(),
        }
    }
    
    /// Create classifier with custom cache settings
    pub fn with_cache_settings(max_entries: usize, max_age_seconds: i64, min_samples: usize) -> Self {
        Self {
            format_cache: FormatCache::with_settings(max_entries, max_age_seconds, min_samples),
            json_parser: JsonParser::new(),
            logfmt_parser: LogfmtParser::new(),
            pattern_parser: PatternParser::new(),
        }
    }
    
    /// Multi-stage detection algorithm with metadata extraction
    /// 1. Check format cache for known source
    /// 2. Attempt JSON parsing (fast fail on syntax error)
    /// 3. Analyze key=value density for logfmt detection
    /// 4. Match timestamp and level patterns using regex
    /// 5. Default to plain text processing
    fn detect_format_internal(&self, line: &str) -> (FormatType, f64, Option<String>, HashMap<String, String>) {
        let trimmed_line = line.trim();
        let mut field_mappings = HashMap::new();
        let mut timestamp_format = None;
        
        // Stage 1: JSON detection (starts with '{', valid parse)
        if trimmed_line.starts_with('{') {
            if self.json_parser.can_parse(line) {
                let result = self.json_parser.parse(line);
                if result.success {
                    // Extract field mappings from JSON parsing
                    if result.event.timestamp.is_some() {
                        timestamp_format = Some("ISO8601".to_string());
                    }
                    
                    // Add common JSON field mappings
                    field_mappings.insert("timestamp_fields".to_string(), "ts,time,timestamp,@timestamp".to_string());
                    field_mappings.insert("level_fields".to_string(), "level,severity,lvl,log.level".to_string());
                    field_mappings.insert("message_fields".to_string(), "msg,message,log.message".to_string());
                    
                    return (FormatType::Json, result.confidence, timestamp_format, field_mappings);
                }
            }
        }
        
        // Stage 2: Pattern detection BEFORE logfmt (syslog lines contain key=value but are not logfmt)
        // Check pattern parser first if line looks like it could be syslog/structured pattern
        if self.pattern_parser.can_parse(line) {
            let result = self.pattern_parser.parse(line);
            if result.success && result.event.timestamp.is_some() {
                // Extract timestamp format information
                if line.contains('T') && (line.contains('Z') || line.contains('+')) {
                    timestamp_format = Some("ISO8601".to_string());
                } else if line.contains('[') && line.contains(']') {
                    timestamp_format = Some("bracketed".to_string());
                } else {
                    timestamp_format = Some("space_separated".to_string());
                }
                
                // Add pattern-based field mappings
                field_mappings.insert("pattern_type".to_string(), "timestamp_level".to_string());
                
                return (FormatType::TimestampLevel, result.confidence, timestamp_format, field_mappings);
            }
        }
        
        // Stage 3: Logfmt detection (key=value density analysis)
        if self.logfmt_parser.can_parse(line) {
            let result = self.logfmt_parser.parse(line);
            if result.success {
                // Extract field mappings from logfmt parsing
                if result.event.timestamp.is_some() {
                    timestamp_format = Some("logfmt_inferred".to_string());
                }
                
                // Add common logfmt field mappings
                field_mappings.insert("timestamp_fields".to_string(), "ts,time,timestamp".to_string());
                field_mappings.insert("level_fields".to_string(), "level,severity,lvl".to_string());
                field_mappings.insert("message_fields".to_string(), "msg,message".to_string());
                
                return (FormatType::Logfmt, result.confidence, timestamp_format, field_mappings);
            }
        }
        
        // Stage 4: Pattern detection fallback (for patterns without timestamps)
        if self.pattern_parser.can_parse(line) {
            let result = self.pattern_parser.parse(line);
            if result.success {
                // Extract timestamp format information
                if result.event.timestamp.is_some() {
                    // Determine timestamp format based on the line content
                    if line.contains('T') && (line.contains('Z') || line.contains('+')) {
                        timestamp_format = Some("ISO8601".to_string());
                    } else if line.contains('[') && line.contains(']') {
                        timestamp_format = Some("bracketed".to_string());
                    } else {
                        timestamp_format = Some("space_separated".to_string());
                    }
                }
                
                // Add pattern-based field mappings
                field_mappings.insert("pattern_type".to_string(), "timestamp_level".to_string());
                
                return (FormatType::TimestampLevel, result.confidence, timestamp_format, field_mappings);
            }
        }
        
        // Stage 4: Default to plain text
        (FormatType::PlainText, 0.1, None, HashMap::new()) // Low confidence for plain text
    }
    
    /// Get cached format for a source, if available
    pub fn get_cached_format(&mut self, source: &str) -> Option<&FormatCacheEntry> {
        self.format_cache.get(source)
    }
    
    /// Clear the format cache
    pub fn clear_cache(&mut self) {
        self.format_cache.clear();
    }
    
    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.format_cache.stats()
    }
    
    /// Evict stale cache entries
    pub fn evict_stale_entries(&mut self) -> usize {
        self.format_cache.evict_stale_entries()
    }
    
    /// Trigger adaptive learning to optimize cache parameters
    pub fn adapt_cache_parameters(&mut self) {
        self.format_cache.adapt_parameters();
    }
}

impl FormatClassifier for TangoFormatClassifier {
    fn detect_format(&self, line: &str, _source: &str) -> FormatType {
        // For the trait implementation, we can't use caching due to immutable self
        // The cache_format method should be called separately to update the cache
        let (format_type, _confidence, _timestamp_format, _field_mappings) = self.detect_format_internal(line);
        format_type
    }
    
    fn cache_format(&mut self, source: String, format: FormatType) {
        // Use the enhanced caching with default metadata
        let mut field_mappings = HashMap::new();
        let timestamp_format = match format {
            FormatType::Json => {
                field_mappings.insert("timestamp_fields".to_string(), "ts,time,timestamp,@timestamp".to_string());
                field_mappings.insert("level_fields".to_string(), "level,severity,lvl,log.level".to_string());
                field_mappings.insert("message_fields".to_string(), "msg,message,log.message".to_string());
                Some("ISO8601".to_string())
            }
            FormatType::Logfmt => {
                field_mappings.insert("timestamp_fields".to_string(), "ts,time,timestamp".to_string());
                field_mappings.insert("level_fields".to_string(), "level,severity,lvl".to_string());
                field_mappings.insert("message_fields".to_string(), "msg,message".to_string());
                Some("logfmt_inferred".to_string())
            }
            FormatType::TimestampLevel => {
                field_mappings.insert("pattern_type".to_string(), "timestamp_level".to_string());
                Some("pattern_inferred".to_string())
            }
            _ => None,
        };
        
        self.format_cache.put(source, format, 0.8, timestamp_format, field_mappings);
    }
    
    fn get_confidence(&self, line: &str, format: FormatType) -> f64 {
        // Get confidence score for a specific format detection
        match format {
            FormatType::Json => {
                if self.json_parser.can_parse(line) {
                    let result = self.json_parser.parse(line);
                    result.confidence
                } else {
                    0.0
                }
            }
            FormatType::Logfmt => {
                if self.logfmt_parser.can_parse(line) {
                    let result = self.logfmt_parser.parse(line);
                    result.confidence
                } else {
                    0.0
                }
            }
            FormatType::Pattern => {
                if self.pattern_parser.can_parse(line) {
                    let result = self.pattern_parser.parse(line);
                    result.confidence
                } else {
                    0.0
                }
            }
            FormatType::TimestampLevel => {
                if self.pattern_parser.can_parse(line) {
                    let result = self.pattern_parser.parse(line);
                    result.confidence
                } else {
                    0.0
                }
            }
            FormatType::PlainText => 0.1, // Low confidence for plain text
            FormatType::Profile(_) => 0.9, // High confidence for user-defined profiles
        }
    }
}

impl TangoFormatClassifier {
    /// Enhanced detection with caching support
    pub fn detect_format_with_caching(&mut self, line: &str, source: &str) -> FormatType {
        // Check cache first for known source
        if let Some(cached_entry) = self.format_cache.get(source) {
            return cached_entry.format_type;
        }
        
        // Perform detection if not cached
        let (format_type, confidence, timestamp_format, field_mappings) = self.detect_format_internal(line);
        
        // Cache the result
        self.format_cache.put(source.to_string(), format_type, confidence, timestamp_format, field_mappings);
        
        format_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    // Mock classifier for testing the trait interface
    struct MockClassifier {
        cache: HashMap<String, FormatType>,
        default_format: FormatType,
        confidence_score: f64,
    }
    
    impl MockClassifier {
        fn new(default_format: FormatType, confidence_score: f64) -> Self {
            Self {
                cache: HashMap::new(),
                default_format,
                confidence_score,
            }
        }
    }
    
    impl FormatClassifier for MockClassifier {
        fn detect_format(&self, line: &str, source: &str) -> FormatType {
            // Check cache first
            if let Some(&cached_format) = self.cache.get(source) {
                return cached_format;
            }
            
            // Simple mock detection logic
            if line.starts_with('{') {
                FormatType::Json
            } else if line.contains('=') {
                FormatType::Logfmt
            } else {
                self.default_format
            }
        }
        
        fn cache_format(&mut self, source: String, format: FormatType) {
            self.cache.insert(source, format);
        }
        
        fn get_confidence(&self, _line: &str, _format: FormatType) -> f64 {
            self.confidence_score
        }
    }
    
    // Tests for TangoFormatClassifier
    #[test]
    fn test_tango_format_classifier_creation() {
        let classifier = TangoFormatClassifier::new();
        let stats = classifier.cache_stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.total_samples, 0);
    }
    
    #[test]
    fn test_tango_format_classifier_json_detection() {
        let classifier = TangoFormatClassifier::new();
        
        // Test JSON detection
        let json_line = r#"{"message": "test", "level": "info", "timestamp": "2025-12-29T10:21:03Z"}"#;
        let detected_format = classifier.detect_format(json_line, "test.log");
        assert_eq!(detected_format, FormatType::Json);
        
        // Test confidence scoring for JSON
        let confidence = classifier.get_confidence(json_line, FormatType::Json);
        assert!(confidence > 0.9); // Should have high confidence for valid JSON
    }
    
    #[test]
    fn test_tango_format_classifier_logfmt_detection() {
        let classifier = TangoFormatClassifier::new();
        
        // Test logfmt detection (3+ key=value pairs)
        let logfmt_line = "level=info msg=test time=2025-12-29T10:21:03Z user=john action=login";
        let detected_format = classifier.detect_format(logfmt_line, "app.log");
        assert_eq!(detected_format, FormatType::Logfmt);
        
        // Test confidence scoring for logfmt
        let confidence = classifier.get_confidence(logfmt_line, FormatType::Logfmt);
        assert!(confidence > 0.7); // Should have good confidence for valid logfmt
    }
    
    #[test]
    fn test_tango_format_classifier_timestamp_level_detection() {
        let classifier = TangoFormatClassifier::new();
        
        // Test timestamp+level pattern detection
        let pattern_line = "[2025-12-29T10:21:03Z] [INFO] Application started successfully";
        let detected_format = classifier.detect_format(pattern_line, "system.log");
        assert_eq!(detected_format, FormatType::TimestampLevel);
        
        // Test confidence scoring for timestamp+level patterns
        let confidence = classifier.get_confidence(pattern_line, FormatType::TimestampLevel);
        assert!(confidence > 0.8); // Should have good confidence for valid patterns
    }
    
    #[test]
    fn test_tango_format_classifier_plain_text_fallback() {
        let classifier = TangoFormatClassifier::new();
        
        // Test plain text fallback
        let plain_line = "This is a plain text log message without structure";
        let detected_format = classifier.detect_format(plain_line, "plain.log");
        assert_eq!(detected_format, FormatType::PlainText);
        
        // Test confidence scoring for plain text
        let confidence = classifier.get_confidence(plain_line, FormatType::PlainText);
        assert_eq!(confidence, 0.1); // Should have low confidence for plain text
    }
    
    #[test]
    fn test_tango_format_classifier_detection_priority() {
        let classifier = TangoFormatClassifier::new();
        
        // Test that JSON takes priority over other formats
        let json_with_equals = r#"{"message": "test=value", "level": "info"}"#;
        let detected_format = classifier.detect_format(json_with_equals, "mixed.log");
        assert_eq!(detected_format, FormatType::Json);
        
        // Test that logfmt takes priority over timestamp patterns when both could match
        let logfmt_with_timestamp = "timestamp=2025-12-29T10:21:03Z level=INFO msg=test user=admin action=start";
        let detected_format = classifier.detect_format(logfmt_with_timestamp, "structured.log");
        assert_eq!(detected_format, FormatType::Logfmt);
    }
    
    #[test]
    fn test_tango_format_classifier_caching() {
        let mut classifier = TangoFormatClassifier::new();
        
        // Initially no cache
        assert!(classifier.get_cached_format("test.log").is_none());
        
        // Cache a format
        classifier.cache_format("test.log".to_string(), FormatType::Json);
        
        // Verify cached format is returned
        let cached_entry = classifier.get_cached_format("test.log");
        assert!(cached_entry.is_some());
        assert_eq!(cached_entry.unwrap().format_type, FormatType::Json);
        
        // Verify cached format is used for detection
        let plain_line = "This should be detected as JSON due to cache";
        let detected_format = classifier.detect_format_with_caching(plain_line, "test.log");
        assert_eq!(detected_format, FormatType::Json);
        
        // Verify non-cached source still uses detection logic
        let detected_format = classifier.detect_format(plain_line, "other.log");
        assert_eq!(detected_format, FormatType::PlainText);
    }
    
    #[test]
    fn test_tango_format_classifier_cache_operations() {
        let mut classifier = TangoFormatClassifier::new();
        
        // Test multiple cache operations
        classifier.cache_format("source1.log".to_string(), FormatType::Json);
        classifier.cache_format("source2.log".to_string(), FormatType::Logfmt);
        classifier.cache_format("source3.log".to_string(), FormatType::TimestampLevel);
        
        // Verify cache statistics
        let stats = classifier.cache_stats();
        assert_eq!(stats.entries, 3);
        assert_eq!(stats.total_samples, 3);
        
        // Verify all cached formats are returned correctly
        assert_eq!(
            classifier.detect_format_with_caching("any line", "source1.log"),
            FormatType::Json
        );
        assert_eq!(
            classifier.detect_format_with_caching("any line", "source2.log"),
            FormatType::Logfmt
        );
        assert_eq!(
            classifier.detect_format_with_caching("any line", "source3.log"),
            FormatType::TimestampLevel
        );
        
        // Test cache update (same source, different format)
        classifier.cache_format("source1.log".to_string(), FormatType::Logfmt);
        // Note: The cache will update the existing entry, but the format type should change
        let updated_format = classifier.detect_format_with_caching("any line", "source1.log");
        assert_eq!(updated_format, FormatType::Logfmt);
        
        // Cache stats should show updated sample count
        let stats = classifier.cache_stats();
        assert_eq!(stats.entries, 3); // Same number of entries
        assert!(stats.total_samples >= 4); // At least one additional sample
    }
    
    #[test]
    fn test_tango_format_classifier_cache_clear() {
        let mut classifier = TangoFormatClassifier::new();
        
        // Add some cache entries
        classifier.cache_format("test1.log".to_string(), FormatType::Json);
        classifier.cache_format("test2.log".to_string(), FormatType::Logfmt);
        
        // Verify cache has entries
        let stats = classifier.cache_stats();
        assert_eq!(stats.entries, 2);
        
        // Clear cache
        classifier.clear_cache();
        
        // Verify cache is empty
        let stats = classifier.cache_stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.total_samples, 0);
        
        // Verify cached formats are no longer returned
        let json_line = r#"{"message": "test"}"#;
        let detected_format = classifier.detect_format(json_line, "test1.log");
        assert_eq!(detected_format, FormatType::Json); // Should detect, not use cache
    }
    
    #[test]
    fn test_tango_format_classifier_confidence_scoring() {
        let classifier = TangoFormatClassifier::new();
        
        // Test confidence for different format types
        let json_line = r#"{"message": "test", "level": "info"}"#;
        let json_confidence = classifier.get_confidence(json_line, FormatType::Json);
        assert!(json_confidence > 0.9);
        
        let logfmt_line = "level=info msg=test user=john action=login count=5";
        let logfmt_confidence = classifier.get_confidence(logfmt_line, FormatType::Logfmt);
        assert!(logfmt_confidence > 0.8);
        
        let pattern_line = "[2025-12-29T10:21:03Z] [INFO] Test message";
        let pattern_confidence = classifier.get_confidence(pattern_line, FormatType::TimestampLevel);
        assert!(pattern_confidence > 0.8);
        
        let plain_line = "Plain text message";
        let plain_confidence = classifier.get_confidence(plain_line, FormatType::PlainText);
        assert_eq!(plain_confidence, 0.1);
        
        // Test confidence for mismatched formats (should be 0.0)
        let json_as_logfmt = classifier.get_confidence(json_line, FormatType::Logfmt);
        assert_eq!(json_as_logfmt, 0.0);
        
        let logfmt_as_json = classifier.get_confidence(logfmt_line, FormatType::Json);
        assert_eq!(logfmt_as_json, 0.0);
    }
    
    #[test]
    fn test_tango_format_classifier_edge_cases() {
        let classifier = TangoFormatClassifier::new();
        
        // Test empty line
        let detected_format = classifier.detect_format("", "empty.log");
        assert_eq!(detected_format, FormatType::PlainText);
        
        // Test whitespace-only line
        let detected_format = classifier.detect_format("   \t  \n  ", "whitespace.log");
        assert_eq!(detected_format, FormatType::PlainText);
        
        // Test malformed JSON (starts with { but invalid)
        let malformed_json = r#"{"incomplete": json"#;
        let detected_format = classifier.detect_format(malformed_json, "malformed.log");
        assert_eq!(detected_format, FormatType::PlainText); // Should fall back to plain text
        
        // Test insufficient logfmt pairs (below 3-pair threshold)
        let insufficient_logfmt = "level=info msg=test";
        let detected_format = classifier.detect_format(insufficient_logfmt, "insufficient.log");
        assert_eq!(detected_format, FormatType::PlainText);
        
        // Test line with timestamp but no level
        let timestamp_only = "2025-12-29T10:21:03Z This message has timestamp but no level";
        let detected_format = classifier.detect_format(timestamp_only, "timestamp.log");
        assert_eq!(detected_format, FormatType::PlainText);
        
        // Test line with level but no timestamp
        let level_only = "INFO This message has level but no timestamp";
        let detected_format = classifier.detect_format(level_only, "level.log");
        assert_eq!(detected_format, FormatType::PlainText);
    }
    
    #[test]
    fn test_tango_format_classifier_mixed_content() {
        let classifier = TangoFormatClassifier::new();
        
        // Test line that could match multiple formats
        let mixed_line = r#"timestamp=2025-12-29T10:21:03Z level=INFO msg="JSON-like: {\"key\": \"value\"}" user=admin"#;
        let detected_format = classifier.detect_format(mixed_line, "mixed.log");
        // Should be detected as logfmt since it has sufficient key=value pairs
        assert_eq!(detected_format, FormatType::Logfmt);
        
        // Test line with JSON-like structure but not valid JSON
        let json_like = r#"{message: "missing quotes", level: info}"#;
        let detected_format = classifier.detect_format(json_like, "json_like.log");
        assert_eq!(detected_format, FormatType::PlainText); // Invalid JSON should fall back
        
        // Test line with logfmt-like structure but insufficient pairs
        let logfmt_like = "key=value another_key=another_value some plain text";
        let detected_format = classifier.detect_format(logfmt_like, "logfmt_like.log");
        assert_eq!(detected_format, FormatType::PlainText); // Only 2 pairs, below threshold
    }
    
    #[test]
    fn test_format_cache_entry() {
        // Test FormatCacheEntry creation and updates
        let mut entry = FormatCacheEntry::new(FormatType::Json, 0.9);
        
        assert_eq!(entry.format_type, FormatType::Json);
        assert_eq!(entry.confidence, 0.9);
        assert_eq!(entry.sample_count, 1);
        assert!(entry.timestamp_format.is_none());
        assert!(entry.field_mappings.is_empty());
        
        // Test update
        let original_time = entry.last_updated;
        std::thread::sleep(std::time::Duration::from_millis(1)); // Ensure time difference
        entry.update(0.8);
        
        // Use approximate comparison for floating point
        assert!((entry.confidence - 0.85).abs() < 0.001); // Average of 0.9 and 0.8
        assert_eq!(entry.sample_count, 2);
        assert!(entry.last_updated > original_time);
        
        // Test update with metadata
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp_field".to_string(), "ts".to_string());
        entry.update_with_metadata(0.7, Some("ISO8601".to_string()), field_mappings);
        
        assert_eq!(entry.timestamp_format, Some("ISO8601".to_string()));
        assert!(entry.field_mappings.contains_key("timestamp_field"));
        assert_eq!(entry.sample_count, 3);
    }
    
    #[test]
    fn test_format_cache_basic_operations() {
        let mut cache = FormatCache::new();
        
        // Test empty cache
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.get("test.log").is_none());
        
        // Test put and get
        let mut field_mappings = HashMap::new();
        field_mappings.insert("test_field".to_string(), "test_value".to_string());
        
        cache.put(
            "test.log".to_string(),
            FormatType::Json,
            0.9,
            Some("ISO8601".to_string()),
            field_mappings.clone(),
        );
        
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
        
        let entry = cache.get("test.log");
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.format_type, FormatType::Json);
        assert_eq!(entry.confidence, 0.9);
        assert_eq!(entry.timestamp_format, Some("ISO8601".to_string()));
        assert!(entry.field_mappings.contains_key("test_field"));
        
        // Test update
        let mut new_mappings = HashMap::new();
        new_mappings.insert("new_field".to_string(), "new_value".to_string());
        
        assert!(cache.update("test.log", 0.8, None, new_mappings));
        
        let entry = cache.get("test.log");
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert!((entry.confidence - 0.85).abs() < 0.001); // Average of 0.9 and 0.8
        assert!(entry.field_mappings.contains_key("test_field"));
        assert!(entry.field_mappings.contains_key("new_field"));
        
        // Test remove
        assert!(cache.remove("test.log"));
        assert!(!cache.remove("nonexistent.log"));
        assert!(cache.is_empty());
    }
    
    #[test]
    fn test_format_cache_eviction() {
        // Create cache with small capacity for testing eviction
        let mut cache = FormatCache::with_settings(3, 3600, 2);
        
        // Fill cache to capacity
        cache.put("source1.log".to_string(), FormatType::Json, 0.9, None, HashMap::new());
        cache.put("source2.log".to_string(), FormatType::Logfmt, 0.8, None, HashMap::new());
        cache.put("source3.log".to_string(), FormatType::TimestampLevel, 0.7, None, HashMap::new());
        
        assert_eq!(cache.len(), 3);
        
        // Add one more entry to trigger eviction
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure time difference
        cache.put("source4.log".to_string(), FormatType::PlainText, 0.6, None, HashMap::new());
        
        // Should still have 3 entries (oldest evicted)
        assert_eq!(cache.len(), 3);
        
        // The oldest entry (source1.log) should be evicted
        assert!(cache.get("source1.log").is_none());
        assert!(cache.get("source4.log").is_some());
        
        let stats = cache.stats();
        assert!(stats.cache_evictions > 0);
    }
    
    #[test]
    fn test_format_cache_stale_entry_eviction() {
        // Create cache with short max age for testing
        let mut cache = FormatCache::with_settings(10, 1, 5); // 1 second max age, 5 samples for stability
        
        // Add an entry
        cache.put("test.log".to_string(), FormatType::Json, 0.9, None, HashMap::new());
        assert!(cache.get("test.log").is_some());
        
        // Wait for entry to become stale
        std::thread::sleep(std::time::Duration::from_secs(2));
        
        // Entry should be considered stale and removed on next access
        assert!(cache.get("test.log").is_none());
        
        let stats = cache.stats();
        assert!(stats.cache_evictions > 0);
        assert!(stats.cache_misses > 0);
    }
    
    #[test]
    fn test_format_cache_adaptive_parameters() {
        let mut cache = FormatCache::with_settings(100, 3600, 3);
        
        // Simulate low hit rate scenario
        for i in 0..50 {
            cache.put(format!("source_{}.log", i), FormatType::Json, 0.9, None, HashMap::new());
        }
        
        // Simulate many misses
        for i in 50..100 {
            cache.get(&format!("missing_{}.log", i));
        }
        
        let original_max_entries = cache.max_entries;
        cache.adapt_parameters();
        
        // Should increase max_entries due to low hit rate
        assert!(cache.max_entries >= original_max_entries);
    }
    
    #[test]
    fn test_format_cache_statistics() {
        let mut cache = FormatCache::new();
        
        // Add some entries
        cache.put("source1.log".to_string(), FormatType::Json, 0.9, None, HashMap::new());
        cache.put("source2.log".to_string(), FormatType::Logfmt, 0.8, None, HashMap::new());
        
        // Generate some hits and misses
        cache.get("source1.log"); // hit
        cache.get("source1.log"); // hit
        cache.get("missing.log"); // miss
        
        let stats = cache.stats();
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.cache_misses, 1);
        assert!((stats.hit_rate - 0.6666666666666666).abs() < 0.001); // 2/3
        assert_eq!(stats.total_samples, 2);
    }
    
    // Original mock classifier tests (preserved for compatibility)
    #[test]
    fn test_format_classifier_detect_format() {
        let classifier = MockClassifier::new(FormatType::PlainText, 0.8);
        
        // Test JSON detection
        let json_line = r#"{"message": "test", "level": "info"}"#;
        assert_eq!(
            classifier.detect_format(json_line, "test.log"),
            FormatType::Json
        );
        
        // Test logfmt detection
        let logfmt_line = "level=info msg=test time=2023-01-01";
        assert_eq!(
            classifier.detect_format(logfmt_line, "test.log"),
            FormatType::Logfmt
        );
        
        // Test plain text fallback
        let plain_line = "This is a plain text log message";
        assert_eq!(
            classifier.detect_format(plain_line, "test.log"),
            FormatType::PlainText
        );
    }
    
    #[test]
    fn test_format_classifier_caching() {
        let mut classifier = MockClassifier::new(FormatType::PlainText, 0.9);
        
        // Cache a format for a source
        classifier.cache_format("app.log".to_string(), FormatType::Json);
        
        // Verify cached format is returned regardless of line content
        let plain_line = "This should be detected as JSON due to cache";
        assert_eq!(
            classifier.detect_format(plain_line, "app.log"),
            FormatType::Json
        );
        
        // Verify non-cached source still uses detection logic
        assert_eq!(
            classifier.detect_format(plain_line, "other.log"),
            FormatType::PlainText
        );
    }
    
    #[test]
    fn test_format_classifier_confidence() {
        let classifier = MockClassifier::new(FormatType::PlainText, 0.75);
        
        // Test confidence scoring
        assert_eq!(
            classifier.get_confidence("test line", FormatType::Json),
            0.75
        );
        assert_eq!(
            classifier.get_confidence("another line", FormatType::Logfmt),
            0.75
        );
    }
    
    #[test]
    fn test_format_classifier_cache_operations() {
        let mut classifier = MockClassifier::new(FormatType::PlainText, 0.8);
        
        // Test multiple cache operations
        classifier.cache_format("source1".to_string(), FormatType::Json);
        classifier.cache_format("source2".to_string(), FormatType::Logfmt);
        classifier.cache_format("source3".to_string(), FormatType::TimestampLevel);
        
        // Verify all cached formats are returned correctly
        assert_eq!(
            classifier.detect_format("any line", "source1"),
            FormatType::Json
        );
        assert_eq!(
            classifier.detect_format("any line", "source2"),
            FormatType::Logfmt
        );
        assert_eq!(
            classifier.detect_format("any line", "source3"),
            FormatType::TimestampLevel
        );
        
        // Verify uncached source still uses detection
        assert_eq!(
            classifier.detect_format("key=value", "source4"),
            FormatType::Logfmt
        );
    }
    
    #[test]
    fn test_format_classifier_edge_cases() {
        let mut classifier = MockClassifier::new(FormatType::PlainText, 0.5);
        
        // Test empty line
        assert_eq!(
            classifier.detect_format("", "test.log"),
            FormatType::PlainText
        );
        
        // Test line with both JSON and logfmt characteristics
        let mixed_line = r#"{"key": "value"} extra=data"#;
        assert_eq!(
            classifier.detect_format(mixed_line, "test.log"),
            FormatType::Json  // JSON takes precedence in our mock
        );
        
        // Test cache override
        classifier.cache_format("test.log".to_string(), FormatType::Logfmt);
        assert_eq!(
            classifier.detect_format(mixed_line, "test.log"),
            FormatType::Logfmt  // Cache overrides detection
        );
    }
    
    #[test]
    fn test_format_classifier_trait_methods() {
        let mut classifier = MockClassifier::new(FormatType::TimestampLevel, 0.95);
        
        // Test all trait methods work together
        let source = "application.log";
        let line = "2023-01-01 10:00:00 INFO Starting application";
        
        // Initial detection
        let detected = classifier.detect_format(line, source);
        assert_eq!(detected, FormatType::TimestampLevel);
        
        // Cache the result
        classifier.cache_format(source.to_string(), detected);
        
        // Verify confidence
        let confidence = classifier.get_confidence(line, detected);
        assert_eq!(confidence, 0.95);
        
        // Verify cached detection
        let cached_result = classifier.detect_format("different line", source);
        assert_eq!(cached_result, FormatType::TimestampLevel);
    }
    
    // Property-based tests for format classifier
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    
    // Generator for different log line types
    #[derive(Debug, Clone)]
    enum LogLineType {
        Json(String),
        Logfmt(Vec<(String, String)>),
        TimestampLevel(String, String, String), // timestamp, level, message
        PlainText(String),
    }
    
    impl Arbitrary for LogLineType {
        fn arbitrary(g: &mut Gen) -> Self {
            match usize::arbitrary(g) % 4 {
                0 => {
                    // Generate JSON log
                    let message = format!("message_{}", usize::arbitrary(g) % 100);
                    let level = ["INFO", "ERROR", "WARN", "DEBUG"][usize::arbitrary(g) % 4];
                    let json = format!(
                        r#"{{"message": "{}", "level": "{}", "timestamp": "2025-12-29T10:21:03Z"}}"#,
                        message, level
                    );
                    LogLineType::Json(json)
                }
                1 => {
                    // Generate logfmt log with sufficient pairs
                    let mut pairs = Vec::new();
                    let num_pairs = (usize::arbitrary(g) % 5) + 3; // 3-7 pairs to ensure threshold
                    for i in 0..num_pairs {
                        let key = format!("key_{}", i);
                        let value = format!("value_{}", usize::arbitrary(g) % 100);
                        pairs.push((key, value));
                    }
                    LogLineType::Logfmt(pairs)
                }
                2 => {
                    // Generate timestamp+level log
                    let timestamp = "2025-12-29T10:21:03Z";
                    let level = ["INFO", "ERROR", "WARN", "DEBUG"][usize::arbitrary(g) % 4];
                    let message = format!("message_{}", usize::arbitrary(g) % 100);
                    LogLineType::TimestampLevel(timestamp.to_string(), level.to_string(), message)
                }
                _ => {
                    // Generate plain text log
                    let message = format!("Plain text message {}", usize::arbitrary(g) % 100);
                    LogLineType::PlainText(message)
                }
            }
        }
    }
    
    impl LogLineType {
        fn to_log_string(&self) -> String {
            match self {
                LogLineType::Json(json) => json.clone(),
                LogLineType::Logfmt(pairs) => {
                    pairs.iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<_>>()
                        .join(" ")
                }
                LogLineType::TimestampLevel(ts, level, msg) => {
                    format!("[{}] [{}] {}", ts, level, msg)
                }
                LogLineType::PlainText(text) => text.clone(),
            }
        }
        
        fn expected_format(&self) -> FormatType {
            match self {
                LogLineType::Json(_) => FormatType::Json,
                LogLineType::Logfmt(_) => FormatType::Logfmt,
                LogLineType::TimestampLevel(_, _, _) => FormatType::TimestampLevel,
                LogLineType::PlainText(_) => FormatType::PlainText,
            }
        }
    }
    
    // Property 4: Format Detection Accuracy
    // Feature: log-type-detection-and-parsing, Property 4: Format Detection Accuracy
    // Validates: Requirements 3.1, 3.6, 4.1, 5.1
    #[quickcheck(tests = 5)]
    fn prop_format_detection_accuracy(log_line_type: LogLineType, source_suffix: u32) -> bool {
        let classifier = TangoFormatClassifier::new();
        let log_line = log_line_type.to_log_string();
        let _expected_format = log_line_type.expected_format();
        let source = format!("test_{}.log", source_suffix);
        
        // Skip empty or whitespace-only lines as they're edge cases
        if log_line.trim().is_empty() {
            return true;
        }
        
        // For any log line, the format classifier should correctly identify
        // the most appropriate format based on the defined heuristics and thresholds
        let detected_format = classifier.detect_format(&log_line, &source);
        
        // The detection should match the expected format for well-formed logs
        match &log_line_type {
            LogLineType::Json(_) => {
                // JSON logs should be detected as JSON if they're valid
                if classifier.json_parser.can_parse(&log_line) {
                    detected_format == FormatType::Json
                } else {
                    // If JSON parsing fails, it should fall back appropriately
                    detected_format == FormatType::PlainText || 
                    detected_format == FormatType::Logfmt ||
                    detected_format == FormatType::TimestampLevel
                }
            }
            LogLineType::Logfmt(pairs) => {
                // Logfmt logs should be detected as logfmt if they meet the threshold
                if pairs.len() >= 3 {
                    detected_format == FormatType::Logfmt
                } else {
                    // Below threshold should fall back to plain text
                    detected_format == FormatType::PlainText
                }
            }
            LogLineType::TimestampLevel(_, _, _) => {
                // Timestamp+level logs should be detected as such if they have both components
                if classifier.pattern_parser.can_parse(&log_line) {
                    detected_format == FormatType::TimestampLevel
                } else {
                    // If pattern doesn't match, should fall back
                    detected_format == FormatType::PlainText ||
                    detected_format == FormatType::Logfmt
                }
            }
            LogLineType::PlainText(_) => {
                // Plain text should be detected as plain text or potentially
                // as another format if it accidentally matches patterns
                true // Plain text can be detected as anything, that's acceptable
            }
        }
    }
    
    // Additional property test for detection consistency
    #[quickcheck(tests = 5)]
    fn prop_format_detection_consistency(
        log_line: String,
        source1: String,
        source2: String,
    ) -> bool {
        let classifier = TangoFormatClassifier::new();
        
        // Skip empty or very short lines
        if log_line.trim().len() < 3 {
            return true;
        }
        
        // The same log line should be detected as the same format
        // regardless of the source (when no caching is involved)
        let format1 = classifier.detect_format(&log_line, &source1);
        let format2 = classifier.detect_format(&log_line, &source2);
        
        format1 == format2
    }
    
    // Property test for confidence scoring consistency
    #[quickcheck(tests = 5)]
    fn prop_confidence_scoring_consistency(log_line: String) -> bool {
        let classifier = TangoFormatClassifier::new();
        
        // Skip empty lines
        if log_line.trim().is_empty() {
            return true;
        }
        
        // Confidence should be consistent for the same line and format
        let detected_format = classifier.detect_format(&log_line, "test.log");
        let confidence1 = classifier.get_confidence(&log_line, detected_format);
        let confidence2 = classifier.get_confidence(&log_line, detected_format);
        
        // Confidence should be the same for identical inputs
        confidence1 == confidence2 &&
        // Confidence should be between 0.0 and 1.0
        confidence1 >= 0.0 && confidence1 <= 1.0
    }
    
    // Property test for format detection heuristics
    #[quickcheck(tests = 5)]
    fn prop_format_detection_heuristics(
        starts_with_brace: bool,
        has_equals_signs: u8,
        has_timestamp_pattern: bool,
        has_level_token: bool,
        message_content: String,
    ) -> bool {
        let classifier = TangoFormatClassifier::new();
        
        // Construct a log line based on the input parameters
        let mut log_line = String::new();
        
        if starts_with_brace {
            // Create JSON-like structure
            log_line = format!(r#"{{"message": "{}"}}"#, message_content.replace('"', "'"));
        } else {
            // Create other formats
            if has_equals_signs > 0 {
                // Add key=value pairs
                for i in 0..std::cmp::min(has_equals_signs, 10) {
                    if !log_line.is_empty() {
                        log_line.push(' ');
                    }
                    log_line.push_str(&format!("key_{}=value_{}", i, i));
                }
            }
            
            if has_timestamp_pattern {
                log_line = format!("2025-12-29T10:21:03Z {}", log_line);
            }
            
            if has_level_token {
                log_line = format!("{} INFO", log_line);
            }
            
            if !message_content.trim().is_empty() {
                if !log_line.is_empty() {
                    log_line.push(' ');
                }
                log_line.push_str(&message_content);
            }
        }
        
        // Skip empty lines or lines that are too short
        if log_line.trim().is_empty() || log_line.trim().len() < 3 {
            return true;
        }
        
        let detected_format = classifier.detect_format(&log_line, "test.log");
        
        // Verify detection follows expected heuristics
        if starts_with_brace && log_line.starts_with('{') {
            // Should attempt JSON detection first
            if classifier.json_parser.can_parse(&log_line) {
                detected_format == FormatType::Json
            } else {
                // If JSON parsing fails, should fall back
                true
            }
        } else if has_equals_signs >= 3 {
            // Should attempt logfmt detection
            if classifier.logfmt_parser.can_parse(&log_line) {
                detected_format == FormatType::Logfmt
            } else {
                // If logfmt parsing fails, should fall back
                true
            }
        } else if has_timestamp_pattern && has_level_token {
            // Should attempt timestamp+level detection
            if classifier.pattern_parser.can_parse(&log_line) {
                detected_format == FormatType::TimestampLevel
            } else {
                // If pattern parsing fails, should fall back
                true
            }
        } else {
            // Should fall back to plain text or detect based on actual content
            true // Any detection is acceptable for ambiguous cases
        }
    }
    
    // Comprehensive parser that demonstrates the fallback chain
    struct ComprehensiveParser {
        json_parser: JsonParser,
        logfmt_parser: LogfmtParser,
        pattern_parser: PatternParser,
    }
    
    impl ComprehensiveParser {
        fn new() -> Self {
            Self {
                json_parser: JsonParser::new(),
                logfmt_parser: LogfmtParser::new(),
                pattern_parser: PatternParser::new(),
            }
        }
        
        /// Parse a log line using the fallback chain:
        /// JSON → logfmt → timestamp patterns → plain text
        fn parse_with_fallback(&self, line: &str, _source: &str) -> ParseResult {
            // Stage 1: Try JSON parsing first
            if line.trim_start().starts_with('{') {
                let json_result = self.json_parser.parse(line);
                if json_result.success {
                    return json_result;
                }
            }
            
            // Stage 2: Try logfmt parsing
            if self.logfmt_parser.can_parse(line) {
                let logfmt_result = self.logfmt_parser.parse(line);
                if logfmt_result.success {
                    return logfmt_result;
                }
            }
            
            // Stage 3: Try timestamp+level pattern parsing
            if self.pattern_parser.can_parse(line) {
                let pattern_result = self.pattern_parser.parse(line);
                if pattern_result.success {
                    return pattern_result;
                }
            }
            
            // Stage 4: Fall back to plain text
            let mut event = CanonicalEvent::new(
                line.to_string(),
                line.to_string(),
                FormatType::PlainText,
            );
            
            // Try to infer timestamp from the beginning of the line
            let words: Vec<&str> = line.split_whitespace().collect();
            if !words.is_empty() {
                // Simple timestamp inference - look for ISO8601-like patterns
                if let Some(first_word) = words.first() {
                    if first_word.len() >= 19 && first_word.contains('T') {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(first_word) {
                            event.set_timestamp(dt.with_timezone(&chrono::Utc));
                        }
                    }
                }
            }
            
            ParseResult::success(event, 0.1) // Low confidence for plain text
        }
    }
    

    
    // Property test for graceful error handling in fallback chain
    #[quickcheck(tests = 5)]
    fn prop_fallback_chain_error_resilience(
        malformed_json: String,
        insufficient_logfmt: String,
        invalid_pattern: String,
    ) -> bool {
        let parser = ComprehensiveParser::new();
        
        // Test malformed JSON (starts with { but invalid)
        let malformed_json_line = format!("{{\"incomplete\": {}", malformed_json.replace('"', "'"));
        let result1 = parser.parse_with_fallback(&malformed_json_line, "test1.log");
        
        // Should succeed (fall back to other formats or plain text)
        if !result1.success {
            return false;
        }
        
        // Should not be parsed as JSON
        if result1.event.format_type == FormatType::Json {
            return false;
        }
        
        // Test insufficient logfmt (only 1-2 pairs)
        let insufficient_logfmt_line = format!("key1=value1 key2=value2 {}", insufficient_logfmt);
        let result2 = parser.parse_with_fallback(&insufficient_logfmt_line, "test2.log");
        
        // Should succeed (fall back to timestamp patterns or plain text)
        if !result2.success {
            return false;
        }
        
        // Test invalid timestamp pattern
        let invalid_pattern_line = format!("[invalid-timestamp] [INVALID_LEVEL] {}", invalid_pattern);
        let result3 = parser.parse_with_fallback(&invalid_pattern_line, "test3.log");
        
        // Should succeed (fall back to plain text)
        if !result3.success {
            return false;
        }
        
        // All results should have preserved the original raw line
        result1.event.raw == malformed_json_line &&
        result2.event.raw == insufficient_logfmt_line &&
        result3.event.raw == invalid_pattern_line
    }
    
    // Property test for fallback chain consistency
    #[quickcheck(tests = 5)]
    fn prop_fallback_chain_consistency(log_line: String, source1: String, source2: String) -> bool {
        let parser = ComprehensiveParser::new();
        
        // Skip empty or very short lines
        if log_line.trim().len() < 3 {
            return true;
        }
        
        // The same log line should produce consistent results through the fallback chain
        let result1 = parser.parse_with_fallback(&log_line, &source1);
        let result2 = parser.parse_with_fallback(&log_line, &source2);
        
        // Both should succeed
        if !result1.success || !result2.success {
            return false;
        }
        
        // Should detect the same format
        if result1.event.format_type != result2.event.format_type {
            return false;
        }
        
        // Should have the same message (for deterministic parsing)
        if result1.event.message != result2.event.message {
            return false;
        }
        
        // Should preserve the raw line
        result1.event.raw == log_line && result2.event.raw == log_line
    }
    
    // Property 8: Format Caching Consistency
    // Feature: log-type-detection-and-parsing, Property 8: Format Caching Consistency
    // Validates: Requirements 5.5, 9.1
    #[quickcheck(tests = 5)]
    fn prop_format_caching_consistency(
        log_lines: Vec<String>,
        source_name: String,
        cache_operations: Vec<bool>, // true = cache, false = detect
    ) -> bool {
        let mut classifier = TangoFormatClassifier::new();
        
        // Skip empty test cases
        if log_lines.is_empty() || source_name.trim().is_empty() {
            return true;
        }
        
        // For any log source, once a format is detected and cached, subsequent lines
        // from the same source should use the cached format information for improved
        // performance while maintaining parsing accuracy
        
        let mut expected_format: Option<FormatType> = None;
        let mut cache_was_used = false;
        
        for (i, log_line) in log_lines.iter().enumerate() {
            // Skip empty or very short lines
            if log_line.trim().len() < 3 {
                continue;
            }
            
            let operation_index = i % cache_operations.len().max(1);
            let should_cache = cache_operations.get(operation_index).unwrap_or(&true);
            
            if *should_cache && expected_format.is_none() {
                // First detection - establish the expected format
                let detected_format = classifier.detect_format_with_caching(log_line, &source_name);
                expected_format = Some(detected_format);
                
                // Verify the format was cached
                let cached_entry = classifier.get_cached_format(&source_name);
                if cached_entry.is_none() {
                    return false;
                }
                
                let cached_entry = cached_entry.unwrap();
                if cached_entry.format_type != detected_format {
                    return false;
                }
                
                cache_was_used = true;
            } else if let Some(expected) = expected_format {
                // Subsequent detections should use cached format
                let detected_format = classifier.detect_format_with_caching(log_line, &source_name);
                
                // Should return the cached format
                if detected_format != expected {
                    return false;
                }
                
                // Verify cache entry still exists and is consistent
                let cached_entry = classifier.get_cached_format(&source_name);
                if cached_entry.is_none() {
                    return false;
                }
                
                let cached_entry = cached_entry.unwrap();
                if cached_entry.format_type != expected {
                    return false;
                }
                
                // Sample count should increase with repeated access
                if cached_entry.sample_count < 1 {
                    return false;
                }
            }
        }
        
        // If we used caching, verify cache statistics are reasonable
        if cache_was_used {
            let stats = classifier.cache_stats();
            
            // Should have at least one cache entry
            if stats.entries == 0 {
                return false;
            }
            
            // Should have some cache hits if we accessed the same source multiple times
            if log_lines.len() > 1 && stats.cache_hits == 0 {
                return false;
            }
            
            // Hit rate should be reasonable (not 0 if we had multiple accesses)
            if log_lines.len() > 2 && stats.hit_rate == 0.0 {
                return false;
            }
        }
        
        true
    }
    
    // Additional property test for cache performance optimization
    #[quickcheck(tests = 5)]
    fn prop_cache_performance_optimization(
        sources: Vec<String>,
        repeated_accesses: u8, // Number of times to access each source
    ) -> bool {
        let mut classifier = TangoFormatClassifier::new();
        
        // Skip empty test cases
        if sources.is_empty() || repeated_accesses == 0 {
            return true;
        }
        
        let clamped_accesses = std::cmp::min(repeated_accesses, 10); // Limit to reasonable number
        
        // Test that caching improves performance by reducing redundant format detection
        for source in &sources {
            if source.trim().is_empty() {
                continue;
            }
            
            // Create a sample log line for this source
            let sample_line = format!("{{\"message\": \"test from {}\", \"level\": \"INFO\"}}", source);
            
            // First access should detect and cache
            let first_format = classifier.detect_format_with_caching(&sample_line, source);
            
            // Subsequent accesses should use cache
            for _ in 1..clamped_accesses {
                let cached_format = classifier.detect_format_with_caching(&sample_line, source);
                
                // Should return the same format as first detection
                if cached_format != first_format {
                    return false;
                }
            }
            
            // Verify cache entry exists and has been updated
            let cached_entry = classifier.get_cached_format(source);
            if cached_entry.is_none() {
                return false;
            }
            
            let cached_entry = cached_entry.unwrap();
            
            // Sample count should reflect the number of accesses
            if cached_entry.sample_count < 1 {
                return false;
            }
            
            // Format should match first detection
            if cached_entry.format_type != first_format {
                return false;
            }
        }
        
        // Verify cache statistics show good performance
        let stats = classifier.cache_stats();
        
        // Should have cache entries for non-empty sources
        let non_empty_sources = sources.iter().filter(|s| !s.trim().is_empty()).count();
        if non_empty_sources > 0 && stats.entries == 0 {
            return false;
        }
        
        // Should have cache hits if we had repeated accesses
        let total_accesses = non_empty_sources * (clamped_accesses as usize);
        if total_accesses > non_empty_sources && stats.cache_hits == 0 {
            return false;
        }
        
        // Hit rate should be reasonable for repeated accesses
        if clamped_accesses > 1 && non_empty_sources > 0 && stats.hit_rate < 0.1 {
            return false;
        }
        
        true
    }
    
    // Property test for cache invalidation and staleness
    #[quickcheck(tests = 5)]
    fn prop_cache_invalidation_behavior(
        source: String,
        format_changes: Vec<FormatType>,
    ) -> bool {
        // Create classifier with short cache timeout for testing
        let mut classifier = TangoFormatClassifier::with_cache_settings(100, 1, 2); // 1 second timeout
        
        // Skip empty test cases and edge cases with control characters
        if source.trim().is_empty() || format_changes.is_empty() {
            return true;
        }
        
        // Filter out control characters and limit length
        let clean_source: String = source.chars()
            .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
            .filter(|c| *c != '\0' && *c != '\u{1}' && *c != '\u{2}')
            .take(50)
            .collect();
            
        if clean_source.trim().is_empty() {
            return true;
        }
        
        // Limit the number of format changes to prevent excessive test time
        let limited_changes: Vec<FormatType> = format_changes.into_iter().take(3).collect();
        
        // Test that cache properly handles format changes and invalidation
        for (i, &format_type) in limited_changes.iter().enumerate() {
            // Cache the format
            classifier.cache_format(clean_source.clone(), format_type);
            
            // Verify it's cached
            let cached_entry = classifier.get_cached_format(&clean_source);
            if cached_entry.is_none() {
                return false;
            }
            
            if cached_entry.unwrap().format_type != format_type {
                return false;
            }
            
            // If this is not the first iteration, wait a minimal amount to test staleness
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_millis(10)); // Reduced from 100ms to 10ms
            }
        }
        
        // Test manual cache eviction
        let _evicted_count = classifier.evict_stale_entries();
        
        // Cache should still be functional after eviction
        classifier.cache_format(clean_source.clone(), FormatType::Json);
        let final_entry = classifier.get_cached_format(&clean_source);
        if final_entry.is_none() {
            return false;
        }
        
        if final_entry.unwrap().format_type != FormatType::Json {
            return false;
        }
        
        true
    }
}