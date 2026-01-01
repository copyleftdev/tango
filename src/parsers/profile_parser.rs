use crate::models::*;
use crate::error::ParseError;
use crate::parse_result::ParseResult;
use crate::parsers::LogParser;
use crate::profiles::*;
use std::sync::Arc;

/// Profile-based parser that uses user-defined parsing configurations
pub struct ProfileParser {
    profile: Arc<dyn Profile>,
}

impl ProfileParser {
    /// Create a new profile parser with a regex profile
    pub fn new_regex(config: RegexProfileConfig) -> Result<Self, ParseError> {
        let profile = RegexProfile::new(config)?;
        Ok(Self {
            profile: Arc::new(profile),
        })
    }
    
    /// Create a new profile parser with a CSV profile
    pub fn new_csv(config: CsvProfileConfig) -> Result<Self, ParseError> {
        let profile = CsvProfile::new(config)?;
        Ok(Self {
            profile: Arc::new(profile),
        })
    }
    
    /// Create a new profile parser with an Apache profile
    pub fn new_apache() -> Self {
        let profile = ApacheProfile::new();
        Self {
            profile: Arc::new(profile),
        }
    }
    
    /// Create a new profile parser with an Nginx profile
    pub fn new_nginx() -> Self {
        let profile = NginxProfile::new();
        Self {
            profile: Arc::new(profile),
        }
    }
    
    /// Create a new profile parser with a Syslog profile
    pub fn new_syslog() -> Self {
        let profile = SyslogProfile::new();
        Self {
            profile: Arc::new(profile),
        }
    }
    
    /// Create a profile parser from any profile implementation
    pub fn from_profile(profile: Arc<dyn Profile>) -> Self {
        Self { profile }
    }
    
    /// Get the underlying profile
    pub fn get_profile(&self) -> &Arc<dyn Profile> {
        &self.profile
    }
    
    /// Validate the profile configuration
    pub fn validate(&self) -> Result<(), ParseError> {
        self.profile.validate()
    }
}

impl LogParser for ProfileParser {
    fn parse(&self, line: &str) -> ParseResult {
        self.profile.parse(line)
    }
    
    fn can_parse(&self, line: &str) -> bool {
        self.profile.can_parse(line)
    }
    
    fn get_format_type(&self) -> FormatType {
        FormatType::Profile(self.profile.get_profile_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_profile_parser_regex() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("timestamp".to_string(), 1);
        field_mappings.insert("level".to_string(), 2);
        field_mappings.insert("message".to_string(), 3);
        
        let config = RegexProfileConfig {
            name: "test_profile".to_string(),
            pattern: r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) \[(\w+)\] (.+)$".to_string(),
            field_mappings,
            timestamp_field: Some("timestamp".to_string()),
            level_field: Some("level".to_string()),
            message_field: Some("message".to_string()),
            timestamp_format: None,
        };
        
        let parser = ProfileParser::new_regex(config).unwrap();
        let result = parser.parse("2025-12-30T10:21:03Z [INFO] Test message");
        
        assert!(result.success);
        assert_eq!(result.event.message, "Test message");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
        assert_eq!(parser.get_format_type(), FormatType::Profile(ProfileType::Regex));
    }
    
    #[test]
    fn test_profile_parser_csv() {
        let mut column_mappings = HashMap::new();
        column_mappings.insert("timestamp".to_string(), 0);
        column_mappings.insert("level".to_string(), 1);
        column_mappings.insert("message".to_string(), 2);
        
        let config = CsvProfileConfig {
            name: "test_csv".to_string(),
            delimiter: ',',
            has_header: false,
            column_mappings,
            timestamp_column: Some("timestamp".to_string()),
            level_column: Some("level".to_string()),
            message_column: Some("message".to_string()),
            timestamp_format: None,
        };
        
        let parser = ProfileParser::new_csv(config).unwrap();
        let result = parser.parse("2025-12-30T10:21:03Z,INFO,Test message");
        
        assert!(result.success);
        assert_eq!(result.event.message, "Test message");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
        assert_eq!(parser.get_format_type(), FormatType::Profile(ProfileType::Csv));
    }
    
    #[test]
    fn test_profile_parser_apache() {
        let parser = ProfileParser::new_apache();
        let log_line = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326"#;
        
        let result = parser.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "GET /apache_pb.gif HTTP/1.0");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
        assert_eq!(parser.get_format_type(), FormatType::Profile(ProfileType::Apache));
    }
    
    #[test]
    fn test_profile_parser_nginx() {
        let parser = ProfileParser::new_nginx();
        let log_line = r#"127.0.0.1 - - [10/Oct/2000:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0""#;
        
        let result = parser.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "GET /index.html HTTP/1.1");
        assert_eq!(result.event.level, Some(LogLevel::Info));
        assert!(result.event.timestamp.is_some());
        assert_eq!(parser.get_format_type(), FormatType::Profile(ProfileType::Nginx));
    }
    
    #[test]
    fn test_profile_parser_syslog() {
        let parser = ProfileParser::new_syslog();
        let log_line = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";
        
        let result = parser.parse(log_line);
        
        assert!(result.success);
        assert_eq!(result.event.message, "'su root' failed for lonvick on /dev/pts/8");
        assert!(result.event.timestamp.is_some());
        assert_eq!(parser.get_format_type(), FormatType::Profile(ProfileType::Syslog));
    }
    
    #[test]
    fn test_profile_parser_validation() {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("invalid_field".to_string(), 99); // Invalid capture group
        
        let config = RegexProfileConfig {
            name: "invalid_profile".to_string(),
            pattern: r"^(\w+)$".to_string(), // Only has 1 capture group (index 1)
            field_mappings,
            timestamp_field: None,
            level_field: None,
            message_field: None,
            timestamp_format: None,
        };
        
        let parser_result = ProfileParser::new_regex(config);
        assert!(parser_result.is_err());
        
        if let Err(ParseError::ConfigurationError { parameter, error_message }) = parser_result {
            assert!(parameter.contains("invalid_field"));
            assert!(error_message.contains("Capture group 99 does not exist"));
        } else {
            panic!("Expected ConfigurationError");
        }
    }
}