use std::collections::HashMap;
use std::fmt;
use serde::{Deserialize, Serialize};

/// Comprehensive error types for log parsing operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParseError {
    /// JSON parsing failed with syntax error
    JsonSyntaxError {
        message: String,
        line_number: Option<usize>,
        column: Option<usize>,
    },
    /// JSON structure is valid but not an object
    JsonNotObject {
        actual_type: String,
    },
    /// Logfmt parsing failed due to insufficient key=value pairs
    LogfmtInsufficientPairs {
        found_pairs: usize,
        required_pairs: usize,
    },
    /// Logfmt parsing failed due to malformed key=value syntax
    LogfmtMalformedSyntax {
        invalid_segment: String,
        position: usize,
    },
    /// Timestamp parsing failed
    TimestampParseError {
        input: String,
        attempted_formats: Vec<String>,
    },
    /// Level parsing failed - unrecognized level token
    LevelParseError {
        input: String,
        valid_levels: Vec<String>,
    },
    /// Pattern matching failed - no recognized patterns
    PatternMatchError {
        input: String,
        attempted_patterns: Vec<String>,
    },
    /// Field extraction failed
    FieldExtractionError {
        field_name: String,
        error_message: String,
    },
    /// Regex compilation or execution error
    RegexError {
        pattern: String,
        error_message: String,
    },
    /// I/O error during parsing
    IoError {
        operation: String,
        error_message: String,
    },
    /// Memory or resource exhaustion
    ResourceExhausted {
        resource_type: String,
        limit: String,
    },
    /// Configuration error
    ConfigurationError {
        parameter: String,
        error_message: String,
    },
    /// Generic parsing error with context
    GenericError {
        message: String,
        context: HashMap<String, String>,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::JsonSyntaxError { message, line_number, column } => {
                write!(f, "JSON syntax error: {}", message)?;
                if let Some(line) = line_number {
                    write!(f, " at line {}", line)?;
                }
                if let Some(col) = column {
                    write!(f, " column {}", col)?;
                }
                Ok(())
            }
            ParseError::JsonNotObject { actual_type } => {
                write!(f, "JSON is not an object, found: {}", actual_type)
            }
            ParseError::LogfmtInsufficientPairs { found_pairs, required_pairs } => {
                write!(f, "Insufficient logfmt pairs: found {}, required {}", found_pairs, required_pairs)
            }
            ParseError::LogfmtMalformedSyntax { invalid_segment, position } => {
                write!(f, "Malformed logfmt syntax at position {}: '{}'", position, invalid_segment)
            }
            ParseError::TimestampParseError { input, attempted_formats } => {
                write!(f, "Failed to parse timestamp '{}', tried formats: {:?}", input, attempted_formats)
            }
            ParseError::LevelParseError { input, valid_levels } => {
                write!(f, "Unrecognized level '{}', valid levels: {:?}", input, valid_levels)
            }
            ParseError::PatternMatchError { input, attempted_patterns } => {
                write!(f, "No pattern matched for '{}', tried: {:?}", input, attempted_patterns)
            }
            ParseError::FieldExtractionError { field_name, error_message } => {
                write!(f, "Failed to extract field '{}': {}", field_name, error_message)
            }
            ParseError::RegexError { pattern, error_message } => {
                write!(f, "Regex error for pattern '{}': {}", pattern, error_message)
            }
            ParseError::IoError { operation, error_message } => {
                write!(f, "I/O error during {}: {}", operation, error_message)
            }
            ParseError::ResourceExhausted { resource_type, limit } => {
                write!(f, "Resource exhausted: {} exceeded limit {}", resource_type, limit)
            }
            ParseError::ConfigurationError { parameter, error_message } => {
                write!(f, "Configuration error for '{}': {}", parameter, error_message)
            }
            ParseError::GenericError { message, context } => {
                write!(f, "Parse error: {}", message)?;
                if !context.is_empty() {
                    write!(f, " (context: {:?})", context)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ParseError {}