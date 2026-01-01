use crate::models::*;
use crate::parse_result::ParseResult;

/// Common interface for all log parsers
pub trait LogParser {
    fn parse(&self, line: &str) -> ParseResult;
    fn can_parse(&self, line: &str) -> bool;
    fn get_format_type(&self) -> FormatType;
}

// Re-export individual parser modules
pub mod json_parser;
pub mod logfmt_parser;
pub mod pattern_parser;
pub mod plain_text_parser;
pub mod profile_parser;

pub use json_parser::JsonParser;
pub use logfmt_parser::LogfmtParser;
pub use pattern_parser::PatternParser;
pub use plain_text_parser::PlainTextParser;
pub use profile_parser::ProfileParser;