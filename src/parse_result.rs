use crate::models::CanonicalEvent;
use crate::error::ParseError;

/// Result of a parsing operation with enhanced error reporting
#[derive(Debug, Clone)]
pub struct ParseResult {
    pub success: bool,
    pub event: CanonicalEvent,
    pub error: Option<ParseError>,
    pub confidence: f64,
    pub line_number: Option<usize>,
    pub processing_time_micros: Option<u64>,
}

impl ParseResult {
    /// Create a successful parse result
    pub fn success(event: CanonicalEvent, confidence: f64) -> Self {
        Self {
            success: true,
            event,
            error: None,
            confidence,
            line_number: None,
            processing_time_micros: None,
        }
    }
    
    /// Create a successful parse result with timing information
    pub fn success_with_timing(event: CanonicalEvent, confidence: f64, processing_time_micros: u64) -> Self {
        Self {
            success: true,
            event,
            error: None,
            confidence,
            line_number: None,
            processing_time_micros: Some(processing_time_micros),
        }
    }
    
    /// Create a failed parse result with detailed error
    pub fn failure(raw: String, error: ParseError) -> Self {
        let error_message = error.to_string();
        Self {
            success: false,
            event: CanonicalEvent::with_error(raw, error_message),
            error: Some(error),
            confidence: 0.0,
            line_number: None,
            processing_time_micros: None,
        }
    }
    
    /// Create a failed parse result with line number and timing
    pub fn failure_with_context(
        raw: String, 
        error: ParseError, 
        line_number: Option<usize>,
        processing_time_micros: Option<u64>
    ) -> Self {
        let error_message = error.to_string();
        Self {
            success: false,
            event: CanonicalEvent::with_error(raw, error_message),
            error: Some(error),
            confidence: 0.0,
            line_number,
            processing_time_micros,
        }
    }
    
    /// Set line number for this parse result
    pub fn with_line_number(mut self, line_number: usize) -> Self {
        self.line_number = Some(line_number);
        self
    }
    
    /// Set processing time for this parse result
    pub fn with_processing_time(mut self, processing_time_micros: u64) -> Self {
        self.processing_time_micros = Some(processing_time_micros);
        self
    }
    
    /// Get a detailed error description including context
    pub fn detailed_error_description(&self) -> Option<String> {
        if let Some(error) = &self.error {
            let mut description = error.to_string();
            
            if let Some(line_num) = self.line_number {
                description = format!("Line {}: {}", line_num, description);
            }
            
            if let Some(time) = self.processing_time_micros {
                description = format!("{} (processed in {}μs)", description, time);
            }
            
            Some(description)
        } else {
            None
        }
    }
}