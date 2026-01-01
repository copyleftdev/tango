use crate::cli::OutputFormat;
use crate::CanonicalEvent;
use colored::*;
use std::io::{self, Write};

pub struct OutputFormatter {
    format: OutputFormat,
    highlight_pattern: Option<regex::Regex>,
    fields: Option<Vec<String>>,
    include_raw: bool,
}

impl OutputFormatter {
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            highlight_pattern: None,
            fields: None,
            include_raw: true,
        }
    }
    
    pub fn with_highlight(mut self, pattern: Option<&str>) -> Self {
        if let Some(p) = pattern {
            self.highlight_pattern = regex::Regex::new(&format!("(?i){}", p)).ok();
        }
        self
    }
    
    pub fn with_fields(mut self, fields: Option<String>) -> Self {
        self.fields = fields.map(|f| f.split(',').map(|s| s.trim().to_string()).collect());
        self
    }
    
    pub fn with_raw(mut self, include: bool) -> Self {
        self.include_raw = include;
        self
    }
    
    pub fn format_event(&self, event: &CanonicalEvent) -> String {
        match self.format {
            OutputFormat::Table => self.format_table(event),
            OutputFormat::Json => self.format_json(event),
            OutputFormat::Ndjson => self.format_json(event),
            OutputFormat::Csv => self.format_csv(event),
            OutputFormat::Raw => self.format_raw(event),
        }
    }
    
    pub fn format_events(&self, events: &[CanonicalEvent]) -> String {
        match self.format {
            OutputFormat::Json => {
                let json_events: Vec<serde_json::Value> = events.iter()
                    .map(|e| self.event_to_json(e))
                    .collect();
                serde_json::to_string_pretty(&json_events).unwrap_or_default()
            }
            _ => events.iter()
                .map(|e| self.format_event(e))
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
    
    pub fn print_header(&self, writer: &mut impl Write) -> io::Result<()> {
        match self.format {
            OutputFormat::Csv => {
                writeln!(writer, "timestamp,level,message,format,fields")?;
            }
            OutputFormat::Table => {
                writeln!(writer, "{}", "─".repeat(100).dimmed())?;
            }
            _ => {}
        }
        Ok(())
    }
    
    fn format_table(&self, event: &CanonicalEvent) -> String {
        let mut output = String::new();
        
        // Timestamp
        let ts = event.timestamp
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!("{} ", ts.cyan()));
        
        // Level with color
        let level = event.level
            .map(|l| format!("{:?}", l))
            .unwrap_or_else(|| "-".to_string());
        let colored_level = match event.level {
            Some(crate::LogLevel::Fatal) => level.red().bold(),
            Some(crate::LogLevel::Error) => level.red(),
            Some(crate::LogLevel::Warn) => level.yellow(),
            Some(crate::LogLevel::Info) => level.green(),
            Some(crate::LogLevel::Debug) => level.blue(),
            Some(crate::LogLevel::Trace) => level.dimmed(),
            None => level.dimmed(),
        };
        output.push_str(&format!("[{:^5}] ", colored_level));
        
        // Message with highlighting
        let message = if let Some(ref pattern) = self.highlight_pattern {
            pattern.replace_all(&event.message, |caps: &regex::Captures| {
                caps[0].to_string().on_yellow().black().to_string()
            }).to_string()
        } else {
            event.message.clone()
        };
        output.push_str(&message);
        
        // Fields if present
        if !event.fields.is_empty() {
            let fields_str: Vec<String> = event.fields.iter()
                .filter(|(k, _)| {
                    if let Some(ref allowed) = self.fields {
                        allowed.contains(k)
                    } else {
                        true
                    }
                })
                .map(|(k, v)| format!("{}={}", k.dimmed(), format_value(v)))
                .collect();
            if !fields_str.is_empty() {
                output.push_str(&format!(" {}", fields_str.join(" ").dimmed()));
            }
        }
        
        output
    }
    
    fn format_json(&self, event: &CanonicalEvent) -> String {
        serde_json::to_string(&self.event_to_json(event)).unwrap_or_default()
    }
    
    fn event_to_json(&self, event: &CanonicalEvent) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        
        if let Some(ts) = event.timestamp {
            obj.insert("timestamp".to_string(), serde_json::Value::String(ts.to_rfc3339()));
        }
        
        if let Some(level) = event.level {
            obj.insert("level".to_string(), serde_json::Value::String(format!("{:?}", level).to_lowercase()));
        }
        
        obj.insert("message".to_string(), serde_json::Value::String(event.message.clone()));
        obj.insert("format".to_string(), serde_json::Value::String(format!("{:?}", event.format_type)));
        
        if !event.fields.is_empty() {
            let fields: serde_json::Map<String, serde_json::Value> = event.fields.iter()
                .filter(|(k, _)| {
                    if let Some(ref allowed) = self.fields {
                        allowed.contains(k)
                    } else {
                        true
                    }
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if !fields.is_empty() {
                obj.insert("fields".to_string(), serde_json::Value::Object(fields));
            }
        }
        
        if self.include_raw {
            obj.insert("raw".to_string(), serde_json::Value::String(event.raw.clone()));
        }
        
        serde_json::Value::Object(obj)
    }
    
    fn format_csv(&self, event: &CanonicalEvent) -> String {
        let ts = event.timestamp
            .map(|t| t.to_rfc3339())
            .unwrap_or_default();
        let level = event.level
            .map(|l| format!("{:?}", l).to_lowercase())
            .unwrap_or_default();
        let message = event.message.replace('"', "\"\"");
        let format_type = format!("{:?}", event.format_type);
        let fields = serde_json::to_string(&event.fields).unwrap_or_default().replace('"', "\"\"");
        
        format!("{},\"{}\",\"{}\",{},\"{}\"", ts, level, message, format_type, fields)
    }
    
    fn format_raw(&self, event: &CanonicalEvent) -> String {
        event.raw.clone()
    }
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

pub fn print_stats_summary(
    total: usize,
    parsed: usize,
    with_ts: usize,
    with_level: usize,
    format_dist: &std::collections::HashMap<String, usize>,
) {
    println!("\n{}", "═".repeat(50).cyan());
    println!("{}", "SUMMARY".cyan().bold());
    println!("{}", "═".repeat(50).cyan());
    println!("Total lines:      {}", total.to_string().white().bold());
    println!("Parsed OK:        {} ({:.1}%)", 
        parsed.to_string().green(), 
        (parsed as f64 / total as f64) * 100.0);
    println!("With timestamp:   {} ({:.1}%)", 
        with_ts.to_string().cyan(), 
        (with_ts as f64 / total as f64) * 100.0);
    println!("With level:       {} ({:.1}%)", 
        with_level.to_string().yellow(), 
        (with_level as f64 / total as f64) * 100.0);
    
    if !format_dist.is_empty() {
        println!("\n{}:", "Format Distribution".dimmed());
        for (format, count) in format_dist {
            println!("  {}: {} ({:.1}%)", 
                format.white(), 
                count, 
                (*count as f64 / total as f64) * 100.0);
        }
    }
}
