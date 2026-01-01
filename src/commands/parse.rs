use crate::cli::ParseArgs;
use crate::commands::output::{OutputFormatter, print_stats_summary};
use crate::{TangoParser, CanonicalEvent, LogLevel};
use std::fs::File;
use std::io::{BufRead, BufReader, Write, stdout};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use glob::glob;

pub fn run_parse(args: ParseArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = TangoParser::new();
    let formatter = OutputFormatter::new(args.output)
        .with_highlight(args.grep.as_deref())
        .with_fields(args.fields.clone())
        .with_raw(!args.no_raw);
    
    // Expand glob patterns
    let files = expand_globs(&args.files)?;
    
    if files.is_empty() {
        eprintln!("No files matched the given patterns");
        return Ok(());
    }
    
    // Parse time filters
    let since = args.since.as_ref().and_then(|s| parse_time(s));
    let until = args.until.as_ref().and_then(|s| parse_time(s));
    
    // Parse level filters
    let levels: Option<Vec<LogLevel>> = args.level.as_ref().map(|lvls| {
        lvls.iter()
            .filter_map(|l| LogLevel::from_str(l))
            .collect()
    });
    
    // Parse field filters
    let field_filters = parse_field_filters(&args.field);
    
    // Compile grep pattern
    let grep_pattern = args.grep.as_ref()
        .and_then(|p| regex::Regex::new(&format!("(?i){}", p)).ok());
    
    let mut output: Box<dyn Write> = if let Some(ref path) = args.output_file {
        Box::new(File::create(path)?)
    } else {
        Box::new(stdout())
    };
    
    formatter.print_header(&mut output)?;
    
    let mut total = 0;
    let mut parsed_ok = 0;
    let mut with_timestamp = 0;
    let mut with_level = 0;
    let mut format_counts: HashMap<String, usize> = HashMap::new();
    let mut output_count = 0;
    
    for file_path in &files {
        if args.format_detect {
            eprintln!("Processing: {}", file_path.display());
        }
        
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let source = file_path.to_string_lossy().to_string();
        
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            total += 1;
            let result = parser.parse_line_with_source(&line, &source);
            let event = &result.event;
            
            if result.success {
                parsed_ok += 1;
            }
            if event.timestamp.is_some() {
                with_timestamp += 1;
            }
            if event.level.is_some() {
                with_level += 1;
            }
            
            let format_name = format!("{:?}", event.format_type);
            *format_counts.entry(format_name).or_insert(0) += 1;
            
            // Apply filters
            if !matches_filters(event, &levels, &since, &until, &grep_pattern, &field_filters) {
                continue;
            }
            
            // Check limit
            if let Some(limit) = args.limit {
                if output_count >= limit {
                    break;
                }
            }
            
            writeln!(output, "{}", formatter.format_event(event))?;
            output_count += 1;
        }
        
        if args.format_detect {
            eprintln!("  Format: {:?}", format_counts);
        }
    }
    
    // Print summary to stderr if outputting to file
    if args.output_file.is_some() {
        print_stats_summary(total, parsed_ok, with_timestamp, with_level, &format_counts);
    }
    
    Ok(())
}

pub fn expand_globs(patterns: &[std::path::PathBuf]) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for pattern in patterns {
        let pattern_str = pattern.to_string_lossy();
        if pattern_str.contains('*') || pattern_str.contains('?') {
            for entry in glob(&pattern_str)? {
                files.push(entry?);
            }
        } else {
            files.push(pattern.clone());
        }
    }
    Ok(files)
}

pub fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    
    // Try common date formats
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
    }
    if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(DateTime::from_naive_utc_and_offset(dt.and_hms_opt(0, 0, 0)?, Utc));
    }
    
    // Try humantime relative (e.g., "1 hour ago")
    if let Ok(duration) = humantime::parse_duration(s.trim_end_matches(" ago")) {
        let now = Utc::now();
        return Some(now - chrono::Duration::from_std(duration).ok()?);
    }
    
    None
}

pub fn parse_field_filters(filters: &Option<Vec<String>>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(filters) = filters {
        for filter in filters {
            if let Some((key, value)) = filter.split_once('=') {
                map.insert(key.to_string(), value.to_string());
            }
        }
    }
    map
}

pub fn matches_filters(
    event: &CanonicalEvent,
    levels: &Option<Vec<LogLevel>>,
    since: &Option<DateTime<Utc>>,
    until: &Option<DateTime<Utc>>,
    grep: &Option<regex::Regex>,
    field_filters: &HashMap<String, String>,
) -> bool {
    // Level filter
    if let Some(ref allowed_levels) = levels {
        if let Some(event_level) = event.level {
            if !allowed_levels.contains(&event_level) {
                return false;
            }
        } else {
            return false; // No level, but level filter specified
        }
    }
    
    // Time filters
    if let Some(ref start) = since {
        if let Some(ts) = event.timestamp {
            if ts < *start {
                return false;
            }
        }
    }
    if let Some(ref end) = until {
        if let Some(ts) = event.timestamp {
            if ts > *end {
                return false;
            }
        }
    }
    
    // Grep filter
    if let Some(ref pattern) = grep {
        if !pattern.is_match(&event.message) && !pattern.is_match(&event.raw) {
            return false;
        }
    }
    
    // Field filters
    for (key, expected_value) in field_filters {
        if let Some(value) = event.fields.get(key) {
            let value_str = match value {
                serde_json::Value::String(s) => s.clone(),
                _ => value.to_string(),
            };
            if !value_str.contains(expected_value) {
                return false;
            }
        } else {
            return false;
        }
    }
    
    true
}
