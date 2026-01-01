use crate::cli::StatsArgs;
use crate::commands::parse::expand_globs;
use crate::commands::output::print_stats_summary;
use crate::TangoParser;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;
use colored::*;

pub fn run_stats(args: StatsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = TangoParser::new();
    let files = expand_globs(&args.files)?;
    
    if files.is_empty() {
        eprintln!("No files matched the given patterns");
        return Ok(());
    }
    
    let mut total = 0;
    let mut parsed_ok = 0;
    let mut with_timestamp = 0;
    let mut with_level = 0;
    let mut format_counts: HashMap<String, usize> = HashMap::new();
    let mut level_counts: HashMap<String, usize> = HashMap::new();
    let mut field_counts: HashMap<String, HashMap<String, usize>> = HashMap::new();
    let mut time_buckets: HashMap<String, usize> = HashMap::new();
    
    for file_path in &files {
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
                
                // Time histogram
                if args.histogram {
                    if let Some(ts) = event.timestamp {
                        let bucket_key = match args.bucket.as_str() {
                            "minute" => ts.format("%Y-%m-%d %H:%M").to_string(),
                            "hour" => ts.format("%Y-%m-%d %H:00").to_string(),
                            "day" => ts.format("%Y-%m-%d").to_string(),
                            _ => ts.format("%Y-%m-%d %H:00").to_string(),
                        };
                        *time_buckets.entry(bucket_key).or_insert(0) += 1;
                    }
                }
            }
            if let Some(level) = event.level {
                with_level += 1;
                let level_name = format!("{:?}", level);
                *level_counts.entry(level_name).or_insert(0) += 1;
            }
            
            let format_name = format!("{:?}", event.format_type);
            *format_counts.entry(format_name).or_insert(0) += 1;
            
            // Count by field
            if let Some(ref count_field) = args.count_by {
                if let Some(value) = event.fields.get(count_field) {
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    let field_map = field_counts.entry(count_field.clone()).or_insert_with(HashMap::new);
                    *field_map.entry(value_str).or_insert(0) += 1;
                }
            }
            
            // Unique values
            if let Some(ref unique_field) = args.unique {
                if let Some(value) = event.fields.get(unique_field) {
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    let field_map = field_counts.entry(unique_field.clone()).or_insert_with(HashMap::new);
                    field_map.entry(value_str).or_insert(0);
                }
            }
            
            // Top by field
            if let Some(ref by_field) = args.by {
                if let Some(value) = event.fields.get(by_field) {
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    let field_map = field_counts.entry(by_field.clone()).or_insert_with(HashMap::new);
                    *field_map.entry(value_str).or_insert(0) += 1;
                }
            }
        }
    }
    
    // Print basic stats
    print_stats_summary(total, parsed_ok, with_timestamp, with_level, &format_counts);
    
    // Print level distribution
    if !level_counts.is_empty() {
        println!("\n{}:", "Level Distribution".cyan().bold());
        let mut sorted: Vec<_> = level_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (level, count) in sorted {
            let bar_len = (*count as f64 / total as f64 * 40.0) as usize;
            let bar = "█".repeat(bar_len);
            println!("  {:8} {:>6} ({:5.1}%) {}", 
                level, count, (*count as f64 / total as f64) * 100.0, bar.green());
        }
    }
    
    // Print time histogram
    if args.histogram && !time_buckets.is_empty() {
        println!("\n{}:", "Time Distribution".cyan().bold());
        let mut sorted: Vec<_> = time_buckets.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        let max_count = sorted.iter().map(|(_, c)| **c).max().unwrap_or(1);
        for (bucket, count) in sorted {
            let bar_len = (*count as f64 / max_count as f64 * 40.0) as usize;
            let bar = "█".repeat(bar_len);
            println!("  {} {:>6} {}", bucket, count, bar.blue());
        }
    }
    
    // Print count by / top by
    if let Some(ref field) = args.count_by.as_ref().or(args.by.as_ref()) {
        if let Some(counts) = field_counts.get(*field) {
            println!("\n{} by '{}':", "Count".cyan().bold(), field);
            let mut sorted: Vec<_> = counts.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));
            for (value, count) in sorted.iter().take(args.top) {
                println!("  {:40} {:>8}", value, count);
            }
        }
    }
    
    // Print unique values
    if let Some(ref field) = args.unique {
        if let Some(counts) = field_counts.get(field) {
            println!("\n{} values for '{}':", "Unique".cyan().bold(), field);
            let mut values: Vec<_> = counts.keys().collect();
            values.sort();
            for value in values.iter().take(args.top) {
                println!("  {}", value);
            }
            println!("  ... {} unique values total", counts.len());
        }
    }
    
    Ok(())
}
