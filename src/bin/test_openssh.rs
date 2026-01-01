use std::fs::File;
use std::io::{BufRead, BufReader};
use tango::TangoParser;

fn main() {
    let log_path = "test_data/OpenSSH/OpenSSH_2k.log";
    
    println!("Tango Log Parser - OpenSSH Log Test");
    println!("====================================");
    println!("Parsing: {}\n", log_path);
    
    let file = File::open(log_path).expect("Failed to open OpenSSH log file");
    let reader = BufReader::new(file);
    
    let mut parser = TangoParser::new();
    
    let mut total = 0;
    let mut parsed_ok = 0;
    let mut with_timestamp = 0;
    let mut with_level = 0;
    let mut format_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        
        if line.trim().is_empty() {
            continue;
        }
        
        total += 1;
        
        let result = parser.parse_line(&line);
        let event = &result.event;
        
        let format_name = format!("{:?}", event.format_type);
        *format_counts.entry(format_name).or_insert(0) += 1;
        
        if result.success {
            parsed_ok += 1;
        }
        
        if event.timestamp.is_some() {
            with_timestamp += 1;
        }
        
        if event.level.is_some() {
            with_level += 1;
        }
        
        // Show first 5 parsed results
        if total <= 5 {
            println!("--- Line {} ---", total);
            println!("  Success:   {}", result.success);
            println!("  Format:    {:?}", event.format_type);
            println!("  Timestamp: {:?}", event.timestamp);
            println!("  Level:     {:?}", event.level);
            println!("  Message:   {}", event.message.chars().take(70).collect::<String>());
            if !event.fields.is_empty() {
                println!("  Fields:    {:?}", event.fields);
            }
            println!();
        }
    }
    
    println!("====================================");
    println!("RESULTS SUMMARY");
    println!("====================================");
    println!("Total lines:      {}", total);
    println!("Parsed OK:        {} ({:.1}%)", parsed_ok, (parsed_ok as f64 / total as f64) * 100.0);
    println!("With timestamp:   {} ({:.1}%)", with_timestamp, (with_timestamp as f64 / total as f64) * 100.0);
    println!("With level:       {} ({:.1}%)", with_level, (with_level as f64 / total as f64) * 100.0);
    println!("\nFormat distribution:");
    for (format, count) in &format_counts {
        println!("  {}: {} ({:.1}%)", format, count, (*count as f64 / total as f64) * 100.0);
    }
}
