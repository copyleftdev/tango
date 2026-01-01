use crate::cli::ConvertArgs;
use crate::commands::output::OutputFormatter;
use crate::commands::parse::expand_globs;
use crate::{TangoParser, CanonicalEvent};
use std::fs::File;
use std::io::{BufRead, BufReader, Write, stdout};

pub fn run_convert(args: ConvertArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = TangoParser::new();
    let formatter = OutputFormatter::new(args.format)
        .with_fields(args.fields.clone())
        .with_raw(!args.no_raw);
    
    let files = expand_globs(&args.files)?;
    
    if files.is_empty() {
        eprintln!("No files matched the given patterns");
        return Ok(());
    }
    
    let mut output: Box<dyn Write> = if let Some(ref path) = args.output_file {
        Box::new(File::create(path)?)
    } else {
        Box::new(stdout())
    };
    
    if args.merge {
        // Collect all events and sort by timestamp
        let mut all_events: Vec<CanonicalEvent> = Vec::new();
        
        for file_path in &files {
            let file = File::open(file_path)?;
            let reader = BufReader::new(file);
            let source = file_path.to_string_lossy().to_string();
            
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                
                let result = parser.parse_line_with_source(&line, &source);
                all_events.push(result.event);
            }
        }
        
        // Sort by timestamp
        all_events.sort_by(|a, b| {
            match (a.timestamp, b.timestamp) {
                (Some(ta), Some(tb)) => ta.cmp(&tb),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        
        // Output merged events
        formatter.print_header(&mut output)?;
        for event in &all_events {
            writeln!(output, "{}", formatter.format_event(event))?;
        }
        
        eprintln!("Converted and merged {} events from {} files", all_events.len(), files.len());
    } else {
        // Process files sequentially
        formatter.print_header(&mut output)?;
        let mut total = 0;
        
        for file_path in &files {
            let file = File::open(file_path)?;
            let reader = BufReader::new(file);
            let source = file_path.to_string_lossy().to_string();
            
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                
                let result = parser.parse_line_with_source(&line, &source);
                writeln!(output, "{}", formatter.format_event(&result.event))?;
                total += 1;
            }
        }
        
        eprintln!("Converted {} events from {} files", total, files.len());
    }
    
    Ok(())
}
