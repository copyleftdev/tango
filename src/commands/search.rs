use crate::cli::SearchArgs;
use crate::commands::output::OutputFormatter;
use crate::commands::parse::{expand_globs, parse_time, parse_field_filters, matches_filters};
use crate::{TangoParser, LogLevel};
use std::fs::File;
use std::io::{BufRead, BufReader, Write, stdout};

pub fn run_search(args: SearchArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = TangoParser::new();
    let formatter = OutputFormatter::new(args.output)
        .with_highlight(args.grep.as_deref());
    
    let files = expand_globs(&args.files)?;
    
    if files.is_empty() {
        eprintln!("No files matched the given patterns");
        return Ok(());
    }
    
    let since = args.since.as_ref().and_then(|s| parse_time(s));
    let until = args.until.as_ref().and_then(|s| parse_time(s));
    
    let levels: Option<Vec<LogLevel>> = args.level.as_ref().map(|lvls| {
        lvls.iter()
            .filter_map(|l| LogLevel::from_str(l))
            .collect()
    });
    
    let field_filters = parse_field_filters(&args.field);
    
    let grep_pattern = args.grep.as_ref().and_then(|p| {
        let pattern = if args.ignore_case {
            format!("(?i){}", p)
        } else {
            p.clone()
        };
        regex::Regex::new(&pattern).ok()
    });
    
    let mut output: Box<dyn Write> = Box::new(stdout());
    formatter.print_header(&mut output)?;
    
    let mut match_count = 0;
    let mut context_buffer: Vec<String> = Vec::new();
    let mut pending_after = 0;
    
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
            let event = &result.event;
            
            let matches = matches_filters(event, &levels, &since, &until, &grep_pattern, &field_filters);
            let should_show = if args.invert { !matches } else { matches };
            
            if should_show {
                // Print before context
                if args.before > 0 {
                    for ctx_line in context_buffer.iter().rev().take(args.before).rev() {
                        writeln!(output, "{}", ctx_line)?;
                    }
                }
                
                writeln!(output, "{}", formatter.format_event(event))?;
                match_count += 1;
                pending_after = args.after;
                
                if let Some(limit) = args.limit {
                    if match_count >= limit {
                        break;
                    }
                }
            } else if pending_after > 0 {
                writeln!(output, "{}", formatter.format_event(event))?;
                pending_after -= 1;
            }
            
            // Update context buffer
            if args.before > 0 {
                context_buffer.push(formatter.format_event(event));
                if context_buffer.len() > args.before {
                    context_buffer.remove(0);
                }
            }
        }
    }
    
    eprintln!("\n{} matches found", match_count);
    Ok(())
}
