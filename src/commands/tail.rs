use crate::cli::TailArgs;
use crate::commands::output::OutputFormatter;
use crate::{TangoParser, LogLevel};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::thread;
use std::time::Duration;

pub fn run_tail(args: TailArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = TangoParser::new();
    let formatter = OutputFormatter::new(args.output)
        .with_highlight(args.grep.as_deref());
    
    let levels: Option<Vec<LogLevel>> = args.level.as_ref().map(|lvls| {
        lvls.iter()
            .filter_map(|l| LogLevel::from_str(l))
            .collect()
    });
    
    let grep_pattern = args.grep.as_ref()
        .and_then(|p| regex::Regex::new(&format!("(?i){}", p)).ok());
    
    let source = args.file.to_string_lossy().to_string();
    
    // Open file and seek to end minus N lines
    let mut file = File::open(&args.file)?;
    let initial_lines = read_last_n_lines(&mut file, args.lines)?;
    
    // Print initial lines
    for line in initial_lines {
        let result = parser.parse_line_with_source(&line, &source);
        let event = &result.event;
        
        // Apply filters
        if let Some(ref allowed_levels) = levels {
            if let Some(level) = event.level {
                if !allowed_levels.contains(&level) {
                    continue;
                }
            } else {
                continue;
            }
        }
        
        if let Some(ref pattern) = grep_pattern {
            if !pattern.is_match(&event.message) && !pattern.is_match(&event.raw) {
                continue;
            }
        }
        
        println!("{}", formatter.format_event(event));
    }
    
    // Follow mode
    if args.follow {
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::End(0))?;
        
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    // No new data, wait a bit
                    thread::sleep(Duration::from_millis(100));
                }
                Ok(_) => {
                    let line = line.trim_end();
                    if line.is_empty() {
                        continue;
                    }
                    
                    let result = parser.parse_line_with_source(line, &source);
                    let event = &result.event;
                    
                    // Apply filters
                    if let Some(ref allowed_levels) = levels {
                        if let Some(level) = event.level {
                            if !allowed_levels.contains(&level) {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                    
                    if let Some(ref pattern) = grep_pattern {
                        if !pattern.is_match(&event.message) && !pattern.is_match(&event.raw) {
                            continue;
                        }
                    }
                    
                    println!("{}", formatter.format_event(event));
                }
                Err(e) => {
                    eprintln!("Error reading file: {}", e);
                    break;
                }
            }
        }
    }
    
    Ok(())
}

fn read_last_n_lines(file: &mut File, n: usize) -> Result<Vec<String>, std::io::Error> {
    use std::io::Read;
    
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        return Ok(Vec::new());
    }
    
    // Read from end, chunk by chunk
    let chunk_size = 8192u64;
    let mut lines = Vec::new();
    let mut buffer = Vec::new();
    let mut pos = file_size;
    
    while lines.len() < n && pos > 0 {
        let read_size = std::cmp::min(chunk_size, pos);
        pos -= read_size;
        file.seek(SeekFrom::Start(pos))?;
        
        let mut chunk = vec![0u8; read_size as usize];
        file.read_exact(&mut chunk)?;
        
        // Prepend to buffer
        chunk.append(&mut buffer);
        buffer = chunk;
        
        // Count newlines
        lines = String::from_utf8_lossy(&buffer)
            .lines()
            .map(|s| s.to_string())
            .collect();
    }
    
    // Return last N lines
    let start = if lines.len() > n { lines.len() - n } else { 0 };
    Ok(lines[start..].to_vec())
}
