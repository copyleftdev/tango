use clap::{Parser, Subcommand, Args, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "tango")]
#[command(author, version, about = "High-performance multi-format log parser for security, sysops, and devops")]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    
    /// Number of parallel threads (0 = auto-detect)
    #[arg(long, short = 'j', global = true, default_value = "0")]
    pub parallel: usize,
    
    /// Memory limit in MB
    #[arg(long, global = true, default_value = "100")]
    pub memory_limit: usize,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Parse log files and output structured data
    Parse(ParseArgs),
    
    /// Live tail log files with parsing
    Tail(TailArgs),
    
    /// Show statistics and summaries
    Stats(StatsArgs),
    
    /// Search logs with filters
    Search(SearchArgs),
    
    /// Convert logs between formats
    Convert(ConvertArgs),
}

#[derive(Args)]
pub struct ParseArgs {
    /// Log files to parse (supports glob patterns)
    #[arg(required = true)]
    pub files: Vec<PathBuf>,
    
    /// Output format
    #[arg(long, short, value_enum, default_value = "table")]
    pub output: OutputFormat,
    
    /// Filter by log level(s)
    #[arg(long, short)]
    pub level: Option<Vec<String>>,
    
    /// Filter by time - start (e.g., "1 hour ago", "2025-01-01")
    #[arg(long)]
    pub since: Option<String>,
    
    /// Filter by time - end
    #[arg(long)]
    pub until: Option<String>,
    
    /// Pattern to search in message
    #[arg(long, short)]
    pub grep: Option<String>,
    
    /// Filter by field value (format: field=value)
    #[arg(long, short = 'F')]
    pub field: Option<Vec<String>>,
    
    /// Fields to include in output (comma-separated)
    #[arg(long)]
    pub fields: Option<String>,
    
    /// Exclude raw log line from output
    #[arg(long)]
    pub no_raw: bool,
    
    /// Highlight matches in output
    #[arg(long, short = 'H')]
    pub highlight: bool,
    
    /// Maximum number of results
    #[arg(long, short = 'n')]
    pub limit: Option<usize>,
    
    /// Show detected format for each file
    #[arg(long)]
    pub format_detect: bool,
    
    /// Output file (default: stdout)
    #[arg(long, short = 'o')]
    pub output_file: Option<PathBuf>,
}

#[derive(Args)]
pub struct TailArgs {
    /// Log file to tail
    #[arg(required = true)]
    pub file: PathBuf,
    
    /// Follow file changes (like tail -f)
    #[arg(long, short)]
    pub follow: bool,
    
    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub output: OutputFormat,
    
    /// Filter by log level(s)
    #[arg(long, short)]
    pub level: Option<Vec<String>>,
    
    /// Pattern to search in message
    #[arg(long, short)]
    pub grep: Option<String>,
    
    /// Highlight matches
    #[arg(long, short = 'H')]
    pub highlight: bool,
    
    /// Number of lines to show initially
    #[arg(long, short = 'n', default_value = "10")]
    pub lines: usize,
}

#[derive(Args)]
pub struct StatsArgs {
    /// Log files to analyze
    #[arg(required = true)]
    pub files: Vec<PathBuf>,
    
    /// Count entries by field
    #[arg(long)]
    pub count_by: Option<String>,
    
    /// Show top N entries
    #[arg(long, default_value = "10")]
    pub top: usize,
    
    /// Field to rank by (for --top)
    #[arg(long)]
    pub by: Option<String>,
    
    /// Show unique values for a field
    #[arg(long, short)]
    pub unique: Option<String>,
    
    /// Show time histogram
    #[arg(long)]
    pub histogram: bool,
    
    /// Time bucket for histogram (hour, day, minute)
    #[arg(long, default_value = "hour")]
    pub bucket: String,
    
    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub output: OutputFormat,
}

#[derive(Args)]
pub struct SearchArgs {
    /// Log files to search (supports glob patterns)
    #[arg(required = true)]
    pub files: Vec<PathBuf>,
    
    /// Pattern to search in message (required)
    #[arg(long, short)]
    pub grep: Option<String>,
    
    /// Filter by log level(s)
    #[arg(long, short)]
    pub level: Option<Vec<String>>,
    
    /// Filter by time - start
    #[arg(long)]
    pub since: Option<String>,
    
    /// Filter by time - end
    #[arg(long)]
    pub until: Option<String>,
    
    /// Filter by field value (format: field=value)
    #[arg(long, short = 'F')]
    pub field: Option<Vec<String>>,
    
    /// Case-insensitive search
    #[arg(long, short)]
    pub ignore_case: bool,
    
    /// Invert match (show non-matching)
    #[arg(long, short = 'v')]
    pub invert: bool,
    
    /// Show context lines before match
    #[arg(long, short = 'B', default_value = "0")]
    pub before: usize,
    
    /// Show context lines after match
    #[arg(long, short = 'A', default_value = "0")]
    pub after: usize,
    
    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub output: OutputFormat,
    
    /// Highlight matches
    #[arg(long, short = 'H')]
    pub highlight: bool,
    
    /// Maximum number of results
    #[arg(long, short = 'n')]
    pub limit: Option<usize>,
}

#[derive(Args)]
pub struct ConvertArgs {
    /// Log files to convert
    #[arg(required = true)]
    pub files: Vec<PathBuf>,
    
    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "json")]
    pub format: OutputFormat,
    
    /// Output file
    #[arg(long, short = 'o')]
    pub output_file: Option<PathBuf>,
    
    /// Merge files by timestamp
    #[arg(long)]
    pub merge: bool,
    
    /// Fields to include (comma-separated)
    #[arg(long)]
    pub fields: Option<String>,
    
    /// Exclude raw log line
    #[arg(long)]
    pub no_raw: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table
    Table,
    /// JSON (one object per line)
    Json,
    /// Newline-delimited JSON
    Ndjson,
    /// CSV format
    Csv,
    /// Raw parsed output
    Raw,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Ndjson => write!(f, "ndjson"),
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Raw => write!(f, "raw"),
        }
    }
}
