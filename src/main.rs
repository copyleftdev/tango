use clap::Parser;
use tango::cli::{Cli, Commands};
use tango::commands::{run_parse, run_search, run_stats, run_tail, run_convert};

fn main() {
    let cli = Cli::parse();
    
    let result = match cli.command {
        Commands::Parse(args) => run_parse(args),
        Commands::Search(args) => run_search(args),
        Commands::Stats(args) => run_stats(args),
        Commands::Tail(args) => run_tail(args),
        Commands::Convert(args) => run_convert(args),
    };
    
    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
