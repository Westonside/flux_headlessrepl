use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;

use fluxcore::semantic::{self, Analyzer};

#[derive(Debug, StructOpt)]
#[structopt(about = "analyze a query log database")]
struct AnalyzeQueryLog {
    database: PathBuf,
}

fn main() -> Result<()> {
    let app = AnalyzeQueryLog::from_args();

    let connection = rusqlite::Connection::open(&app.database)?;

    let stdlib_path = PathBuf::from("../stdlib");
    let (prelude, imports, _sem_pkgs) = semantic::bootstrap::infer_stdlib_dir(&stdlib_path)?;

    let mut analyzer = Analyzer::new((&prelude).into(), &imports, Default::default());
    for source in connection
        .prepare("SELECT source FROM query limit 100")?
        .query_map([], |row| row.get(0))?
    {
        let source: String = source?;

        eprintln!("{}", source);
        if let Err(err) = analyzer.analyze_source("".into(), "".into(), &source) {
            eprintln!("{}", err.error.pretty(&source));
        }
        eprintln!("-------------------------------");
    }

    Ok(())
}
