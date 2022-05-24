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

    let label_polymorphism_config = semantic::AnalyzerConfig {
        features: vec![semantic::Feature::LabelPolymorphism],
    };

    let stdlib_path = PathBuf::from("../stdlib");

    let (prelude, imports, _sem_pkgs) =
        semantic::bootstrap::infer_stdlib_dir(&stdlib_path, semantic::AnalyzerConfig::default())?;

    let mut analyzer = Analyzer::new(
        (&prelude).into(),
        imports,
        semantic::AnalyzerConfig::default(),
    );

    let (prelude, imports, _sem_pkgs) =
        semantic::bootstrap::infer_stdlib_dir(&stdlib_path, label_polymorphism_config.clone())?;

    let mut label_polymorphism_analyzer = Analyzer::new(
        (&prelude).into(),
        imports,
        label_polymorphism_config.clone(),
    );

    let mut count = 0;
    for source in connection
        .prepare("SELECT source FROM query limit 100")?
        .query_map([], |row| row.get(0))?
    {
        let source: String = source?;

        let current_result = analyzer.analyze_source("".into(), "".into(), &source);

        let label_polymorphism_result =
            label_polymorphism_analyzer.analyze_source("".into(), "".into(), &source);

        match (current_result, label_polymorphism_result) {
            (Ok(_), Ok(_)) => (),
            (Err(err), Ok(_)) => {
                eprintln!("{}", source);

                eprintln!(
                    "Missing errors when label polymorphism is enabled: {}",
                    err.error.pretty(&source)
                );
                eprintln!("-------------------------------");
            }
            (Ok(_), Err(err)) => {
                eprintln!("{}", source);

                eprintln!(
                    "New errors when label polymorphism is enabled: {}",
                    err.error.pretty(&source)
                );
                eprintln!("-------------------------------");
            }
            (Err(current_err), Err(label_polymorphism_err)) => {
                let current_err = current_err.error.pretty(&source);
                let label_polymorphism_err = label_polymorphism_err.error.pretty(&source);
                if current_err != label_polymorphism_err {
                    eprintln!("{}", source);

                    eprintln!(
                        "Different when label polymorphism is enabled:\n{}",
                        pretty_assertions::StrComparison::new(
                            &current_err,
                            &label_polymorphism_err,
                        )
                    );
                    eprintln!("-------------------------------");
                }
            }
        }
        count += 1;
    }

    eprintln!("Checked {} queries", count);

    Ok(())
}
