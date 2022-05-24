use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;

use fluxcore::semantic::{self, Analyzer};

#[derive(Debug, StructOpt)]
#[structopt(about = "analyze a query log database")]
struct AnalyzeQueryLog {
    #[structopt(long, help = "How many sources to skip")]
    skip: Option<usize>,
    database: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();

    let app = AnalyzeQueryLog::from_args();

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

    if app.database.extension() == Some(std::ffi::OsStr::new("flux")) {
        let source = std::fs::read_to_string(&app.database)?;
        label_polymorphism_analyzer
            .analyze_source("".into(), "".into(), &source)
            .map_err(|err| err.error.pretty_error())?;
        return Ok(());
    }

    let connection = rusqlite::Connection::open(&app.database)?;

    let mut count = 0;
    for (i, source) in connection
        .prepare("SELECT source FROM query limit 10000")?
        .query_map([], |row| row.get(0))?
        .enumerate()
    {
        if let Some(skip) = app.skip {
            if i < skip {
                continue;
            }
        }

        let source: String = source?;

        // eprintln!("{}", source);

        let analyzer = std::panic::AssertUnwindSafe(&mut analyzer);
        let current_result = match std::panic::catch_unwind(|| {
            let analyzer = analyzer;
            analyzer.0.analyze_source("".into(), "".into(), &source)
        }) {
            Ok(x) => x,
            Err(_) => panic!("Panic at source {}: {}", i, source),
        };

        let label_polymorphism_analyzer =
            std::panic::AssertUnwindSafe(&mut label_polymorphism_analyzer);
        let label_polymorphism_result = match std::panic::catch_unwind(|| {
            let label_polymorphism_analyzer = label_polymorphism_analyzer;
            label_polymorphism_analyzer
                .0
                .analyze_source("".into(), "".into(), &source)
        }) {
            Ok(x) => x,
            Err(_) => panic!("Panic at source {}: {}", i, source),
        };

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
                if false {
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
        }
        count += 1;

        if count % 100 == 0 {
            eprintln!("Checked {} queries", count);
        }
    }

    eprintln!("Done! Checked {} queries", count);

    Ok(())
}
