use std::path::PathBuf;

use anyhow::{Error, Result};
use rayon::prelude::*;
use structopt::StructOpt;

use fluxcore::semantic::{self, Analyzer};

#[derive(Debug, StructOpt)]
#[structopt(about = "analyze a query log database")]
struct AnalyzeQueryLog {
    #[structopt(long, help = "How many sources to skip")]
    skip: Option<usize>,
    #[structopt(
        long,
        required = true,
        min_values = 1,
        help = "Which new features to compare against"
    )]
    new_features: Vec<semantic::Feature>,
    database: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();

    let app = AnalyzeQueryLog::from_args();

    let new_config = semantic::AnalyzerConfig {
        features: app.new_features,
    };

    let stdlib_path = PathBuf::from("../stdlib");

    let (prelude, imports, _sem_pkgs) =
        semantic::bootstrap::infer_stdlib_dir(&stdlib_path, semantic::AnalyzerConfig::default())?;

    let analyzer = || {
        Analyzer::new(
            (&prelude).into(),
            &imports,
            semantic::AnalyzerConfig::default(),
        )
    };

    let (prelude, imports, _sem_pkgs) =
        semantic::bootstrap::infer_stdlib_dir(&stdlib_path, new_config.clone())?;

    let new_analyzer = || Analyzer::new((&prelude).into(), &imports, new_config.clone());

    if app.database.extension() == Some(std::ffi::OsStr::new("flux")) {
        let source = std::fs::read_to_string(&app.database)?;
        new_analyzer()
            .analyze_source("".into(), "".into(), &source)
            .map_err(|err| err.error.pretty_error())?;
        return Ok(());
    }

    let connection = rusqlite::Connection::open(&app.database)?;

    let (tx, rx) = crossbeam_channel::bounded(128);

    let (final_tx, final_rx) = crossbeam_channel::bounded(128);

    let mut count = 0;

    let (r, r2, _) = join3(
        move || {
            for (i, result) in connection
                .prepare("SELECT source FROM query limit 100000")?
                .query_map([], |row| row.get(0))?
                .enumerate()
            {
                if let Some(skip) = app.skip {
                    if i < skip {
                        continue;
                    }
                }

                let source: String = result?;
                tx.send((i, source))?;
            }

            Ok::<_, Error>(())
        },
        move || {
            rx.into_iter()
                .par_bridge()
                .try_for_each(|(i, source): (usize, String)| {
                    // eprintln!("{}", source);

                    let current_result = match std::panic::catch_unwind(|| {
                        analyzer().analyze_source("".into(), "".into(), &source)
                    }) {
                        Ok(x) => x,
                        Err(_) => panic!("Panic at source {}: {}", i, source),
                    };

                    let new_result = match std::panic::catch_unwind(|| {
                        new_analyzer().analyze_source("".into(), "".into(), &source)
                    }) {
                        Ok(x) => x,
                        Err(_) => panic!("Panic at source {}: {}", i, source),
                    };

                    match (current_result, new_result) {
                        (Ok(_), Ok(_)) => (),
                        (Err(err), Ok(_)) => {
                            eprintln!("### {}", i);
                            eprintln!("{}", source);

                            eprintln!(
                                "Missing errors when the features are enabled: {}",
                                err.error.pretty(&source)
                            );
                            eprintln!("-------------------------------");
                        }
                        (Ok(_), Err(err)) => {
                            eprintln!("### {}", i);
                            eprintln!("{}", source);

                            eprintln!(
                                "New errors when the features are enabled: {}",
                                err.error.pretty(&source)
                            );
                            eprintln!("-------------------------------");
                        }
                        (Err(current_err), Err(new_err)) => {
                            if false {
                                let current_err = current_err.error.pretty(&source);
                                let new_err = new_err.error.pretty(&source);
                                if current_err != new_err {
                                    eprintln!("{}", source);

                                    eprintln!(
                                        "Different when the new features are enabled:\n{}",
                                        pretty_assertions::StrComparison::new(
                                            &current_err,
                                            &new_err,
                                        )
                                    );
                                    eprintln!("-------------------------------");
                                }
                            }
                        }
                    }

                    final_tx.send(())?;

                    Ok::<_, Error>(())
                })
        },
        || {
            for _ in final_rx {
                count += 1;

                if count % 100 == 0 {
                    eprintln!("Checked {} queries", count);
                }
            }
        },
    );

    r?;
    r2?;

    eprintln!("Done! Checked {} queries", count);

    Ok(())
}

fn join3<A, B, C>(
    a: impl FnOnce() -> A + Send,
    b: impl FnOnce() -> B + Send,
    c: impl FnOnce() -> C + Send,
) -> (A, B, C)
where
    A: Send,
    B: Send,
    C: Send,
{
    let (a, (b, c)) = rayon::join(a, || rayon::join(b, c));
    (a, b, c)
}
