use std::collections::HashMap;
use anyhow::__private::kind::TraitKind;
use clap::Parser;
use serde::Deserialize;
use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use tldextract::{TldExtractor, TldOption};

#[derive(Parser)]
struct Options {
    file: String,

    #[arg(long)]
    since_last: bool,

    #[arg(long, short)]
    verbose: bool,
}

const LAST_INDEX_FILE: &str = "out/last_index.txt";

fn main() -> Result<()> {
    let opts = Options::parse();
    let file = std::fs::read_to_string(opts.file)?;
    let lines = file.lines();

    let last: usize = if opts.since_last {
        let last = std::fs::read_to_string(LAST_INDEX_FILE)
            .context("Error loading last_index.txt")
            .and_then(|s| Ok(s.trim().parse()?))
            .unwrap_or(0);
        last
    } else {
        0
    };

    let mut sites = HashMap::new();

    let extractor = TldExtractor::new(TldOption::default());
    let mut new_last = last;
    for line in lines.skip(last) {
        let entry: Entry = serde_json::from_str(line)?;
        match entry {
            Entry::Site { url } => {
                let extracted = extractor.extract(&url)?;
                let domain = extracted.domain.ok_or(anyhow!("No domain"))?;
                let suffix = extracted.suffix.ok_or(anyhow!("No suffix"))?;
                (*sites.entry(format!("{domain}.{suffix}")).or_insert(0)) += 1;
                if opts.verbose {
                    println!("{}", url)
                }
            },
            Entry::Link { .. } => (),
        }
        new_last += 1;
    }

    std::fs::write(LAST_INDEX_FILE, new_last.to_string())?;

    for (site, count) in sites.iter().sorted_by_key(|(_, count)| **count) {
        println!("{}: {}", site, count);
    }

    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Entry {
    Site { url: String },
    Link { from: String, to: String },
}
