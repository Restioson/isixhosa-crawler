use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use clap::Parser;
use serde::Deserialize;
use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use tldextract::{TldExtractor, TldOption};
use dashmap::DashMap;
use rayon::prelude::*;

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
    let file = BufReader::new(File::open(opts.file)?);

    let last: usize = if opts.since_last {
        let last = std::fs::read_to_string(LAST_INDEX_FILE)
            .context("Error loading last_index.txt")
            .and_then(|s| Ok(s.trim().parse()?))
            .unwrap_or(0);
        last
    } else {
        0
    };

    let sites = DashMap::new();

    let extractor = TldExtractor::new(TldOption::default());
    let new_last = AtomicUsize::new(last);

    file
        .lines()
        .skip(last)
        .map(Result::unwrap)
        .par_bridge()
        .for_each(|line| {
            let entry: Entry = serde_json::from_str(&line).unwrap();
            match entry {
                Entry::Site { url } => {
                    let extracted = extractor.extract(&url).unwrap();
                    let domain = extracted.domain.ok_or(anyhow!("No domain")).unwrap();
                    let suffix = extracted.suffix.ok_or(anyhow!("No suffix")).unwrap();
                    (*sites.entry(format!("{domain}.{suffix}")).or_insert(0)) += 1;
                    if opts.verbose {
                        println!("{}", url)
                    }
                },
                Entry::Link { .. } => (),
            }
            new_last.fetch_add(1, Ordering::SeqCst);
        });

    std::fs::write(LAST_INDEX_FILE, new_last.load(Ordering::SeqCst).to_string())?;

    let mut sum = 0;
    for entry in sites.iter().sorted_by_key(|entry| *entry.value()) {
        println!("{}: {}", entry.key(), entry.value());
        sum += entry.value();
    }

    println!("{}", sum);

    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Entry {
    Site { url: String },
    #[allow(dead_code)]
    Link { from: String, to: String },
}
