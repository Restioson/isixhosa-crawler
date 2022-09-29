use std::collections::HashSet;
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

fn extract_domain(extractor: &TldExtractor, url: &str) -> String {
    let extracted = extractor.extract(&url).unwrap();
    let domain = extracted.domain.ok_or(anyhow!("No domain")).unwrap();
    let suffix = extracted.suffix.ok_or(anyhow!("No suffix")).unwrap();
    format!("{domain}.{suffix}")
}

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
                    (*sites.entry(extract_domain(&extractor, &url)).or_insert(0)) += 1;
                    if opts.verbose {
                        println!("{}", url)
                    }
                },
                Entry::Link { .. } => (),
            }
            new_last.fetch_add(1, Ordering::SeqCst);
        });

    std::fs::write(LAST_INDEX_FILE, new_last.load(Ordering::SeqCst).to_string())?;

    let sum: usize = sites
        .iter()
        .sorted_by_key(|entry| *entry.value())
        .map(|entry| {
            println!("{}: {}", entry.key(), entry.value());
            *entry.value()
        })
        .sum();

    println!("{}", sum);

    let crawled_sites: HashSet<String> = sites
        .iter()
        .map(|entry| entry.key().clone())
        .collect();

    let seed_sites = std::fs::read_to_string("seeds.txt")?
        .lines()
        .map(|url| extract_domain(&extractor, url))
        .collect();

    println!("\nList of sites crawled not in seeds:");
    for diff in crawled_sites.difference(&seed_sites) {
        println!("{diff}: {}", *sites.get(diff).unwrap());
    }

    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Entry {
    Site { url: String },
    #[allow(dead_code)]
    Link { from: String, to: String },
}
