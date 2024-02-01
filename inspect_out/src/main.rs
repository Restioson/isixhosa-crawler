#![feature(array_windows)]

use anyhow::{Context, Result};
use clap::Parser;
use gethostname::gethostname;
use itertools::Itertools;
use kuchiki::iter::NodeIterator;
use kuchiki::traits::TendrilSink;
use pollster::block_on;
use punkt::params::Standard;
use punkt::{SentenceTokenizer, TrainingData};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use tldextract::{TldExtractor, TldOption};
use xtra::prelude::*;

#[derive(Parser)]
struct Options {
    file: String,

    #[arg(long)]
    since_last: bool,

    #[arg(long, short)]
    verbose: bool,
}

const LAST_INDEX_FILE: &str = "out/last_index.txt";

#[derive(Actor)]
struct LanguageIdActor {
    input: BufReader<TcpStream>,
    output: BufWriter<TcpStream>,
}

impl LanguageIdActor {
    fn identify(&mut self, text: String) -> IsIsiXhosa {
        #[derive(Serialize)]
        struct NchltLanguageIdRequest {
            text: String,
            benchmark: u8,
        }

        #[derive(Deserialize, Debug)]
        struct NchltLanguageResponse {
            language: String,
            confidence: f32,
        }

        let buf = serde_json::to_vec(&NchltLanguageIdRequest { text, benchmark: 0 }).unwrap();
        self.output.write_all(&buf).unwrap();
        self.output.flush().unwrap();
        let mut res = [0; 128];
        let n = self.input.read(&mut res).unwrap();
        let res = std::str::from_utf8(&res[..n])
            .unwrap()
            .replace("confidence:\"", "confidence\":");
        let res: NchltLanguageResponse = serde_json::from_str(&res).unwrap();

        if res.confidence < 0.5 || res.language != "isiXhosa" {
            IsIsiXhosa::No
        } else {
            IsIsiXhosa::Yes
        }
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
enum IsIsiXhosa {
    Yes,
    No,
}

#[async_trait]
impl Handler<String> for LanguageIdActor {
    type Return = IsIsiXhosa;

    async fn handle(&mut self, text: String, _ctx: &mut xtra::Context<Self>) -> IsIsiXhosa {
        self.identify(text)
    }
}

#[derive(Default)]
struct GloballyLockedData {
    total_words: usize,
    total_words_deduped_sentences: usize,
    sentences: HashMap<String, usize>,
    vocabulary_with_dupes: HashMap<String, usize>,
    vocabulary_without_dupes: HashMap<String, usize>,
    trigrams_with_dupes: HashMap<[u8; 3], usize>,
    trigrams_without_dupes: HashMap<[u8; 3], usize>,
    sites: HashMap<Domain, usize>,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
struct Domain(String);

impl Domain {
    fn new(url: &str) -> Result<Domain> {
        let res = TldExtractor::new(TldOption::default()).extract(url)?;
        Ok(Domain(
            [res.subdomain.filter(|s| s != "www"), res.domain, res.suffix]
                .iter()
                .flatten()
                .join("."),
        ))
    }
}

impl Display for Domain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
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

    // let lid = {
    //     let (lid, mailbox) = Mailbox::bounded(16);
    //
    //     for i in 0..12 {
    //         let stream = TcpStream::connect((gethostname().to_str().unwrap(), 7770 + i)).unwrap();
    //         let (input, output) = (
    //             BufReader::new(stream.try_clone().unwrap()),
    //             BufWriter::new(stream),
    //         );
    //         let act = LanguageIdActor { input, output };
    //         let mailbox = mailbox.clone();
    //         thread::spawn(move || block_on(xtra::run(mailbox, act)));
    //     }
    //
    //     lid
    // };

    let total_outlinks = AtomicUsize::new(0);
    let outlinks = dashmap::DashMap::new();
    let global_locked_data = Arc::new(Mutex::new(GloballyLockedData::default()));
    let punkt_data = TrainingData::english();

    println!("Total words;Total words (deduped sentences);Unique words;Unique trigrams");

    let spaces_to_one_re = Regex::new(r"\s+").unwrap();

    file.lines()
        .skip(last)
        .map(Result::unwrap)
        .par_bridge()
        .for_each(|line| {
            let entry: Entry = serde_json::from_str(&line).unwrap();
            match entry {
                Entry::Site { content, url } => {
                    {
                        let domain = Domain::new(&url).unwrap();
                        let mut data = global_locked_data.lock().unwrap();
                        *data.sites.entry(domain).or_insert(0) += 1;
                    }

                    return;
                    //
                    // let parser = kuchiki::parse_html().one(content);
                    //
                    // parser
                    //     .inclusive_descendants()
                    //     .filter(|node| {
                    //         node.as_element().map_or(false, |e| {
                    //             matches!(e.name.local.as_ref(), "script" | "style" | "noscript")
                    //         })
                    //     })
                    //     .collect::<Vec<_>>()
                    //     .iter()
                    //     .for_each(|node| node.detach());
                    //
                    // let mut content = String::new();
                    // for node in parser.inclusive_descendants().text_nodes() {
                    //     let node: &RefCell<String> = &*node;
                    //     let node = &node.borrow();
                    //     let trimmed = node.trim();
                    //     if !trimmed.is_empty() {
                    //         content.push_str(trimmed);
                    //         content.push(' ');
                    //     }
                    // }
                    //
                    // let xhosa_sentences: Vec<Cow<'_, str>> =
                    //     SentenceTokenizer::<Standard>::new(&content, &punkt_data)
                    //         .into_iter()
                    //         .flat_map(|sentence| textwrap::wrap(sentence, 300))
                    //         .filter(|sentence| !sentence.is_empty())
                    //         .filter(|sentence| {
                    //             block_on(lid.send(sentence.to_string())).unwrap() == IsIsiXhosa::Yes
                    //         })
                    //         .collect();
                    //
                    // for sentence in xhosa_sentences {
                    //     let mut data = global_locked_data.lock().unwrap();
                    //
                    //     let sentence = sentence
                    //         .replace(|c: char| !c.is_ascii_alphabetic(), " ")
                    //         .to_lowercase();
                    //     let sentence = spaces_to_one_re.replace_all(&sentence, " ");
                    //     let sentence_is_dupe = data.sentences.contains_key(sentence.as_ref());
                    //     *data.sentences.entry(sentence.to_string()).or_insert(0) += 1;
                    //
                    //     let words: Vec<&str> = sentence
                    //         .split_whitespace()
                    //         .filter(|word| !word.is_empty())
                    //         .collect();
                    //     let start_words = data.total_words;
                    //
                    //     for word in words {
                    //         *data
                    //             .vocabulary_with_dupes
                    //             .entry(word.to_owned())
                    //             .or_insert(0) += 1;
                    //
                    //         if !sentence_is_dupe {
                    //             *data
                    //                 .vocabulary_without_dupes
                    //                 .entry(word.to_owned())
                    //                 .or_insert(0) += 1;
                    //             data.total_words_deduped_sentences += 1;
                    //         }
                    //
                    //         data.total_words += 1;
                    //     }
                    //
                    //     for (a, b, c) in Some(&b'$')
                    //         .into_iter()
                    //         .chain(sentence.as_bytes())
                    //         .chain(Some(&b'.'))
                    //         .tuple_windows()
                    //     {
                    //         let trigram = [*a, *b, *c];
                    //         *data.trigrams_with_dupes.entry(trigram).or_insert(0) += 1;
                    //
                    //         if !sentence_is_dupe {
                    //             *data.trigrams_without_dupes.entry(trigram).or_insert(0) += 1;
                    //         }
                    //     }
                    //
                    //     let start_lower_100k = (start_words / 100_000) * 100_000;
                    //     let end_lower_100k = (data.total_words / 100_000) * 100_000;
                    //
                    //     // A 100_000 boundary was crossed, so print stats
                    //     if end_lower_100k > start_lower_100k {
                    //         println!(
                    //             "{};{};{};{}",
                    //             data.total_words,
                    //             data.total_words_deduped_sentences,
                    //             data.vocabulary_with_dupes.len(),
                    //             data.trigrams_with_dupes.len(),
                    //         );
                    //     }
                    // }
                }
                Entry::Link { from, to }  => {
                    let from = match Domain::new(&from) {
                        Ok(d) => d,
                        Err(_) => return,
                    };
                    let to = match Domain::new(&to) {
                        Ok(d) => d,
                        Err(_) => return,
                    };

                    total_outlinks.fetch_add(1, Ordering::Relaxed);

                    if from != to {
                        *outlinks.entry(to).or_insert(0) += 1;
                    }
                },
                Entry::SkippedSite { .. } => (),
            }
        });
    //
    let data = global_locked_data.lock().unwrap();
    // println!("{};{}", data.total_words, data.vocabulary_with_dupes.len());
    //
    // println!("Zipf's law (words) with duplicates");
    // println!("\nRank;Word;Frequency");
    //
    // let rankings = data
    //     .vocabulary_with_dupes
    //     .iter()
    //     .sorted_by_key(|(_word, frequency)| Reverse(*frequency))
    //     .take(2000)
    //     .enumerate();
    //
    // for (rank, (word, frequency)) in rankings {
    //     println!("{rank};{word};{frequency}");
    // }
    //
    // println!("Zipf's law (words) without duplicates");
    // println!("\nRank;Word;Frequency");
    //
    // let rankings = data
    //     .vocabulary_without_dupes
    //     .iter()
    //     .sorted_by_key(|(_word, frequency)| Reverse(*frequency))
    //     .take(2000)
    //     .enumerate();
    //
    // for (rank, (word, frequency)) in rankings {
    //     println!("{rank};{word};{frequency}");
    // }
    //
    // println!("Zipf's law (trigrams) with duplicates");
    // println!("\nRank;Word;Frequency");
    //
    // let rankings = data
    //     .trigrams_with_dupes
    //     .iter()
    //     .sorted_by_key(|(_trigram, frequency)| Reverse(*frequency))
    //     .take(2000)
    //     .enumerate();
    //
    // for (rank, (trigram, frequency)) in rankings {
    //     println!(
    //         "{};{};{}",
    //         rank,
    //         std::str::from_utf8(trigram).unwrap(),
    //         frequency
    //     );
    // }
    //
    // println!("Zipf's law (trigrams) without duplicates");
    // println!("\nRank;Word;Frequency");
    //
    // let rankings = data
    //     .trigrams_without_dupes
    //     .iter()
    //     .sorted_by_key(|(_trigram, frequency)| Reverse(*frequency))
    //     .take(2000)
    //     .enumerate();
    //
    // for (rank, (trigram, frequency)) in rankings {
    //     println!(
    //         "{};{};{}",
    //         rank,
    //         std::str::from_utf8(trigram).unwrap(),
    //         frequency
    //     );
    // }
    //
    // println!("\nTotal sentences;Unique sentences");
    // println!(
    //     "{};{}",
    //     data.sentences.values().sum::<usize>(),
    //     data.sentences.len()
    // );

    println!("\nSites");
    println!("Domain;Pages crawled");
    for (domain, pages) in data
        .sites
        .iter()
        .sorted_by_key(|(_site, pages)| Reverse(*pages))
    {
        println!("{domain};{pages}")
    }

    println!("\nOutlinks");
    println!("Domain;Links from other domains");
    for entry in outlinks
        .iter()
        .sorted_by_key(|entry| Reverse(*entry.value()))
    {
        println!("{};{}", entry.key(), entry.value())
    }

    println!("\nTotal links; Total outlinks");
    println!("{};{}", total_outlinks.load(Ordering::SeqCst), outlinks.iter().map(|e| *e.value()).sum::<usize>());

    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum Entry {
    SkippedSite { url: String, g_translate: bool },
    Site { url: String, content: String },
    Link { from: String, to: String },
}
