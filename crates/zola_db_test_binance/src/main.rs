mod download;
mod ingest;
mod parse;
mod symbols;
mod verify;

use std::path::PathBuf;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let total = Instant::now();

    // --- Phase 1: Discover symbols ---
    println!("Phase 1: Discovering USDT perpetual symbols...");
    let t = Instant::now();
    let client = reqwest::Client::new();
    let symbol_ids = symbols::fetch_symbols(&client).await;
    println!(
        "  found {} symbols ({:.1}s)",
        symbol_ids.len(),
        t.elapsed().as_secs_f64()
    );

    let symbol_names: Vec<String> = symbol_ids.keys().cloned().collect();
    let dates = ["2026-02-10", "2026-02-11", "2026-02-12"];

    // --- Phase 2: Download ---
    println!(
        "Phase 2: Downloading aggTrades ({} symbols × {} dates)...",
        symbol_names.len(),
        dates.len()
    );
    let t = Instant::now();

    let cache_dir: PathBuf = std::env::var("ZOLA_TEST_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/test-data/binance"));
    std::fs::create_dir_all(&cache_dir).expect("failed to create cache dir");

    let files = download::download_all(&client, &symbol_names, &dates, &cache_dir).await;
    println!(
        "  downloaded {} files ({:.1}s)",
        files.len(),
        t.elapsed().as_secs_f64()
    );

    // --- Phase 3: Ingest ---
    println!("Phase 3: Ingesting into zola_db...");
    let t = Instant::now();

    let tmp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
    let mut db = zola_db::Db::open(tmp_dir.path()).expect("failed to open db");

    ingest::ingest(&mut db, &files, &symbol_ids);
    println!("  ingestion complete ({:.1}s)", t.elapsed().as_secs_f64());

    // --- Phase 4: Verify ---
    println!("Phase 4: Running asof join verification...");
    let t = Instant::now();
    verify::run_all(&db, &symbol_ids);
    println!("  verification complete ({:.1}s)", t.elapsed().as_secs_f64());

    println!(
        "\nAll done! Total time: {:.1}s",
        total.elapsed().as_secs_f64()
    );
}
