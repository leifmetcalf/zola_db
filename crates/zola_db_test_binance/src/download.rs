use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Semaphore;

/// Download all aggTrade zips for the given symbols and dates, with file caching.
/// Returns (symbol, date_str, path) for each successful download.
pub async fn download_all(
    client: &reqwest::Client,
    symbols: &[String],
    dates: &[&str],
    cache_dir: &Path,
) -> Vec<(String, String, PathBuf)> {
    let sem = Arc::new(Semaphore::new(15));
    let mut handles = Vec::new();

    for symbol in symbols {
        for &date in dates {
            let client = client.clone();
            let sem = sem.clone();
            let symbol = symbol.clone();
            let date = date.to_string();
            let cache_dir = cache_dir.to_path_buf();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                match download_one(&client, &symbol, &date, &cache_dir).await {
                    Some(path) => Some((symbol, date, path)),
                    None => None,
                }
            }));
        }
    }

    let mut results = Vec::new();
    for handle in handles {
        if let Ok(Some(item)) = handle.await {
            results.push(item);
        }
    }
    results
}

async fn download_one(
    client: &reqwest::Client,
    symbol: &str,
    date: &str,
    cache_dir: &Path,
) -> Option<PathBuf> {
    let filename = format!("{symbol}-aggTrades-{date}.zip");
    let final_path = cache_dir.join(&filename);

    // Cache hit
    if final_path.exists() && std::fs::metadata(&final_path).map(|m| m.len() > 0).unwrap_or(false)
    {
        return Some(final_path);
    }

    let url = format!(
        "https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{filename}"
    );

    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("  download error {symbol} {date}: {e}");
            return None;
        }
    };

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }

    if !resp.status().is_success() {
        eprintln!("  HTTP {} for {symbol} {date}", resp.status());
        return None;
    }

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("  body error {symbol} {date}: {e}");
            return None;
        }
    };

    // Atomic write via .tmp rename
    let tmp_path = cache_dir.join(format!("{filename}.tmp"));
    if let Err(e) = tokio::fs::write(&tmp_path, &bytes).await {
        eprintln!("  write error {symbol} {date}: {e}");
        return None;
    }
    if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
        eprintln!("  rename error {symbol} {date}: {e}");
        return None;
    }

    Some(final_path)
}
