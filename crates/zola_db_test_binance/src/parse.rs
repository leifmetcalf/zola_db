use std::io::Read;
use std::path::Path;

pub struct AggTrade {
    pub timestamp_us: i64,
    pub price: f64,
    pub quantity: f64,
}

/// Parse aggTrades from a Binance zip file.
/// CSV columns (no header): agg_trade_id, price, quantity, first_trade_id, last_trade_id, timestamp_ms, is_buyer_maker
/// Returns trades sorted by timestamp.
pub fn parse_zip(path: &Path) -> Vec<AggTrade> {
    let file = std::fs::File::open(path).expect("failed to open zip");
    let mut archive = zip::ZipArchive::new(file).expect("failed to read zip");
    let mut entry = archive.by_index(0).expect("zip has no entries");

    let mut contents = String::new();
    entry
        .read_to_string(&mut contents)
        .expect("failed to read zip entry");

    let mut trades: Vec<AggTrade> = contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let mut parts = line.splitn(7, ',');
            let _agg_id = parts.next()?;
            let price: f64 = parts.next()?.parse().ok()?;
            let quantity: f64 = parts.next()?.parse().ok()?;
            let _first_id = parts.next()?;
            let _last_id = parts.next()?;
            let ts_ms: i64 = parts.next()?.parse().ok()?;
            Some(AggTrade {
                timestamp_us: ts_ms * 1000,
                price,
                quantity,
            })
        })
        .collect();

    trades.sort_by_key(|t| t.timestamp_us);
    trades
}
