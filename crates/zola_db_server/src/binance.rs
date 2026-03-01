use std::io::{BufRead, BufReader};
use std::sync::Arc;

use arrow::array::types::Int32Type;
use arrow::array::{Float64Array, Int32Array, Int64Array, RunArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use zola_db::{SYMBOL_COL, TIMESTAMP_COL};
use zola_db_proto::Market;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const S3_ENDPOINT: &str = "https://s3.ap-northeast-1.amazonaws.com/data.binance.vision";
const DOWNLOAD_HOST: &str = "https://data.binance.vision";
const MAX_CONCURRENT: usize = 64;

fn s3_prefix(market: Market) -> &'static str {
    match market {
        Market::Spot => "data/spot/daily/aggTrades/",
        Market::Perp => "data/futures/um/daily/aggTrades/",
    }
}

pub fn table_name(market: Market) -> &'static str {
    match market {
        Market::Spot => "spot_aggtrades",
        Market::Perp => "perp_aggtrades",
    }
}

pub async fn list_symbols(client: &Client, market: Market) -> Result<Vec<String>> {
    let prefix = s3_prefix(market);
    let mut symbols = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut req = client
            .get(S3_ENDPOINT)
            .query(&[("list-type", "2"), ("prefix", prefix), ("delimiter", "/")]);
        if let Some(token) = &continuation {
            req = req.query(&[("continuation-token", token.as_str())]);
        }

        let body = req.send().await?.error_for_status()?.text().await?;

        for part in body.split("<Prefix>").skip(1) {
            if let Some(end) = part.find("</Prefix>") {
                let p = &part[..end];
                if let Some(sym) = p.strip_prefix(prefix).and_then(|s| s.strip_suffix('/')) {
                    if !sym.is_empty() {
                        symbols.push(sym.to_string());
                    }
                }
            }
        }

        if body.contains("<IsTruncated>true</IsTruncated>") {
            let tag = "<NextContinuationToken>";
            if let Some(start) = body.find(tag) {
                let rest = &body[start + tag.len()..];
                if let Some(end) = rest.find("</NextContinuationToken>") {
                    continuation = Some(rest[..end].to_string());
                    continue;
                }
            }
        }
        break;
    }

    Ok(symbols)
}

struct SymbolData {
    timestamps: Vec<i64>,
    prices: Vec<f64>,
    quantities: Vec<f64>,
}

async fn download_and_parse(
    client: &Client,
    market: Market,
    symbol: &str,
    date: &str,
) -> Result<Option<SymbolData>> {
    let prefix = s3_prefix(market);
    let url = format!("{DOWNLOAD_HOST}/{prefix}{symbol}/{symbol}-aggTrades-{date}.zip");

    let resp = client.get(&url).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let zip_bytes = resp.error_for_status()?.bytes().await?;

    tokio::task::spawn_blocking(move || {
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes))?;
        let csv = archive.by_index(0)?;
        parse_csv(BufReader::new(csv))
    })
    .await?
    .map(Some)
}

fn parse_csv(reader: impl BufRead) -> Result<SymbolData> {
    let mut timestamps = Vec::new();
    let mut prices = Vec::new();
    let mut quantities = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("agg_trade_id") {
            continue;
        }

        let mut f = line.split(',');
        f.next(); // agg_trade_id
        let price: f64 = f.next().unwrap().parse()?;
        let qty: f64 = f.next().unwrap().parse()?;
        f.next(); // first_trade_id
        f.next(); // last_trade_id
        let ts_str = f.next().unwrap();
        let ts: i64 = ts_str.parse()?;
        // Binance timestamps are milliseconds (13 digits); normalize to microseconds.
        // Guard handles the hypothetical case of a format change to microseconds.
        timestamps.push(if ts_str.len() == 13 { ts * 1000 } else { ts });
        prices.push(price);
        quantities.push(qty);
    }

    Ok(SymbolData { timestamps, prices, quantities })
}

fn build_batch(mut data: Vec<(String, SymbolData)>) -> RecordBatch {
    data.sort_by(|a, b| a.0.cmp(&b.0));

    let total: usize = data.iter().map(|(_, d)| d.timestamps.len()).sum();
    let mut all_ts = Vec::with_capacity(total);
    let mut all_px = Vec::with_capacity(total);
    let mut all_qty = Vec::with_capacity(total);
    let mut run_ends = Vec::with_capacity(data.len());
    let mut sym_vals = Vec::with_capacity(data.len());

    let mut offset = 0i32;
    for (sym, d) in data {
        offset += d.timestamps.len() as i32;
        all_ts.extend(d.timestamps);
        all_px.extend(d.prices);
        all_qty.extend(d.quantities);
        run_ends.push(offset);
        sym_vals.push(sym);
    }

    let symbol_col = RunArray::<Int32Type>::try_new(
        &Int32Array::from(run_ends),
        &StringArray::from(sym_vals),
    )
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            SYMBOL_COL,
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ),
            false,
        ),
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(symbol_col),
            Arc::new(Int64Array::from(all_ts)),
            Arc::new(Float64Array::from(all_px)),
            Arc::new(Float64Array::from(all_qty)),
        ],
    )
    .unwrap()
}

pub async fn fetch(client: &Client, market: Market, symbols: &[String], day: jiff::civil::Date) -> Result<Option<RecordBatch>> {
    let date = day.to_string();
    eprintln!("downloading aggtrades for {date} across {} symbols...", symbols.len());

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut join_set = JoinSet::new();

    for symbol in symbols.iter().cloned() {
        let client = client.clone();
        let date = date.clone();
        let semaphore = semaphore.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let result = download_and_parse(&client, market, &symbol, &date).await;
            result.map(|opt| opt.map(|data| (symbol, data)))
        });
    }

    // Errors propagate intentionally — a partial day is not useful, we need
    // complete data across all symbols to form a valid partition.
    let mut symbol_data = Vec::new();
    while let Some(result) = join_set.join_next().await {
        if let Some((sym, data)) = result?? {
            if !data.timestamps.is_empty() {
                symbol_data.push((sym, data));
            }
        }
    }

    eprintln!("{} symbols with data", symbol_data.len());
    if symbol_data.is_empty() {
        return Ok(None);
    }
    Ok(Some(build_batch(symbol_data)))
}
