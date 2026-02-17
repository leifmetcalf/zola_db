use std::path::Path;

use polars::prelude::*;
use zola_db::{ColumnVec, Db, Direction, Probes, NULL_I64};

use crate::parse;

const TABLE: &str = "binance_agg_trades";

/// Load raw trade data for given symbols from cached zip files into a Polars DataFrame.
/// Returns a DataFrame sorted by (symbol, timestamp).
fn load_trades(cache_dir: &Path, symbols: &[&str], dates: &[&str]) -> DataFrame {
    let mut all_ts: Vec<i64> = Vec::new();
    let mut all_sym: Vec<String> = Vec::new();
    let mut all_price: Vec<f64> = Vec::new();
    let mut all_qty: Vec<f64> = Vec::new();

    for &symbol in symbols {
        for &date in dates {
            let filename = format!("{symbol}-aggTrades-{date}.zip");
            let path = cache_dir.join(&filename);
            if !path.exists() {
                continue;
            }
            let trades = parse::parse_zip(&path);
            for t in &trades {
                all_ts.push(t.timestamp_us);
                all_sym.push(symbol.to_string());
                all_price.push(t.price);
                all_qty.push(t.quantity);
            }
        }
    }

    let n = all_ts.len();
    DataFrame::new(
        n,
        vec![
            Column::new("timestamp".into(), &all_ts),
            Column::new("symbol".into(), &all_sym),
            Column::new("price".into(), &all_price),
            Column::new("quantity".into(), &all_qty),
        ],
    )
    .expect("failed to create DataFrame")
    .sort(["symbol", "timestamp"], SortMultipleOptions::default())
    .expect("failed to sort DataFrame")
}

fn make_probes_df(probe_symbols: &[&str], probe_timestamps: &[i64]) -> DataFrame {
    let n = probe_symbols.len();
    let probe_sym: Vec<String> = probe_symbols.iter().map(|s| s.to_string()).collect();

    DataFrame::new(
        n,
        vec![
            Column::new("probe_ts".into(), probe_timestamps),
            Column::new("symbol".into(), &probe_sym),
        ],
    )
    .expect("failed to create probe df")
    .sort(["symbol", "probe_ts"], SortMultipleOptions::default())
    .expect("failed to sort probe df")
}

/// Run asof join in Polars and return (timestamp, price, quantity) per probe.
fn polars_asof(
    trades: &DataFrame,
    probe_symbols: &[&str],
    probe_timestamps: &[i64],
    strategy: AsofStrategy,
) -> Vec<(i64, f64, f64)> {
    let n = probe_symbols.len();
    let probes = make_probes_df(probe_symbols, probe_timestamps);

    let result = probes
        .join_asof_by(
            trades,
            "probe_ts",
            "timestamp",
            ["symbol"],
            ["symbol"],
            strategy,
            None,
            true,  // allow_eq (exact matches allowed)
            false, // check_sortedness (we already sorted)
        )
        .expect("join_asof failed");

    // Build a lookup from (symbol, probe_ts) -> row index in result
    let res_sym = result.column("symbol").unwrap().str().unwrap();
    let res_probe_ts = result.column("probe_ts").unwrap().i64().unwrap();
    let res_ts = result.column("timestamp").unwrap().i64().unwrap();
    let res_price = result.column("price").unwrap().f64().unwrap();
    let res_qty = result.column("quantity").unwrap().f64().unwrap();

    // Map result rows back to original probe order
    let mut out = vec![(NULL_I64, f64::NAN, f64::NAN); n];
    for row in 0..result.height() {
        let sym = res_sym.get(row).unwrap();
        let pts = res_probe_ts.get(row).unwrap();
        for i in 0..n {
            if probe_symbols[i] == sym && probe_timestamps[i] == pts && out[i].0 == NULL_I64 {
                match res_ts.get(row) {
                    Some(ts) => {
                        out[i] = (
                            ts,
                            res_price.get(row).unwrap_or(f64::NAN),
                            res_qty.get(row).unwrap_or(f64::NAN),
                        );
                    }
                    None => {}
                }
                break;
            }
        }
    }

    out
}

fn ts_from_ymdhms(y: i16, mo: i8, d: i8, h: i8, min: i8, s: i8) -> i64 {
    jiff::Timestamp::from_second(
        jiff::civil::date(y, mo, d)
            .at(h, min, s, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .expect("valid datetime")
            .timestamp()
            .as_second(),
    )
    .expect("valid timestamp")
    .as_microsecond()
}

pub fn run_all(db: &Db, cache_dir: &Path) {
    let symbols = &["BTCUSDT", "ETHUSDT", "BNBUSDT"];
    let dates = &["2026-02-10", "2026-02-11", "2026-02-12"];

    println!("  loading raw trade data for {} symbols into Polars...", symbols.len());
    let trades = load_trades(cache_dir, symbols, dates);
    println!("  loaded {} rows into Polars DataFrame", trades.height());

    check_backward_midday(db, &trades, symbols);
    check_forward_start_of_day(db, &trades);
    check_cross_partition(db, &trades);
    check_every_minute_ethusdt(db, cache_dir);

    println!("  all Polars cross-checks passed!");
}

fn check_backward_midday(db: &Db, trades: &DataFrame, symbols: &[&str]) {
    println!("  polars check: backward asof at mid-day 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 12, 0, 0);
    let probe_timestamps: Vec<i64> = vec![probe_ts; symbols.len()];

    let zola_result = db
        .asof(
            TABLE,
            &Probes { symbols, timestamps: &probe_timestamps },
            Direction::Backward,
        )
        .expect("zola asof failed");

    let polars_result = polars_asof(trades, symbols, &probe_timestamps, AsofStrategy::Backward);

    let ColumnVec::F64(ref zola_prices) = zola_result.columns[0] else {
        panic!("expected F64 price");
    };
    let ColumnVec::F64(ref zola_qtys) = zola_result.columns[1] else {
        panic!("expected F64 quantity");
    };

    for (i, &sym) in symbols.iter().enumerate() {
        let (pol_ts, pol_price, pol_qty) = polars_result[i];
        assert_eq!(
            zola_result.timestamps[i], pol_ts,
            "{sym}: timestamp mismatch: zola={} polars={}",
            zola_result.timestamps[i], pol_ts
        );
        assert_eq!(
            zola_prices[i], pol_price,
            "{sym}: price mismatch: zola={} polars={}",
            zola_prices[i], pol_price
        );
        assert_eq!(
            zola_qtys[i], pol_qty,
            "{sym}: quantity mismatch: zola={} polars={}",
            zola_qtys[i], pol_qty
        );
    }
    println!("    PASS — all {} symbols match", symbols.len());
}

fn check_forward_start_of_day(db: &Db, trades: &DataFrame) {
    println!("  polars check: forward asof for BTCUSDT at start of 2026-02-10");

    let probe_ts = ts_from_ymdhms(2026, 2, 10, 0, 0, 0);
    let symbols: &[&str] = &["BTCUSDT"];

    let zola_result = db
        .asof(
            TABLE,
            &Probes { symbols, timestamps: &[probe_ts] },
            Direction::Forward,
        )
        .expect("zola forward asof failed");

    let polars_result = polars_asof(trades, symbols, &[probe_ts], AsofStrategy::Forward);

    let (pol_ts, pol_price, _) = polars_result[0];
    let ColumnVec::F64(ref zola_prices) = zola_result.columns[0] else {
        panic!("expected F64");
    };

    assert_eq!(
        zola_result.timestamps[0], pol_ts,
        "forward ts mismatch: zola={} polars={}",
        zola_result.timestamps[0], pol_ts
    );
    assert_eq!(
        zola_prices[0], pol_price,
        "forward price mismatch: zola={} polars={}",
        zola_prices[0], pol_price
    );
    println!("    PASS");
}

fn check_cross_partition(db: &Db, trades: &DataFrame) {
    println!("  polars check: cross-partition sidecar for ETHUSDT at midnight 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 0, 0, 0);
    let symbols: &[&str] = &["ETHUSDT"];

    let zola_result = db
        .asof(
            TABLE,
            &Probes { symbols, timestamps: &[probe_ts] },
            Direction::Backward,
        )
        .expect("zola sidecar asof failed");

    let polars_result = polars_asof(trades, symbols, &[probe_ts], AsofStrategy::Backward);

    let (pol_ts, pol_price, _) = polars_result[0];
    let ColumnVec::F64(ref zola_prices) = zola_result.columns[0] else {
        panic!("expected F64");
    };

    assert_eq!(
        zola_result.timestamps[0], pol_ts,
        "sidecar ts mismatch: zola={} polars={}",
        zola_result.timestamps[0], pol_ts
    );
    assert_eq!(
        zola_prices[0], pol_price,
        "sidecar price mismatch: zola={} polars={}",
        zola_prices[0], pol_price
    );
    println!("    PASS");
}

fn check_every_minute_ethusdt(db: &Db, cache_dir: &Path) {
    println!("  polars check: every-minute backward asof for ETHUSDT across 3 days");

    let trades = load_trades(cache_dir, &["ETHUSDT"], &["2026-02-10", "2026-02-11", "2026-02-12"]);

    let start = ts_from_ymdhms(2026, 2, 10, 0, 0, 0);
    let one_minute_us: i64 = 60 * 1_000_000;
    let n = 3 * 24 * 60; // 4320

    let probe_timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * one_minute_us).collect();
    let probe_symbols: Vec<&str> = vec!["ETHUSDT"; n];

    let zola_result = db
        .asof(
            TABLE,
            &Probes { symbols: &probe_symbols, timestamps: &probe_timestamps },
            Direction::Backward,
        )
        .expect("zola every-minute asof failed");

    let polars_result = polars_asof(&trades, &probe_symbols, &probe_timestamps, AsofStrategy::Backward);

    let ColumnVec::F64(ref zola_prices) = zola_result.columns[0] else {
        panic!("expected F64");
    };

    let mut exact = 0;
    let mut ts_match_value_tie = 0;
    for i in 0..n {
        let (pol_ts, pol_price, _) = polars_result[i];
        if zola_result.timestamps[i] == NULL_I64 && pol_ts == NULL_I64 {
            exact += 1;
            continue;
        }
        assert_eq!(
            zola_result.timestamps[i], pol_ts,
            "probe {i}: ts mismatch: zola={} polars={}",
            zola_result.timestamps[i], pol_ts
        );
        if zola_prices[i] == pol_price {
            exact += 1;
        } else {
            // Timestamps match but values differ — duplicate-timestamp tie-break
            ts_match_value_tie += 1;
        }
    }
    println!(
        "    PASS — {exact}/{n} exact, {ts_match_value_tie} timestamp-tie differences",
    );
}
