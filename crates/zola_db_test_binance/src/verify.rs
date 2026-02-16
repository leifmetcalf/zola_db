use std::collections::BTreeMap;

use jiff::Timestamp;
use zola_db::{ColumnVec, Db, Direction, Probes, NULL_I64};

const TABLE: &str = "binance_agg_trades";

fn ts_from_ymdhms(y: i16, mo: i8, d: i8, h: i8, min: i8, s: i8) -> i64 {
    Timestamp::from_second(
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

/// Run all asof join verification checks.
pub fn run_all(db: &Db, symbol_ids: &BTreeMap<String, i64>) {
    test_backward_known_symbols(db, symbol_ids);
    test_forward_start_of_day(db, symbol_ids);
    test_cross_partition_sidecar(db, symbol_ids);
    test_multi_symbol_batch(db, symbol_ids);
    println!("All verification checks passed!");
}

/// 1. Backward asof for known symbols at mid-day 2026-02-11.
fn test_backward_known_symbols(db: &Db, symbol_ids: &BTreeMap<String, i64>) {
    println!("  check: backward asof for BTCUSDT, ETHUSDT, BNBUSDT at mid-day 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 12, 0, 0);
    let syms: Vec<(&str, i64)> = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        .iter()
        .map(|&name| {
            let id = *symbol_ids
                .get(name)
                .unwrap_or_else(|| panic!("{name} not in symbol map"));
            (name, id)
        })
        .collect();

    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &syms.iter().map(|s| s.1).collect::<Vec<_>>(),
                timestamps: &vec![probe_ts; syms.len()],
            },
            Direction::Backward,
        )
        .expect("backward asof failed");

    for (i, (name, _)) in syms.iter().enumerate() {
        assert_ne!(
            result.timestamps[i], NULL_I64,
            "{name}: expected non-null timestamp"
        );
        assert!(
            result.timestamps[i] <= probe_ts,
            "{name}: timestamp should be <= probe"
        );

        let ColumnVec::F64(ref prices) = result.columns[0] else {
            panic!("expected F64 price column");
        };
        assert!(
            prices[i] > 0.0 && !prices[i].is_nan(),
            "{name}: price should be positive, got {}",
            prices[i]
        );

        let ColumnVec::F64(ref qtys) = result.columns[1] else {
            panic!("expected F64 quantity column");
        };
        assert!(
            qtys[i] > 0.0 && !qtys[i].is_nan(),
            "{name}: quantity should be positive, got {}",
            qtys[i]
        );
    }

    println!("    PASS");
}

/// 2. Forward asof for BTCUSDT at start of 2026-02-10.
fn test_forward_start_of_day(db: &Db, symbol_ids: &BTreeMap<String, i64>) {
    println!("  check: forward asof for BTCUSDT at start of 2026-02-10");

    let probe_ts = ts_from_ymdhms(2026, 2, 10, 0, 0, 0);
    let btc_id = *symbol_ids.get("BTCUSDT").expect("BTCUSDT not found");

    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &[btc_id],
                timestamps: &[probe_ts],
            },
            Direction::Forward,
        )
        .expect("forward asof failed");

    assert_ne!(
        result.timestamps[0], NULL_I64,
        "BTCUSDT forward: expected non-null timestamp"
    );
    assert!(
        result.timestamps[0] >= probe_ts,
        "BTCUSDT forward: timestamp should be >= probe"
    );

    println!("    PASS");
}

/// 3. Cross-partition sidecar: probe ETHUSDT backward at midnight 2026-02-11.
///    Should find the last trade from 2026-02-10 via .last_values sidecar.
fn test_cross_partition_sidecar(db: &Db, symbol_ids: &BTreeMap<String, i64>) {
    println!("  check: cross-partition sidecar for ETHUSDT at midnight 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 0, 0, 0);
    let eth_id = *symbol_ids.get("ETHUSDT").expect("ETHUSDT not found");

    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &[eth_id],
                timestamps: &[probe_ts],
            },
            Direction::Backward,
        )
        .expect("cross-partition asof failed");

    assert_ne!(
        result.timestamps[0], NULL_I64,
        "ETHUSDT sidecar: expected non-null timestamp"
    );
    assert!(
        result.timestamps[0] <= probe_ts,
        "ETHUSDT sidecar: timestamp should be <= probe"
    );

    // The result should come from 2026-02-10 (previous partition)
    let ColumnVec::F64(ref prices) = result.columns[0] else {
        panic!("expected F64 price column");
    };
    assert!(
        prices[0] > 0.0 && !prices[0].is_nan(),
        "ETHUSDT sidecar: price should be positive"
    );

    println!("    PASS");
}

/// 4. Multi-symbol batch: probe 10 liquid symbols at mid-day 2026-02-12.
fn test_multi_symbol_batch(db: &Db, symbol_ids: &BTreeMap<String, i64>) {
    println!("  check: multi-symbol batch asof at mid-day 2026-02-12");

    let probe_ts = ts_from_ymdhms(2026, 2, 12, 12, 0, 0);
    let liquid = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
    ];

    let sym_ids: Vec<i64> = liquid
        .iter()
        .map(|&name| {
            *symbol_ids
                .get(name)
                .unwrap_or_else(|| panic!("{name} not in symbol map"))
        })
        .collect();

    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &sym_ids,
                timestamps: &vec![probe_ts; sym_ids.len()],
            },
            Direction::Backward,
        )
        .expect("multi-symbol asof failed");

    let ColumnVec::F64(ref prices) = result.columns[0] else {
        panic!("expected F64 price column");
    };

    for (i, &name) in liquid.iter().enumerate() {
        assert_ne!(
            result.timestamps[i], NULL_I64,
            "{name}: expected non-null timestamp"
        );
        assert!(
            prices[i] > 0.0 && !prices[i].is_nan(),
            "{name}: price should be positive, got {}",
            prices[i]
        );
    }

    println!("    PASS");
}
