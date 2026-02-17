use std::time::Instant;

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
pub fn run_all(db: &Db) {
    test_backward_known_symbols(db);
    test_forward_start_of_day(db);
    test_cross_partition_sidecar(db);
    test_multi_symbol_batch(db);
    test_every_minute_ethusdt(db);
    println!("All verification checks passed!");
}

/// 1. Backward asof for known symbols at mid-day 2026-02-11.
fn test_backward_known_symbols(db: &Db) {
    println!("  check: backward asof for BTCUSDT, ETHUSDT, BNBUSDT at mid-day 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 12, 0, 0);
    let names: Vec<&str> = vec!["BTCUSDT", "ETHUSDT", "BNBUSDT"];

    let t = Instant::now();
    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &names,
                timestamps: &vec![probe_ts; names.len()],
            },
            Direction::Backward,
        )
        .expect("backward asof failed");
    let elapsed = t.elapsed();

    for (i, &name) in names.iter().enumerate() {
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

    println!("    PASS ({elapsed:.3?})");
}

/// 2. Forward asof for BTCUSDT at start of 2026-02-10.
fn test_forward_start_of_day(db: &Db) {
    println!("  check: forward asof for BTCUSDT at start of 2026-02-10");

    let probe_ts = ts_from_ymdhms(2026, 2, 10, 0, 0, 0);

    let t = Instant::now();
    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &["BTCUSDT"],
                timestamps: &[probe_ts],
            },
            Direction::Forward,
        )
        .expect("forward asof failed");
    let elapsed = t.elapsed();

    assert_ne!(
        result.timestamps[0], NULL_I64,
        "BTCUSDT forward: expected non-null timestamp"
    );
    assert!(
        result.timestamps[0] >= probe_ts,
        "BTCUSDT forward: timestamp should be >= probe"
    );

    println!("    PASS ({elapsed:.3?})");
}

/// 3. Cross-partition sidecar: probe ETHUSDT backward at midnight 2026-02-11.
///    Should find the last trade from 2026-02-10 via .last_values sidecar.
fn test_cross_partition_sidecar(db: &Db) {
    println!("  check: cross-partition sidecar for ETHUSDT at midnight 2026-02-11");

    let probe_ts = ts_from_ymdhms(2026, 2, 11, 0, 0, 0);

    let t = Instant::now();
    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &["ETHUSDT"],
                timestamps: &[probe_ts],
            },
            Direction::Backward,
        )
        .expect("cross-partition asof failed");
    let elapsed = t.elapsed();

    assert_ne!(
        result.timestamps[0], NULL_I64,
        "ETHUSDT sidecar: expected non-null timestamp"
    );
    assert!(
        result.timestamps[0] <= probe_ts,
        "ETHUSDT sidecar: timestamp should be <= probe"
    );

    let ColumnVec::F64(ref prices) = result.columns[0] else {
        panic!("expected F64 price column");
    };
    assert!(
        prices[0] > 0.0 && !prices[0].is_nan(),
        "ETHUSDT sidecar: price should be positive"
    );

    println!("    PASS ({elapsed:.3?})");
}

/// 4. Multi-symbol batch: probe 10 liquid symbols at mid-day 2026-02-12.
fn test_multi_symbol_batch(db: &Db) {
    println!("  check: multi-symbol batch asof at mid-day 2026-02-12");

    let probe_ts = ts_from_ymdhms(2026, 2, 12, 12, 0, 0);
    let liquid: Vec<&str> = vec![
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
    ];

    let t = Instant::now();
    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &liquid,
                timestamps: &vec![probe_ts; liquid.len()],
            },
            Direction::Backward,
        )
        .expect("multi-symbol asof failed");
    let elapsed = t.elapsed();

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

    println!("    PASS ({elapsed:.3?})");
}

/// 5. Backward asof for ETHUSDT at every minute across all 3 days.
///    4320 probes (3 days × 24 hours × 60 minutes), issued as a single batch.
fn test_every_minute_ethusdt(db: &Db) {
    println!("  check: every-minute backward asof for ETHUSDT across 2026-02-10..12");

    let start = ts_from_ymdhms(2026, 2, 10, 0, 0, 0);
    let one_minute_us: i64 = 60 * 1_000_000;
    let n = 3 * 24 * 60; // 4320 probes

    let probe_timestamps: Vec<i64> = (0..n).map(|i| start + i as i64 * one_minute_us).collect();
    let probe_symbols: Vec<&str> = vec!["ETHUSDT"; n];

    let t = Instant::now();
    let result = db
        .asof(
            TABLE,
            &Probes {
                symbols: &probe_symbols,
                timestamps: &probe_timestamps,
            },
            Direction::Backward,
        )
        .expect("every-minute asof failed");
    let elapsed = t.elapsed();

    let ColumnVec::F64(ref prices) = result.columns[0] else {
        panic!("expected F64 price column");
    };

    // First probe is at exactly midnight 2026-02-10 00:00:00 — may or may not have
    // a trade at or before that exact microsecond. All subsequent probes should hit.
    let mut non_null = 0;
    let mut prev_ts = i64::MIN;
    for i in 0..n {
        if result.timestamps[i] != NULL_I64 {
            non_null += 1;
            assert!(
                result.timestamps[i] <= probe_timestamps[i],
                "probe {i}: result timestamp {} > probe {}",
                result.timestamps[i],
                probe_timestamps[i]
            );
            assert!(
                prices[i] > 0.0 && !prices[i].is_nan(),
                "probe {i}: price should be positive, got {}",
                prices[i]
            );
            // Result timestamps should be monotonically non-decreasing
            assert!(
                result.timestamps[i] >= prev_ts,
                "probe {i}: result timestamp went backwards: {} < {}",
                result.timestamps[i],
                prev_ts
            );
            prev_ts = result.timestamps[i];
        }
    }

    // ETHUSDT trades continuously; expect virtually all probes to hit
    let hit_pct = non_null as f64 / n as f64 * 100.0;
    assert!(
        hit_pct > 99.0,
        "expected >99% hit rate, got {hit_pct:.1}% ({non_null}/{n})"
    );

    println!("    PASS ({non_null}/{n} hits, {hit_pct:.1}%, {elapsed:.3?})");
}
