mod error;
mod schema;
mod format;
mod io;
mod partition;
mod asof;

pub use error::{ZolaError, Result};
pub use schema::{
    Schema, ColumnDef, ColumnType, Direction, Probes,
    ColumnSlice, ColumnVec, NULL_I64, NULL_F64,
};
pub use asof::AsofResult;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

struct TableInfo {
    schema: Schema,
    partitions: BTreeMap<String, partition::Partition>,
}

pub struct Db {
    root: PathBuf,
    tables: BTreeMap<String, TableInfo>,
}

impl Db {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();

        if root.exists() {
            io::recover(&root)?;
        } else {
            std::fs::create_dir_all(&root).map_err(|e| ZolaError::io(&root, e))?;
        }

        let mut tables = BTreeMap::new();
        if root.exists() {
            for entry in std::fs::read_dir(&root).map_err(|e| ZolaError::io(&root, e))? {
                let entry = entry.map_err(|e| ZolaError::io(&root, e))?;
                let path = entry.path();
                if path.is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    if let Some(info) = load_table(&path)? {
                        tables.insert(table_name, info);
                    }
                }
            }
        }

        Ok(Db { root, tables })
    }

    pub fn write(
        &mut self,
        table: &str,
        schema: &Schema,
        timestamps: &[i64],
        symbols: &[i64],
        columns: &[ColumnSlice<'_>],
    ) -> Result<()> {
        partition::write_table(&self.root, table, schema, timestamps, symbols, columns)?;

        // Reload the table
        let table_dir = self.root.join(table);
        if let Some(info) = load_table(&table_dir)? {
            self.tables.insert(table.to_string(), info);
        }

        Ok(())
    }

    pub fn asof(
        &self,
        table: &str,
        probes: &Probes<'_>,
        direction: Direction,
    ) -> Result<AsofResult> {
        let info = self
            .tables
            .get(table)
            .ok_or_else(|| ZolaError::TableNotFound(table.to_string()))?;
        asof::asof_join(&info.schema, &info.partitions, probes, direction)
    }
}

fn load_table(table_dir: &Path) -> Result<Option<TableInfo>> {
    let schema = match io::read_schema_file(table_dir)? {
        Some(s) => s,
        None => return Ok(None),
    };

    let mut partitions = BTreeMap::new();
    for entry in std::fs::read_dir(table_dir).map_err(|e| ZolaError::io(table_dir, e))? {
        let entry = entry.map_err(|e| ZolaError::io(table_dir, e))?;
        let path = entry.path();
        if path.is_dir() {
            let date_str = entry.file_name().to_string_lossy().to_string();
            if is_date_str(&date_str) {
                let part = partition::Partition::open(&path)?;
                partitions.insert(date_str, part);
            }
        }
    }

    Ok(Some(TableInfo { schema, partitions }))
}

fn is_date_str(s: &str) -> bool {
    s.len() == 10
        && s.as_bytes().get(4) == Some(&b'.')
        && s.as_bytes().get(7) == Some(&b'.')
        && s[..4].chars().all(|c| c.is_ascii_digit())
        && s[5..7].chars().all(|c| c.is_ascii_digit())
        && s[8..].chars().all(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: microseconds for a given offset from Jan 15, 2024 00:00:00 UTC.
    /// `days` can be negative for dates before Jan 15.
    fn ts(days: i64, hours: i64, mins: i64, secs: i64, micros: i64) -> i64 {
        const JAN15_2024_EPOCH_SECS: i64 = 1_705_276_800;
        (JAN15_2024_EPOCH_SECS + days * 86400 + hours * 3600 + mins * 60 + secs)
            * 1_000_000
            + micros
    }

    fn price_schema() -> Schema {
        Schema {
            value_columns: vec![ColumnDef {
                name: "price".into(),
                col_type: ColumnType::F64,
            }],
        }
    }

    fn two_col_schema() -> Schema {
        Schema {
            value_columns: vec![
                ColumnDef { name: "price".into(), col_type: ColumnType::F64 },
                ColumnDef { name: "size".into(), col_type: ColumnType::I64 },
            ],
        }
    }

    // ---- Test 1: Basic backward asof within a single partition ----

    #[test]
    fn test_backward_asof_basic() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        // Three ticks on Jan 15: sym 1 at 10:00:00 and 10:00:01, sym 2 at 10:00:02
        let timestamps = vec![ts(0, 10, 0, 0, 0), ts(0, 10, 0, 1, 0), ts(0, 10, 0, 2, 0)];
        let symbols = vec![1_i64, 1, 2];
        let prices = vec![100.0_f64, 100.5, 200.0];

        db.write("trades", &schema, &timestamps, &symbols, &[ColumnSlice::F64(&prices)])
            .unwrap();

        // Probe: sym 1 at 10:00:00.5 -> should find 10:00:00 (100.0)
        // Probe: sym 2 at 10:00:03   -> should find 10:00:02 (200.0)
        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1, 2],
                    timestamps: &[ts(0, 10, 0, 0, 500_000), ts(0, 10, 0, 3, 0)],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], ts(0, 10, 0, 0, 0));
        assert_eq!(result.timestamps[1], ts(0, 10, 0, 2, 0));

        let ColumnVec::F64(ref p) = result.columns[0] else {
            panic!("expected F64");
        };
        assert_eq!(p[0], 100.0);
        assert_eq!(p[1], 200.0);
    }

    // ---- Test 2: Forward asof ----

    #[test]
    fn test_forward_asof_basic() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        let timestamps = vec![ts(0, 10, 0, 0, 0), ts(0, 10, 0, 1, 0)];
        let symbols = vec![1_i64, 1];
        let prices = vec![100.0_f64, 100.5];

        db.write("trades", &schema, &timestamps, &symbols, &[ColumnSlice::F64(&prices)])
            .unwrap();

        // Probe: sym 1 at 10:00:00.5 -> forward should find 10:00:01 (100.5)
        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[ts(0, 10, 0, 0, 500_000)],
                },
                Direction::Forward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], ts(0, 10, 0, 1, 0));
        let ColumnVec::F64(ref p) = result.columns[0] else {
            panic!("expected F64");
        };
        assert_eq!(p[0], 100.5);
    }

    // ---- Test 3: Cross-partition backward sidecar (Case A) ----

    #[test]
    fn test_boundary_sidecar_backward() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        // Jan 15: sym 1 at 23:59:59
        db.write(
            "trades",
            &schema,
            &[ts(0, 23, 59, 59, 0)],
            &[1_i64],
            &[ColumnSlice::F64(&[150.0])],
        )
        .unwrap();

        // Jan 16: sym 1 at 10:00:00
        db.write(
            "trades",
            &schema,
            &[ts(1, 10, 0, 0, 0)],
            &[1],
            &[ColumnSlice::F64(&[160.0])],
        )
        .unwrap();

        // Probe: sym 1 at Jan 16 09:00 (before any Jan 16 data)
        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[ts(1, 9, 0, 0, 0)],
                },
                Direction::Backward,
            )
            .unwrap();

        // Should find Jan 15 last value via sidecar
        assert_eq!(result.timestamps[0], ts(0, 23, 59, 59, 0));
        let ColumnVec::F64(ref p) = result.columns[0] else {
            panic!("expected F64");
        };
        assert_eq!(p[0], 150.0);
    }

    // ---- Test 4: Strict no-recursive lookback (Case B) ----

    #[test]
    fn test_strict_no_recursive_lookback() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        // Day -2 (Jan 13): sym 1 has data
        db.write(
            "trades",
            &schema,
            &[ts(-2, 12, 0, 0, 0)],
            &[1_i64],
            &[ColumnSlice::F64(&[99.0])],
        )
        .unwrap();

        // Day -1 (Jan 14): only sym 2 (no sym 1!)
        db.write(
            "trades",
            &schema,
            &[ts(-1, 12, 0, 0, 0)],
            &[2_i64],
            &[ColumnSlice::F64(&[300.0])],
        )
        .unwrap();

        // Day 0 (Jan 15): only sym 2
        db.write(
            "trades",
            &schema,
            &[ts(0, 12, 0, 0, 0)],
            &[2_i64],
            &[ColumnSlice::F64(&[301.0])],
        )
        .unwrap();

        // Probe: sym 1 on Jan 15 -> Jan 14 sidecar has no sym 1 -> NULL
        // Must NOT recurse to Jan 13
        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[ts(0, 12, 0, 0, 0)],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], NULL_I64);
        let ColumnVec::F64(ref p) = result.columns[0] else {
            panic!("expected F64");
        };
        assert!(p[0].is_nan());
    }

    // ---- Test 5: Microsecond precision roundtrip ----

    #[test]
    fn test_microsecond_precision() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        // Timestamps with specific microsecond values
        let t1 = ts(0, 10, 0, 0, 1);     // ...000001
        let t2 = ts(0, 10, 0, 0, 999999); // ...999999
        let timestamps = vec![t1, t2];
        let symbols = vec![1_i64, 1];
        let prices = vec![1.0_f64, 2.0];

        db.write("trades", &schema, &timestamps, &symbols, &[ColumnSlice::F64(&prices)])
            .unwrap();

        // Exact match at t1
        let r1 = db
            .asof(
                "trades",
                &Probes { symbols: &[1], timestamps: &[t1] },
                Direction::Backward,
            )
            .unwrap();
        assert_eq!(r1.timestamps[0], t1);

        // Probe between t1 and t2 -> backward finds t1
        let r2 = db
            .asof(
                "trades",
                &Probes { symbols: &[1], timestamps: &[t1 + 500_000] },
                Direction::Backward,
            )
            .unwrap();
        assert_eq!(r2.timestamps[0], t1);

        // Probe between t1 and t2 -> forward finds t2
        let r3 = db
            .asof(
                "trades",
                &Probes { symbols: &[1], timestamps: &[t1 + 500_000] },
                Direction::Forward,
            )
            .unwrap();
        assert_eq!(r3.timestamps[0], t2);
    }

    // ---- Test 6: Multi-column roundtrip ----

    #[test]
    fn test_multi_column() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = two_col_schema();

        let timestamps = vec![ts(0, 10, 0, 0, 0), ts(0, 10, 0, 1, 0)];
        let symbols = vec![1_i64, 1];
        let prices = vec![50.5_f64, 51.0];
        let sizes = vec![100_i64, 200];

        db.write(
            "trades",
            &schema,
            &timestamps,
            &symbols,
            &[ColumnSlice::F64(&prices), ColumnSlice::I64(&sizes)],
        )
        .unwrap();

        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[ts(0, 10, 0, 0, 500_000)],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], ts(0, 10, 0, 0, 0));
        let ColumnVec::F64(ref p) = result.columns[0] else {
            panic!("expected F64");
        };
        assert_eq!(p[0], 50.5);
        let ColumnVec::I64(ref s) = result.columns[1] else {
            panic!("expected I64");
        };
        assert_eq!(s[0], 100);
    }

    // ---- Test 7: Missing partition returns NULL ----

    #[test]
    fn test_missing_partition_null() {
        let dir = TempDir::new().unwrap();
        let mut db = Db::open(dir.path()).unwrap();
        let schema = price_schema();

        // Only Jan 15 data
        db.write(
            "trades",
            &schema,
            &[ts(0, 10, 0, 0, 0)],
            &[1_i64],
            &[ColumnSlice::F64(&[100.0])],
        )
        .unwrap();

        // Probe on Jan 20 (no partition) -> NULL
        let result = db
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[ts(5, 10, 0, 0, 0)],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], NULL_I64);
    }
}
