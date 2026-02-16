use std::collections::BTreeMap;
use std::path::PathBuf;

use zola_db::{ColumnDef, ColumnSlice, ColumnType, Db, Schema};

use crate::parse;

pub fn schema() -> Schema {
    Schema {
        value_columns: vec![
            ColumnDef { name: "price".into(), col_type: ColumnType::F64 },
            ColumnDef { name: "quantity".into(), col_type: ColumnType::F64 },
        ],
    }
}

/// Ingest downloaded zip files into the database, one `db.write` call per date.
pub fn ingest(
    db: &mut Db,
    files: &[(String, String, PathBuf)],
    symbol_ids: &BTreeMap<String, i64>,
) {
    let schema = schema();

    // Group files by date
    let mut by_date: BTreeMap<String, Vec<&(String, String, PathBuf)>> = BTreeMap::new();
    for item in files {
        by_date.entry(item.1.clone()).or_default().push(item);
    }

    for (date, date_files) in &by_date {
        println!("  ingesting {date}: {} symbol files", date_files.len());

        let mut timestamps = Vec::new();
        let mut symbols = Vec::new();
        let mut prices = Vec::new();
        let mut quantities = Vec::new();

        for &(symbol, _, path) in date_files.iter() {
            let sym_id = match symbol_ids.get(symbol) {
                Some(&id) => id,
                None => continue,
            };

            let trades = parse::parse_zip(path);
            for t in &trades {
                timestamps.push(t.timestamp_us);
                symbols.push(sym_id);
                prices.push(t.price);
                quantities.push(t.quantity);
            }
        }

        if timestamps.is_empty() {
            println!("  skipping {date}: no trades");
            continue;
        }

        println!(
            "  writing {date}: {} rows across {} symbols",
            timestamps.len(),
            date_files.len()
        );

        db.write(
            "binance_agg_trades",
            &schema,
            &timestamps,
            &symbols,
            &[ColumnSlice::F64(&prices), ColumnSlice::F64(&quantities)],
        )
        .expect("db.write failed");
    }
}
