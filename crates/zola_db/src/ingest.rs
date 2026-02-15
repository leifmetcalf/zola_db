use std::fs;
use std::path::Path;
use zerocopy::IntoBytes;

use crate::error::{ZolaError, Result};
use crate::schema::{ColumnSlice, ColumnType, Schema};
use crate::storage::*;

pub fn write_table(
    root: &Path,
    table: &str,
    schema: &Schema,
    timestamps: &[i64],
    symbols: &[i64],
    columns: &[ColumnSlice<'_>],
) -> Result<()> {
    let n = timestamps.len();
    assert_eq!(n, symbols.len());
    assert_eq!(columns.len(), schema.value_columns.len());
    for (i, col) in columns.iter().enumerate() {
        let len = match col {
            ColumnSlice::I64(s) => s.len(),
            ColumnSlice::F64(s) => s.len(),
        };
        assert_eq!(len, n, "column {} length mismatch", schema.value_columns[i].name);
    }

    if n == 0 {
        return Ok(());
    }

    let table_dir = root.join(table);
    fs::create_dir_all(&table_dir).map_err(|e| ZolaError::io(&table_dir, e))?;

    // Write schema file if it doesn't exist
    let schema_path = table_dir.join(".schema");
    if !schema_path.exists() {
        write_schema_file(&table_dir, schema)?;
    }

    // Compute date keys for each row
    let date_ints: Vec<i32> = timestamps
        .iter()
        .map(|&ts| micros_to_date_int(ts))
        .collect::<Result<_>>()?;

    // Sort indices by (date, symbol, timestamp)
    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_unstable_by(|&a, &b| {
        date_ints[a]
            .cmp(&date_ints[b])
            .then(symbols[a].cmp(&symbols[b]))
            .then(timestamps[a].cmp(&timestamps[b]))
    });

    // Group by date
    let mut groups: Vec<(i32, Vec<usize>)> = Vec::new();
    for &i in &indices {
        let date = date_ints[i];
        if groups.last().is_some_and(|(d, _)| *d == date) {
            groups.last_mut().unwrap().1.push(i);
        } else {
            groups.push((date, vec![i]));
        }
    }

    // Write each date group as a partition
    for (date_int, group_indices) in &groups {
        let date_str = date_int_to_str(*date_int);
        let part_dir = table_dir.join(&date_str);

        // Build sorted arrays for this group
        let sorted_ts: Vec<i64> = group_indices.iter().map(|&i| timestamps[i]).collect();
        let sorted_syms: Vec<i64> = group_indices.iter().map(|&i| symbols[i]).collect();

        let sorted_vcol_bytes: Vec<Vec<u8>> = columns
            .iter()
            .map(|col| {
                let mut buf = Vec::with_capacity(group_indices.len() * 8);
                for &i in group_indices {
                    match col {
                        ColumnSlice::I64(s) => buf.extend_from_slice(&s[i].to_ne_bytes()),
                        ColumnSlice::F64(s) => buf.extend_from_slice(&s[i].to_ne_bytes()),
                    }
                }
                buf
            })
            .collect();

        // Build parted index
        let parted = build_parted_index(&sorted_syms);

        // Build sidecars
        let last_entries =
            build_sidecar_entries(&parted, &sorted_ts, &sorted_vcol_bytes, true);
        let first_entries =
            build_sidecar_entries(&parted, &sorted_ts, &sorted_vcol_bytes, false);

        let num_vcols = schema.value_columns.len() as u32;
        let schema_ref = schema;

        atomic_write_partition(&part_dir, |tmp_dir| {
            // Write timestamp column
            write_column_file(
                &tmp_dir.join("timestamp.col"),
                ColumnType::I64,
                sorted_ts.as_bytes(),
            )?;

            // Write symbol column
            write_column_file(
                &tmp_dir.join("symbol.col"),
                ColumnType::I64,
                sorted_syms.as_bytes(),
            )?;

            // Write value columns
            for (ci, col_def) in schema_ref.value_columns.iter().enumerate() {
                write_column_file(
                    &tmp_dir.join(format!("{}.col", col_def.name)),
                    col_def.col_type,
                    &sorted_vcol_bytes[ci],
                )?;
            }

            // Write parted index
            write_parted_file(&tmp_dir.join(".parted"), &parted)?;

            // Write sidecars
            write_sidecar_file(&tmp_dir.join(".last_values"), num_vcols, &last_entries)?;
            write_sidecar_file(&tmp_dir.join(".first_values"), num_vcols, &first_entries)?;

            Ok(())
        })?;
    }

    Ok(())
}

pub(crate) fn micros_to_date_str(ts_micros: i64) -> Result<String> {
    let date_int = micros_to_date_int(ts_micros)?;
    Ok(date_int_to_str(date_int))
}

fn micros_to_date_int(ts_micros: i64) -> Result<i32> {
    let ts = jiff::Timestamp::from_microsecond(ts_micros).map_err(|e| {
        ZolaError::SchemaMismatch(format!("invalid timestamp {ts_micros}: {e}"))
    })?;
    let dt = ts.to_zoned(jiff::tz::TimeZone::UTC);
    let d = dt.date();
    Ok(d.year() as i32 * 10000 + d.month() as i32 * 100 + d.day() as i32)
}

fn date_int_to_str(date_int: i32) -> String {
    let y = date_int / 10000;
    let m = (date_int % 10000) / 100;
    let d = date_int % 100;
    format!("{y:04}.{m:02}.{d:02}")
}

fn build_parted_index(symbols: &[i64]) -> Vec<PartedEntry> {
    if symbols.is_empty() {
        return vec![];
    }
    let mut entries = Vec::new();
    let mut start = 0usize;
    let mut current = symbols[0];
    for (i, &sym) in symbols.iter().enumerate() {
        if sym != current {
            entries.push(PartedEntry {
                symbol_id: current,
                start: start as u64,
                end: i as u64,
            });
            start = i;
            current = sym;
        }
    }
    entries.push(PartedEntry {
        symbol_id: current,
        start: start as u64,
        end: symbols.len() as u64,
    });
    entries
}

fn build_sidecar_entries(
    parted: &[PartedEntry],
    sorted_ts: &[i64],
    sorted_vcol_bytes: &[Vec<u8>],
    pick_last: bool,
) -> Vec<(i64, Vec<u8>)> {
    parted
        .iter()
        .map(|entry| {
            let row = if pick_last {
                entry.end as usize - 1
            } else {
                entry.start as usize
            };
            let num_vcols = sorted_vcol_bytes.len();
            let mut bytes = Vec::with_capacity((1 + num_vcols) * 8);
            bytes.extend_from_slice(&sorted_ts[row].to_ne_bytes());
            for col_bytes in sorted_vcol_bytes {
                let offset = row * 8;
                bytes.extend_from_slice(&col_bytes[offset..offset + 8]);
            }
            (entry.symbol_id, bytes)
        })
        .collect()
}
