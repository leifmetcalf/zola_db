use std::collections::HashMap;
use std::fs;
use std::path::Path;
use memmap2::Mmap;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::{ZolaError, Result};
use crate::format::*;
use crate::io::*;
use crate::schema::{ColumnSlice, ColumnType, Schema};

pub struct Partition {
    columns: HashMap<String, Mmap>,
    parted: Vec<PartedEntry>,
    pub last_values: Option<HashMap<i64, Vec<u8>>>,
    pub first_values: Option<HashMap<i64, Vec<u8>>>,
}

impl Partition {
    pub fn open(dir: &Path) -> Result<Partition> {
        let mut columns = HashMap::new();

        for entry in fs::read_dir(dir).map_err(|e| ZolaError::io(dir, e))? {
            let entry = entry.map_err(|e| ZolaError::io(dir, e))?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "col") {
                let name = path
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                let file = fs::File::open(&path).map_err(|e| ZolaError::io(&path, e))?;
                let mmap = unsafe { Mmap::map(&file) }
                    .map_err(|e| ZolaError::io(&path, e))?;

                if mmap.len() < HEADER_SIZE {
                    return Err(ZolaError::invalid(&path, "file too small for header"));
                }
                let header = ColumnHeader::ref_from_bytes(&mmap[..HEADER_SIZE])
                    .map_err(|e| ZolaError::invalid(&path, format!("bad header: {e}")))?;
                if header.magic != COLUMN_MAGIC {
                    return Err(ZolaError::invalid(&path, "bad magic"));
                }

                columns.insert(name, mmap);
            }
        }

        // Load parted index
        let parted_path = dir.join(".parted");
        let parted = if parted_path.exists() {
            let data = fs::read(&parted_path).map_err(|e| ZolaError::io(&parted_path, e))?;
            let entries = <[PartedEntry]>::ref_from_bytes(&data)
                .map_err(|e| ZolaError::invalid(&parted_path, format!("bad parted index: {e}")))?;
            entries.to_vec()
        } else {
            vec![]
        };

        // Load sidecars
        let last_values = load_sidecar(&dir.join(".last_values"))?;
        let first_values = load_sidecar(&dir.join(".first_values"))?;

        Ok(Partition {
            columns,
            parted,
            last_values,
            first_values,
        })
    }

    pub fn get_i64(&self, col: &str) -> Option<&[i64]> {
        let mmap = self.columns.get(col)?;
        let data = &mmap[HEADER_SIZE..];
        <[i64]>::ref_from_bytes(data).ok()
    }

    pub fn get_f64(&self, col: &str) -> Option<&[f64]> {
        let mmap = self.columns.get(col)?;
        let data = &mmap[HEADER_SIZE..];
        <[f64]>::ref_from_bytes(data).ok()
    }

    pub fn symbol_range(&self, sym_id: i64) -> Option<(usize, usize)> {
        self.parted
            .binary_search_by_key(&sym_id, |e| e.symbol_id)
            .ok()
            .map(|i| (self.parted[i].start as usize, self.parted[i].end as usize))
    }
}

fn load_sidecar(path: &Path) -> Result<Option<HashMap<i64, Vec<u8>>>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(path).map_err(|e| ZolaError::io(path, e))?;
    if data.len() < SIDECAR_HEADER_SIZE {
        return Err(ZolaError::invalid(path, "sidecar too small"));
    }
    let header = SidecarHeader::ref_from_bytes(&data[..SIDECAR_HEADER_SIZE])
        .map_err(|e| ZolaError::invalid(path, format!("bad sidecar header: {e}")))?;
    if header.magic != SIDECAR_MAGIC {
        return Err(ZolaError::invalid(path, "bad sidecar magic"));
    }

    let entry_size = (2 + header.num_value_cols as usize) * 8;
    let entries_data = &data[SIDECAR_HEADER_SIZE..];

    let mut map = HashMap::new();
    for i in 0..header.num_symbols as usize {
        let offset = i * entry_size;
        if offset + entry_size > entries_data.len() {
            return Err(ZolaError::invalid(path, "sidecar truncated"));
        }
        let sym_id = i64::from_ne_bytes(
            entries_data[offset..offset + 8].try_into().unwrap(),
        );
        // Store timestamp + value bytes (skip symbol_id)
        let value_bytes = entries_data[offset + 8..offset + entry_size].to_vec();
        map.insert(sym_id, value_bytes);
    }

    Ok(Some(map))
}

// --- Ingest (write path) ---

pub fn write_table(
    root: &Path,
    table: &str,
    schema: &Schema,
    timestamps: &[i64],
    symbols: &[i64],
    columns: &[ColumnSlice<'_>],
) -> Result<()> {
    let n = timestamps.len();
    if n != symbols.len() {
        return Err(ZolaError::SchemaMismatch(format!(
            "timestamps len {} != symbols len {}", n, symbols.len()
        )));
    }
    if columns.len() != schema.value_columns.len() {
        return Err(ZolaError::SchemaMismatch(format!(
            "columns len {} != schema len {}", columns.len(), schema.value_columns.len()
        )));
    }
    for (i, col) in columns.iter().enumerate() {
        let len = match col {
            ColumnSlice::I64(s) => s.len(),
            ColumnSlice::F64(s) => s.len(),
        };
        if len != n {
            return Err(ZolaError::SchemaMismatch(format!(
                "column {} length {} != row count {}", schema.value_columns[i].name, len, n
            )));
        }
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
            for (ci, col_def) in schema.value_columns.iter().enumerate() {
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
