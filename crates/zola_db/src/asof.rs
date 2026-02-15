use std::collections::BTreeMap;

use crate::error::Result;
use crate::partition::micros_to_date_str;
use crate::partition::Partition;
use crate::schema::*;

#[derive(Debug)]
pub struct AsofResult {
    pub timestamps: Vec<i64>,
    pub columns: Vec<ColumnVec>,
}

pub fn asof_join(
    schema: &Schema,
    partitions: &BTreeMap<String, Partition>,
    probes: &Probes<'_>,
    direction: Direction,
) -> Result<AsofResult> {
    let n = probes.symbols.len();
    assert_eq!(n, probes.timestamps.len());

    let mut result_ts = vec![NULL_I64; n];
    let mut result_cols: Vec<ColumnVec> = schema
        .value_columns
        .iter()
        .map(|cd| match cd.col_type {
            ColumnType::I64 => ColumnVec::I64(vec![NULL_I64; n]),
            ColumnType::F64 => ColumnVec::F64(vec![NULL_F64; n]),
        })
        .collect();

    for probe_idx in 0..n {
        let sym = probes.symbols[probe_idx];
        let ts = probes.timestamps[probe_idx];
        let date_str = micros_to_date_str(ts)?;

        match direction {
            Direction::Backward => {
                backward_lookup(
                    schema,
                    partitions,
                    &date_str,
                    sym,
                    ts,
                    probe_idx,
                    &mut result_ts,
                    &mut result_cols,
                );
            }
            Direction::Forward => {
                forward_lookup(
                    schema,
                    partitions,
                    &date_str,
                    sym,
                    ts,
                    probe_idx,
                    &mut result_ts,
                    &mut result_cols,
                );
            }
        }
    }

    Ok(AsofResult {
        timestamps: result_ts,
        columns: result_cols,
    })
}

fn backward_lookup(
    schema: &Schema,
    partitions: &BTreeMap<String, Partition>,
    date_str: &str,
    sym: i64,
    ts: i64,
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    // Step 2: If partition missing, return NULL
    let part = match partitions.get(date_str) {
        Some(p) => p,
        None => return,
    };

    // Step 3-4: Find symbol range
    let range = match part.symbol_range(sym) {
        Some(r) => r,
        None => {
            // Symbol not in this partition -> check prev partition's last_values
            try_prev_sidecar(schema, partitions, date_str, sym, probe_idx, result_ts, result_cols);
            return;
        }
    };

    let timestamps = match part.get_i64("timestamp") {
        Some(t) => t,
        None => return,
    };
    let sym_timestamps = &timestamps[range.0..range.1];

    // Step 5: Binary search for largest t <= probe_ts
    let pos = sym_timestamps.partition_point(|&t| t <= ts);

    // Step 6: pos == 0 means all timestamps > probe_ts
    if pos == 0 {
        try_prev_sidecar(schema, partitions, date_str, sym, probe_idx, result_ts, result_cols);
        return;
    }

    // Step 7: Found match
    let row = range.0 + pos - 1;
    fill_from_partition(schema, part, row, probe_idx, result_ts, result_cols);
}

fn forward_lookup(
    schema: &Schema,
    partitions: &BTreeMap<String, Partition>,
    date_str: &str,
    sym: i64,
    ts: i64,
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    let part = match partitions.get(date_str) {
        Some(p) => p,
        None => return,
    };

    let range = match part.symbol_range(sym) {
        Some(r) => r,
        None => {
            try_next_sidecar(schema, partitions, date_str, sym, probe_idx, result_ts, result_cols);
            return;
        }
    };

    let timestamps = match part.get_i64("timestamp") {
        Some(t) => t,
        None => return,
    };
    let sym_timestamps = &timestamps[range.0..range.1];

    // Find first t >= probe_ts
    let pos = sym_timestamps.partition_point(|&t| t < ts);

    if pos == sym_timestamps.len() {
        // All timestamps < probe_ts -> check next partition
        try_next_sidecar(schema, partitions, date_str, sym, probe_idx, result_ts, result_cols);
        return;
    }

    let row = range.0 + pos;
    fill_from_partition(schema, part, row, probe_idx, result_ts, result_cols);
}

// --- Sidecar helpers ---

fn try_prev_sidecar(
    schema: &Schema,
    partitions: &BTreeMap<String, Partition>,
    date_str: &str,
    sym: i64,
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    if let Some(prev) = prev_partition(partitions, date_str) {
        if let Some(ref lv) = prev.last_values {
            if let Some(entry) = lv.get(&sym) {
                fill_from_sidecar(schema, entry, probe_idx, result_ts, result_cols);
            }
        }
    }
}

fn try_next_sidecar(
    schema: &Schema,
    partitions: &BTreeMap<String, Partition>,
    date_str: &str,
    sym: i64,
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    if let Some(next) = next_partition(partitions, date_str) {
        if let Some(ref fv) = next.first_values {
            if let Some(entry) = fv.get(&sym) {
                fill_from_sidecar(schema, entry, probe_idx, result_ts, result_cols);
            }
        }
    }
}

fn prev_partition<'a>(
    partitions: &'a BTreeMap<String, Partition>,
    date_str: &str,
) -> Option<&'a Partition> {
    partitions
        .range(..date_str.to_string())
        .next_back()
        .map(|(_, p)| p)
}

fn next_partition<'a>(
    partitions: &'a BTreeMap<String, Partition>,
    date_str: &str,
) -> Option<&'a Partition> {
    use std::ops::Bound;
    partitions
        .range((Bound::Excluded(date_str.to_string()), Bound::Unbounded))
        .next()
        .map(|(_, p)| p)
}

// --- Result filling ---

fn fill_from_partition(
    schema: &Schema,
    part: &Partition,
    row: usize,
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    if let Some(timestamps) = part.get_i64("timestamp") {
        result_ts[probe_idx] = timestamps[row];
    }

    for (ci, col_def) in schema.value_columns.iter().enumerate() {
        match col_def.col_type {
            ColumnType::I64 => {
                if let Some(data) = part.get_i64(&col_def.name) {
                    if let ColumnVec::I64(ref mut v) = result_cols[ci] {
                        v[probe_idx] = data[row];
                    }
                }
            }
            ColumnType::F64 => {
                if let Some(data) = part.get_f64(&col_def.name) {
                    if let ColumnVec::F64(ref mut v) = result_cols[ci] {
                        v[probe_idx] = data[row];
                    }
                }
            }
        }
    }
}

fn fill_from_sidecar(
    schema: &Schema,
    entry: &[u8],
    probe_idx: usize,
    result_ts: &mut [i64],
    result_cols: &mut [ColumnVec],
) {
    // entry layout: [timestamp: 8 bytes] [val0: 8 bytes] [val1: 8 bytes] ...
    if entry.len() < 8 {
        return;
    }
    result_ts[probe_idx] = i64::from_ne_bytes(entry[..8].try_into().unwrap());

    for (ci, col_def) in schema.value_columns.iter().enumerate() {
        let offset = 8 + ci * 8;
        if offset + 8 > entry.len() {
            break;
        }
        let bytes: [u8; 8] = entry[offset..offset + 8].try_into().unwrap();
        match col_def.col_type {
            ColumnType::I64 => {
                if let ColumnVec::I64(ref mut v) = result_cols[ci] {
                    v[probe_idx] = i64::from_ne_bytes(bytes);
                }
            }
            ColumnType::F64 => {
                if let ColumnVec::F64(ref mut v) = result_cols[ci] {
                    v[probe_idx] = f64::from_ne_bytes(bytes);
                }
            }
        }
    }
}
