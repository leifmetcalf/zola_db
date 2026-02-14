use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use memmap2::Mmap;
use zerocopy::FromBytes;

use crate::error::{ZolaError, Result};
use crate::storage::*;

pub struct Partition {
    pub dir: PathBuf,
    columns: HashMap<String, Mmap>,
    parted: Vec<PartedEntry>,
    pub row_count: u64,
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

        // Row count from timestamp column header
        let row_count = columns
            .get("timestamp")
            .map(|mmap| {
                ColumnHeader::ref_from_bytes(&mmap[..HEADER_SIZE])
                    .unwrap()
                    .row_count
            })
            .unwrap_or(0);

        // Load sidecars
        let last_values = load_sidecar(&dir.join(".last_values"))?;
        let first_values = load_sidecar(&dir.join(".first_values"))?;

        Ok(Partition {
            dir: dir.to_path_buf(),
            columns,
            parted,
            row_count,
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
