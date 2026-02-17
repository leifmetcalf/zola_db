use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use zerocopy::IntoBytes;

use crate::error::{ZolaError, Result};
use crate::format::*;
use crate::schema::{ColumnType, Schema};

// --- File writing helpers ---

pub fn write_column_file(path: &Path, col_type: ColumnType, data: &[u8]) -> Result<()> {
    let row_count = data.len() as u64 / 8;
    let header = ColumnHeader {
        magic: COLUMN_MAGIC,
        version: VERSION,
        col_type: col_type as u32,
        _pad: 0,
        row_count,
    };
    let mut file = fs::File::create(path).map_err(|e| ZolaError::io(path, e))?;
    file.write_all(header.as_bytes())
        .map_err(|e| ZolaError::io(path, e))?;
    file.write_all(data)
        .map_err(|e| ZolaError::io(path, e))?;
    file.sync_all().map_err(|e| ZolaError::io(path, e))?;
    Ok(())
}

pub fn write_parted_file(path: &Path, entries: &[PartedEntry]) -> Result<()> {
    let mut file = fs::File::create(path).map_err(|e| ZolaError::io(path, e))?;
    for entry in entries {
        file.write_all(entry.as_bytes())
            .map_err(|e| ZolaError::io(path, e))?;
    }
    file.sync_all().map_err(|e| ZolaError::io(path, e))?;
    Ok(())
}

pub fn write_sidecar_file(
    path: &Path,
    num_value_cols: u32,
    entries: &[(u64, Vec<u8>)],
) -> Result<()> {
    let header = SidecarHeader {
        magic: SIDECAR_MAGIC,
        num_symbols: entries.len() as u32,
        num_value_cols,
        _pad: 0,
    };
    let mut file = fs::File::create(path).map_err(|e| ZolaError::io(path, e))?;
    file.write_all(header.as_bytes())
        .map_err(|e| ZolaError::io(path, e))?;
    let expected_val_size = sidecar_entry_size(num_value_cols) - 8; // entry minus sym_id
    for (sym_id, val_bytes) in entries {
        debug_assert_eq!(val_bytes.len(), expected_val_size);
        file.write_all(&sym_id.to_ne_bytes())
            .map_err(|e| ZolaError::io(path, e))?;
        file.write_all(val_bytes)
            .map_err(|e| ZolaError::io(path, e))?;
    }
    file.sync_all().map_err(|e| ZolaError::io(path, e))?;
    Ok(())
}

// --- Atomic partition write ---

pub fn atomic_write_partition(
    final_dir: &Path,
    write_fn: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let tmp_dir = append_ext(final_dir, ".tmp");
    let old_dir = append_ext(final_dir, ".old");

    // Clean up previous failed attempts
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir).map_err(|e| ZolaError::io(&tmp_dir, e))?;
    }

    fs::create_dir_all(&tmp_dir).map_err(|e| ZolaError::io(&tmp_dir, e))?;

    // Write all files into temp directory
    write_fn(&tmp_dir)?;

    // Fsync temp directory
    fsync_dir(&tmp_dir)?;

    // Swap: existing -> .old, then .tmp -> final
    if final_dir.exists() {
        fs::rename(final_dir, &old_dir).map_err(|e| ZolaError::io(final_dir, e))?;
    }
    fs::rename(&tmp_dir, final_dir).map_err(|e| ZolaError::io(&tmp_dir, e))?;

    if old_dir.exists() {
        fs::remove_dir_all(&old_dir).map_err(|e| ZolaError::io(&old_dir, e))?;
    }

    // Fsync parent to persist the rename
    if let Some(parent) = final_dir.parent() {
        fsync_dir(parent)?;
    }

    Ok(())
}

// --- Schema file ---

pub fn write_schema_file(dir: &Path, schema: &Schema) -> Result<()> {
    let path = dir.join(".schema");
    let mut content = String::new();
    for col in &schema.value_columns {
        let type_str = match col.col_type {
            ColumnType::I64 => "i64",
            ColumnType::F64 => "f64",
        };
        content.push_str(&format!("{}:{}\n", col.name, type_str));
    }
    fs::write(&path, content).map_err(|e| ZolaError::io(&path, e))?;
    Ok(())
}

pub fn read_schema_file(dir: &Path) -> Result<Option<Schema>> {
    let path = dir.join(".schema");
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path).map_err(|e| ZolaError::io(&path, e))?;
    let mut columns = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, type_str) = line
            .split_once(':')
            .ok_or_else(|| ZolaError::invalid(&path, format!("bad schema line: {line}")))?;
        let col_type = match type_str {
            "i64" => ColumnType::I64,
            "f64" => ColumnType::F64,
            _ => return Err(ZolaError::invalid(&path, format!("unknown type: {type_str}"))),
        };
        columns.push(crate::schema::ColumnDef {
            name: name.to_string(),
            col_type,
        });
    }
    Ok(Some(Schema {
        value_columns: columns,
    }))
}

// --- Recovery ---

pub fn recover(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for table_entry in fs::read_dir(root).map_err(|e| ZolaError::io(root, e))? {
        let table_entry = table_entry.map_err(|e| ZolaError::io(root, e))?;
        let table_path = table_entry.path();
        if !table_path.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&table_path).map_err(|e| ZolaError::io(&table_path, e))? {
            let entry = entry.map_err(|e| ZolaError::io(&table_path, e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".tmp") || name.ends_with(".old") {
                let path = entry.path();
                if path.is_dir() {
                    fs::remove_dir_all(&path).map_err(|e| ZolaError::io(&path, e))?;
                }
            }
        }
    }
    Ok(())
}

// --- Helpers ---

fn append_ext(path: &Path, ext: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(ext);
    PathBuf::from(s)
}

fn fsync_dir(dir: &Path) -> Result<()> {
    let f = fs::File::open(dir).map_err(|e| ZolaError::io(dir, e))?;
    f.sync_all().map_err(|e| ZolaError::io(dir, e))?;
    Ok(())
}
