use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::types::{Int32Type, Int64Type};
use arrow::array::{ArrayRef, AsArray, RunArray, StringArray, new_null_array};
use arrow::buffer::Buffer;
use arrow::compute::interleave;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{FileDecoder, read_footer_length};
use arrow::ipc::root_as_footer;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("symbol {0:?} appears in multiple non-contiguous runs")]
    NonContiguousSymbol(String),

    #[error("unsorted timestamps for symbol {0:?}")]
    UnsortedTimestamps(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

pub use zola_db_core::{Direction, EpochDay, SYMBOL_COL, TIMESTAMP_COL};

struct Partition {
    symbol_index: HashMap<String, Range<usize>>,
    batch: RecordBatch,
}

impl Partition {
    /// Builds the symbol index and validates timestamp sortedness per symbol.
    fn new(batch: RecordBatch) -> Result<Self, Error> {
        let symbol_index = build_symbol_index(&batch)?;
        let ts_col = batch.column_by_name(TIMESTAMP_COL).ok_or_else(|| {
            arrow::error::ArrowError::SchemaError("missing timestamp column".into())
        })?;
        let ts = ts_col
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<Int64Type>>()
            .ok_or_else(|| {
                arrow::error::ArrowError::SchemaError("timestamp column must be Int64".into())
            })?
            .values();
        for (symbol, range) in &symbol_index {
            if !ts[range.clone()].is_sorted() {
                return Err(Error::UnsortedTimestamps(symbol.clone()));
            }
        }
        Ok(Self {
            symbol_index,
            batch,
        })
    }

    /// Reads a single-batch Arrow IPC file and wraps it as a `Partition`.
    fn load(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let bytes = bytes::Bytes::from_owner(mmap);
        let buffer = Buffer::from(bytes);

        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())?;
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
            .map_err(|e| arrow::error::ArrowError::IpcError(e.to_string()))?;

        let schema = fb_to_schema(footer.schema().unwrap());
        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());

        for block in footer.dictionaries().iter().flatten() {
            let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
            let data = buffer.slice_with_length(block.offset() as _, block_len);
            decoder.read_dictionary(block, &data)?;
        }

        let batches: Vec<_> = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        if batches.len() != 1 {
            return Err(arrow::error::ArrowError::IpcError(format!(
                "expected exactly one record batch, got {}",
                batches.len()
            ))
            .into());
        }

        let block = &batches[0];
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as _, block_len);
        let batch = decoder
            .read_record_batch(block, &data)?
            .expect("record batch was None");

        // Construct directly rather than calling `Self::new` to avoid the
        // O(rows) timestamp-sortedness check — on-disk data was already
        // validated at ingest time.
        let symbol_index = build_symbol_index(&batch).expect("corrupt on-disk partition");
        Ok(Self {
            symbol_index,
            batch,
        })
    }


    /// Writes this partition's batch to an Arrow IPC file, creating parent dirs.
    /// Uses write-to-temp + rename for atomicity and mmap safety.
    fn save(&self, path: &Path) -> Result<(), Error> {
        let parent = path.parent().expect("partition path must have a parent");
        fs::create_dir_all(parent)?;

        let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
        let mut writer = FileWriter::try_new(tmp.as_file_mut(), &self.batch.schema())?;
        writer.write(&self.batch)?;
        writer.finish()?;
        tmp.persist(path).map_err(|e| e.error)?;
        Ok(())
    }
}

fn build_symbol_index(batch: &RecordBatch) -> Result<HashMap<String, Range<usize>>, Error> {
    let col = batch.column_by_name(SYMBOL_COL).ok_or_else(|| {
        arrow::error::ArrowError::SchemaError("missing symbol column".into())
    })?;
    let run_array = col
        .as_any()
        .downcast_ref::<RunArray<Int32Type>>()
        .ok_or_else(|| {
            arrow::error::ArrowError::SchemaError(
                "symbol column must be RunEndEncoded(Int32, Utf8)".into(),
            )
        })?;
    let run_ends = run_array.run_ends().values();
    let values = run_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            arrow::error::ArrowError::SchemaError("symbol values must be Utf8".into())
        })?;

    let mut index = HashMap::with_capacity(run_ends.len());
    let mut start = 0usize;
    for (i, &end) in run_ends.iter().enumerate() {
        let end = end as usize;
        let symbol = values.value(i);
        if index.insert(symbol.to_string(), start..end).is_some() {
            return Err(Error::NonContiguousSymbol(symbol.to_string()));
        }
        start = end;
    }
    Ok(index)
}

struct Table {
    schema: SchemaRef,
    partitions: BTreeMap<EpochDay, Partition>,
}

impl Table {
    /// For each query timestamp, finds the matching row for `symbol` using an
    /// as-of join in the given `direction`.
    fn join_asof(
        &self,
        symbol: &str,
        query_ts: &RecordBatch,
        direction: Direction,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let ts_col = query_ts.column_by_name(TIMESTAMP_COL).unwrap().as_primitive::<Int64Type>().values();
        let out_schema = output_schema(&self.schema);

        // Pre-resolve symbol ranges once across all partitions.
        let resolved: BTreeMap<EpochDay, (Range<usize>, &[i64])> = self
            .partitions
            .iter()
            .filter_map(|(&day, part)| {
                let range = part.symbol_index.get(symbol)?.clone();
                let ts = part.batch
                    .column_by_name(TIMESTAMP_COL)
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .values()
                    .as_ref();
                Some((day, (range, ts)))
            })
            .collect();

        // Map resolved days to source indices for interleave.
        let day_to_src: HashMap<EpochDay, usize> = resolved
            .keys()
            .enumerate()
            .map(|(i, &d)| (d, i))
            .collect();
        let null_src = resolved.len();

        let indices: Vec<(usize, usize)> = ts_col
            .iter()
            .map(|&qt| {
                let day = EpochDay::from_timestamp_us(qt);
                match direction {
                    Direction::Backward => {
                        for (&d, (range, ts)) in resolved.range(..=day).rev() {
                            if d == day {
                                let pos = ts[range.clone()].partition_point(|&t| t <= qt);
                                if pos > 0 {
                                    return (day_to_src[&d], range.start + pos - 1);
                                }
                            } else {
                                return (day_to_src[&d], range.end - 1);
                            }
                        }
                    }
                    Direction::Forward => {
                        for (&d, (range, ts)) in resolved.range(day..) {
                            if d == day {
                                let symbol_ts = &ts[range.clone()];
                                let pos = symbol_ts.partition_point(|&t| t < qt);
                                if pos < symbol_ts.len() {
                                    return (day_to_src[&d], range.start + pos);
                                }
                            } else {
                                return (day_to_src[&d], range.start);
                            }
                        }
                    }
                }
                (null_src, 0)
            })
            .collect();

        let null_arrays: Vec<ArrayRef> = out_schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), 1))
            .collect();

        let col_indices: Vec<usize> = out_schema
            .fields()
            .iter()
            .map(|f| self.schema.index_of(f.name()).unwrap())
            .collect();

        let columns: Vec<ArrayRef> = col_indices
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                let mut sources: Vec<&dyn arrow::array::Array> = resolved
                    .keys()
                    .map(|d| self.partitions[d].batch.column(col_idx).as_ref())
                    .collect();
                sources.push(null_arrays[i].as_ref());
                interleave(&sources, &indices)
            })
            .collect::<Result<_, _>>()?;

        RecordBatch::try_new(out_schema, columns)
    }
}

fn output_schema(table_schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = table_schema
        .fields()
        .iter()
        .filter(|f| f.name() != SYMBOL_COL)
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn day_to_filename(day: EpochDay) -> String {
    let date: jiff::civil::Date = day.into();
    format!("{date}.arrow")
}

fn parse_day(stem: &str) -> Option<EpochDay> {
    let date: jiff::civil::Date = stem.parse().ok()?;
    Some(date.into())
}

pub struct Db {
    root: PathBuf,
    tables: HashMap<String, Table>,
}

impl Db {
    /// Opens a database from `root`, eagerly loading every partition into memory.
    ///
    /// The directory layout is `<root>/<table>/<YYYY-MM-DD>.arrow`.
    /// Returns an empty `Db` if `root` does not exist.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        let mut db = Db {
            root: root.as_ref().to_path_buf(),
            tables: HashMap::new(),
        };

        if !db.root.exists() {
            return Ok(db);
        }

        let mut table_dirs: Vec<_> = fs::read_dir(root)?.collect::<Result<Vec<_>, _>>()?;
        table_dirs.retain(|e| e.file_type().is_ok_and(|t| t.is_dir()));
        table_dirs.sort_by_key(|e| e.file_name());

        for table_entry in table_dirs {
            let table_name = table_entry.file_name().to_string_lossy().into_owned();

            let mut arrow_files: Vec<_> =
                fs::read_dir(table_entry.path())?.collect::<Result<Vec<_>, _>>()?;
            arrow_files.retain(|e| e.path().extension().is_some_and(|ext| ext == "arrow"));
            arrow_files.sort_by_key(|e| e.file_name());

            for file_entry in arrow_files {
                let stem = file_entry
                    .path()
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                let day = parse_day(&stem).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid partition date: {stem}"),
                    )
                })?;
                let partition = Partition::load(&file_entry.path())?;
                let table = db.tables.entry(table_name.clone()).or_insert_with(|| Table {
                    schema: partition.batch.schema(),
                    partitions: BTreeMap::new(),
                });
                table.partitions.insert(day, partition);
            }
        }

        Ok(db)
    }

    /// Stores a record batch as a partition, writing it to disk immediately.
    /// Replaces existing data for same table+date.
    /// The first batch defines the table schema; subsequent batches must have matching
    /// fields or the call returns an error.
    pub fn ingest(&mut self, table: &str, day: EpochDay, batch: RecordBatch) -> Result<(), Error> {
        let tbl = self.tables.entry(table.to_string()).or_insert_with(|| Table {
            schema: batch.schema(),
            partitions: BTreeMap::new(),
        });

        if tbl.schema.fields() != batch.schema().fields() {
            return Err(arrow::error::ArrowError::SchemaError(format!(
                "expected schema {:?}, got {:?}",
                tbl.schema.fields(),
                batch.schema().fields(),
            ))
            .into());
        }

        let partition = Partition::new(batch)?;
        let path = self.root.join(table).join(day_to_filename(day));
        partition.save(&path)?;
        tbl.partitions.insert(day, partition);
        Ok(())
    }

    /// For each query timestamp, finds the matching row in `table` for `symbol`
    /// using an as-of join in the given `direction`.
    ///
    /// Returns a `RecordBatch` with nullable columns — null indicates no match.
    pub fn join_asof(
        &self,
        table: &str,
        symbol: &str,
        timestamps: &RecordBatch,
        direction: Direction,
    ) -> Result<RecordBatch, Error> {
        let table = self
            .tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        Ok(table.join_asof(symbol, timestamps, direction)?)
    }
}
