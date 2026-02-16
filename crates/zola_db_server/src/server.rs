use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use zerocopy::{FromBytes, IntoBytes};

use zola_db::{
    AsofResult, ColumnDef, ColumnSlice, ColumnType, ColumnVec, Db, Direction, Probes, Schema,
};

use zola_db_proto::*;

pub fn handle_conn(mut stream: TcpStream, db: Arc<RwLock<Db>>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .ok();

    let mut header_buf = [0u8; WIRE_HEADER_SIZE];
    loop {
        match stream.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(ref e)
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return;
            }
            Err(_) => return,
        }

        let header = match WireHeader::ref_from_bytes(&header_buf) {
            Ok(h) => h,
            Err(_) => {
                send_error(&mut stream, "invalid header");
                return;
            }
        };

        if header.magic != WIRE_MAGIC {
            send_error(&mut stream, "bad magic");
            return;
        }

        if header.body_len > MAX_BODY_LEN {
            send_error(&mut stream, "body too large");
            return;
        }

        let body_len = header.body_len as usize;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            if stream.read_exact(&mut body).is_err() {
                return;
            }
        }

        match header.msg_type {
            MSG_WRITE => match handle_write(&body, &db) {
                Ok(()) => send_ok(&mut stream),
                Err(e) => send_error(&mut stream, &e),
            },
            MSG_ASOF => match handle_asof(&body, &db) {
                Ok(result) => send_result(&mut stream, &result),
                Err(e) => send_error(&mut stream, &e),
            },
            _ => {
                send_error(&mut stream, "unknown message type");
            }
        }
    }
}

fn handle_write(body: &[u8], db: &Arc<RwLock<Db>>) -> Result<(), String> {
    let mut r = Reader::new(body);

    let table_bytes = r.read_section()?;
    let table = std::str::from_utf8(table_bytes).map_err(|e| format!("bad table name: {e}"))?;

    let schema_bytes = r.read_section()?;
    let schema_text =
        std::str::from_utf8(schema_bytes).map_err(|e| format!("bad schema: {e}"))?;
    let schema = parse_schema(schema_text)?;

    let row_count = r.read_u64()? as usize;
    let col_count = r.read_u64()? as usize;

    if col_count != schema.value_columns.len() {
        return Err("col_count mismatch with schema".into());
    }

    let timestamps = r.read_i64_slice(row_count)?;
    let symbols = r.read_i64_slice(row_count)?;

    // Read col_types + padding
    let col_types_bytes = r.read_bytes(col_count * 4)?;
    let col_types: Vec<u32> = col_types_bytes
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    let pad = padded_len(col_count * 4) - col_count * 4;
    if pad > 0 {
        r.read_bytes(pad)?;
    }

    for (i, &ct) in col_types.iter().enumerate() {
        if ct != schema.value_columns[i].col_type as u32 {
            return Err(format!("col_type mismatch at index {i}"));
        }
    }

    let mut columns: Vec<ColumnSlice<'_>> = Vec::with_capacity(col_count);
    for &ct in &col_types {
        match ct {
            1 => columns.push(ColumnSlice::I64(r.read_i64_slice(row_count)?)),
            2 => columns.push(ColumnSlice::F64(r.read_f64_slice(row_count)?)),
            _ => return Err(format!("unknown column type: {ct}")),
        }
    }

    let mut db = db.write().map_err(|e| format!("lock error: {e}"))?;
    db.write(table, &schema, timestamps, symbols, &columns)
        .map_err(|e| format!("{e}"))
}

fn handle_asof(body: &[u8], db: &Arc<RwLock<Db>>) -> Result<AsofResult, String> {
    let mut r = Reader::new(body);

    let table_bytes = r.read_section()?;
    let table = std::str::from_utf8(table_bytes).map_err(|e| format!("bad table name: {e}"))?;

    let dir_u32 = r.read_u32()?;
    let _pad = r.read_u32()?;

    let direction = match dir_u32 {
        0 => Direction::Backward,
        1 => Direction::Forward,
        _ => return Err(format!("unknown direction: {dir_u32}")),
    };

    let probe_count = r.read_u64()? as usize;
    let symbols = r.read_i64_slice(probe_count)?;
    let timestamps = r.read_i64_slice(probe_count)?;

    let db = db.read().map_err(|e| format!("lock error: {e}"))?;
    db.asof(table, &Probes { symbols, timestamps }, direction)
        .map_err(|e| format!("{e}"))
}

fn parse_schema(text: &str) -> Result<Schema, String> {
    let mut columns = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (name, type_str) = line
            .split_once(':')
            .ok_or_else(|| format!("bad schema line: {line}"))?;
        let col_type = match type_str {
            "i64" => ColumnType::I64,
            "f64" => ColumnType::F64,
            _ => return Err(format!("unknown type: {type_str}")),
        };
        columns.push(ColumnDef {
            name: name.to_string(),
            col_type,
        });
    }
    Ok(Schema {
        value_columns: columns,
    })
}

fn send_ok(stream: &mut TcpStream) {
    let header = WireHeader {
        magic: WIRE_MAGIC,
        version: WIRE_VERSION,
        msg_type: MSG_OK,
        _pad: 0,
        body_len: 0,
    };
    let _ = stream.write_all(header.as_bytes());
}

fn send_error(stream: &mut TcpStream, msg: &str) {
    let header = WireHeader {
        magic: WIRE_MAGIC,
        version: WIRE_VERSION,
        msg_type: MSG_ERROR,
        _pad: 0,
        body_len: msg.len() as u64,
    };
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(msg.as_bytes());
}

fn send_result(stream: &mut TcpStream, result: &AsofResult) {
    let probe_count = result.timestamps.len();
    let col_count = result.columns.len();

    let mut body = Vec::new();
    body.extend_from_slice(&(probe_count as u64).to_ne_bytes());
    body.extend_from_slice(&(col_count as u64).to_ne_bytes());
    body.extend_from_slice(result.timestamps.as_bytes());

    for col in &result.columns {
        let ct: u32 = match col {
            ColumnVec::I64(_) => ColumnType::I64 as u32,
            ColumnVec::F64(_) => ColumnType::F64 as u32,
        };
        body.extend_from_slice(&ct.to_ne_bytes());
    }
    let pad = padded_len(col_count * 4) - col_count * 4;
    for _ in 0..pad {
        body.push(0);
    }

    for col in &result.columns {
        match col {
            ColumnVec::I64(v) => body.extend_from_slice(v.as_bytes()),
            ColumnVec::F64(v) => body.extend_from_slice(v.as_bytes()),
        }
    }

    let header = WireHeader {
        magic: WIRE_MAGIC,
        version: WIRE_VERSION,
        msg_type: MSG_RESULT,
        _pad: 0,
        body_len: body.len() as u64,
    };
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(&body);
}
