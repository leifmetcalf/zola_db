use std::io::{Read, Write};
use std::net::TcpStream;

use zerocopy::{FromBytes, IntoBytes};

use zola_db::{AsofResult, ColumnSlice, ColumnType, ColumnVec, Direction, Probes, Schema};

use zola_db_proto::*;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Client { stream })
    }

    pub fn write(
        &mut self,
        table: &str,
        schema: &Schema,
        timestamps: &[i64],
        symbols: &[i64],
        columns: &[ColumnSlice<'_>],
    ) -> Result<(), String> {
        let body = build_write_body(table, schema, timestamps, symbols, columns);
        self.send(MSG_WRITE, &body)?;
        self.expect_ok()
    }

    pub fn asof(
        &mut self,
        table: &str,
        probes: &Probes<'_>,
        direction: Direction,
    ) -> Result<AsofResult, String> {
        let body = build_asof_body(table, probes, direction);
        self.send(MSG_ASOF, &body)?;
        self.recv_result()
    }

    fn send(&mut self, msg_type: u32, body: &[u8]) -> Result<(), String> {
        let header = WireHeader {
            magic: WIRE_MAGIC,
            version: WIRE_VERSION,
            msg_type,
            _pad: 0,
            body_len: body.len() as u64,
        };
        self.stream
            .write_all(header.as_bytes())
            .map_err(|e| format!("send error: {e}"))?;
        self.stream
            .write_all(body)
            .map_err(|e| format!("send error: {e}"))?;
        Ok(())
    }

    fn recv_header(&mut self) -> Result<(WireHeader, Vec<u8>), String> {
        let mut header_buf = [0u8; WIRE_HEADER_SIZE];
        self.stream
            .read_exact(&mut header_buf)
            .map_err(|e| format!("recv error: {e}"))?;
        let header = *WireHeader::ref_from_bytes(&header_buf)
            .map_err(|e| format!("bad header: {e}"))?;

        if header.magic != WIRE_MAGIC {
            return Err("bad magic in response".into());
        }
        if header.body_len > MAX_BODY_LEN {
            return Err(format!("body too large: {}", header.body_len));
        }

        let body_len = header.body_len as usize;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            self.stream
                .read_exact(&mut body)
                .map_err(|e| format!("recv error: {e}"))?;
        }
        Ok((header, body))
    }

    fn expect_ok(&mut self) -> Result<(), String> {
        let (header, body) = self.recv_header()?;
        match header.msg_type {
            MSG_OK => Ok(()),
            MSG_ERROR => {
                let msg = String::from_utf8_lossy(&body);
                Err(msg.into_owned())
            }
            other => Err(format!("unexpected response type: {other}")),
        }
    }

    fn recv_result(&mut self) -> Result<AsofResult, String> {
        let (header, body) = self.recv_header()?;
        match header.msg_type {
            MSG_RESULT => parse_asof_result(&body),
            MSG_ERROR => {
                let msg = String::from_utf8_lossy(&body);
                Err(msg.into_owned())
            }
            other => Err(format!("unexpected response type: {other}")),
        }
    }
}

fn build_write_body(
    table: &str,
    schema: &Schema,
    timestamps: &[i64],
    symbols: &[i64],
    columns: &[ColumnSlice<'_>],
) -> Vec<u8> {
    let mut body = Vec::new();

    write_section(&mut body, table.as_bytes());

    let mut schema_text = String::new();
    for col in &schema.value_columns {
        let type_str = match col.col_type {
            ColumnType::I64 => "i64",
            ColumnType::F64 => "f64",
        };
        schema_text.push_str(&col.name);
        schema_text.push(':');
        schema_text.push_str(type_str);
        schema_text.push('\n');
    }
    write_section(&mut body, schema_text.as_bytes());

    let row_count = timestamps.len();
    let col_count = columns.len();
    body.extend_from_slice(&(row_count as u64).to_ne_bytes());
    body.extend_from_slice(&(col_count as u64).to_ne_bytes());

    body.extend_from_slice(timestamps.as_bytes());
    body.extend_from_slice(symbols.as_bytes());

    // col_types + padding
    for col in columns {
        let ct: u32 = match col {
            ColumnSlice::I64(_) => ColumnType::I64 as u32,
            ColumnSlice::F64(_) => ColumnType::F64 as u32,
        };
        body.extend_from_slice(&ct.to_ne_bytes());
    }
    let pad = padded_len(col_count * 4) - col_count * 4;
    for _ in 0..pad {
        body.push(0);
    }

    // column data
    for col in columns {
        match col {
            ColumnSlice::I64(s) => body.extend_from_slice(s.as_bytes()),
            ColumnSlice::F64(s) => body.extend_from_slice(s.as_bytes()),
        }
    }

    body
}

fn build_asof_body(table: &str, probes: &Probes<'_>, direction: Direction) -> Vec<u8> {
    let mut body = Vec::new();

    write_section(&mut body, table.as_bytes());

    let dir_u32: u32 = match direction {
        Direction::Backward => 0,
        Direction::Forward => 1,
    };
    body.extend_from_slice(&dir_u32.to_ne_bytes());
    body.extend_from_slice(&0u32.to_ne_bytes()); // pad

    body.extend_from_slice(&(probes.symbols.len() as u64).to_ne_bytes());
    body.extend_from_slice(probes.symbols.as_bytes());
    body.extend_from_slice(probes.timestamps.as_bytes());

    body
}

fn parse_asof_result(body: &[u8]) -> Result<AsofResult, String> {
    let mut r = Reader::new(body);

    let probe_count = r.read_u64()? as usize;
    let col_count = r.read_u64()? as usize;

    let timestamps = r.read_i64_slice(probe_count)?.to_vec();

    // col_types + padding
    let col_types_bytes = r.read_bytes(col_count * 4)?;
    let col_types: Vec<u32> = col_types_bytes
        .chunks_exact(4)
        .map(|c| u32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    let pad = padded_len(col_count * 4) - col_count * 4;
    if pad > 0 {
        r.read_bytes(pad)?;
    }

    let mut columns = Vec::with_capacity(col_count);
    for &ct in &col_types {
        match ct {
            ct if ct == ColumnType::I64 as u32 => columns.push(ColumnVec::I64(r.read_i64_slice(probe_count)?.to_vec())),
            ct if ct == ColumnType::F64 as u32 => columns.push(ColumnVec::F64(r.read_f64_slice(probe_count)?.to_vec())),
            _ => return Err(format!("unknown column type: {ct}")),
        }
    }

    Ok(AsofResult {
        timestamps,
        columns,
    })
}
