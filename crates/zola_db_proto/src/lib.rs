use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};

pub const WIRE_MAGIC: u32 = 0x5A4E_4554; // "ZNET"
pub const WIRE_VERSION: u32 = 1;

pub const MSG_WRITE: u32 = 1;
pub const MSG_ASOF: u32 = 2;
pub const MSG_OK: u32 = 3;
pub const MSG_RESULT: u32 = 4;
pub const MSG_ERROR: u32 = 5;

pub const MAX_BODY_LEN: u64 = 4 * 1024 * 1024 * 1024; // 4 GB

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct WireHeader {
    pub magic: u32,
    pub version: u32,
    pub msg_type: u32,
    pub _pad: u32,
    pub body_len: u64,
}

pub const WIRE_HEADER_SIZE: usize = std::mem::size_of::<WireHeader>();

/// Round up to next 8-byte boundary.
pub fn padded_len(len: usize) -> usize {
    (len + 7) & !7
}

/// Write a length-prefixed section with padding to 8-byte alignment.
pub fn write_section(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u64).to_ne_bytes());
    buf.extend_from_slice(data);
    let pad = padded_len(data.len()) - data.len();
    for _ in 0..pad {
        buf.push(0);
    }
}

/// Cursor for reading fields from a byte buffer.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.pos + n > self.buf.len() {
            return Err("unexpected end of message".into());
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn read_u64(&mut self) -> Result<u64, String> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_ne_bytes(bytes.try_into().unwrap()))
    }

    pub fn read_u32(&mut self) -> Result<u32, String> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_ne_bytes(bytes.try_into().unwrap()))
    }

    /// Read a length-prefixed section and skip padding.
    pub fn read_section(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_u64()? as usize;
        let data = self.read_bytes(len)?;
        let pad = padded_len(len) - len;
        if pad > 0 {
            self.read_bytes(pad)?;
        }
        Ok(data)
    }

    pub fn read_i64_slice(&mut self, count: usize) -> Result<&'a [i64], String> {
        let bytes = self.read_bytes(count * 8)?;
        <[i64]>::ref_from_bytes(bytes).map_err(|e| format!("alignment error: {e}"))
    }

    pub fn read_f64_slice(&mut self, count: usize) -> Result<&'a [f64], String> {
        let bytes = self.read_bytes(count * 8)?;
        <[f64]>::ref_from_bytes(bytes).map_err(|e| format!("alignment error: {e}"))
    }

    pub fn read_str_array(&mut self) -> Result<Vec<&'a str>, String> {
        let count = self.read_u64()? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let len = self.read_u64()? as usize;
            let bytes = self.read_bytes(len)?;
            let s = std::str::from_utf8(bytes).map_err(|e| format!("bad utf8: {e}"))?;
            out.push(s);
        }
        // Align to 8 bytes
        let pad = padded_len(self.pos) - self.pos;
        if pad > 0 && pad < 8 {
            self.read_bytes(pad)?;
        }
        Ok(out)
    }
}

pub fn write_str_array(buf: &mut Vec<u8>, strings: &[&str]) {
    buf.extend_from_slice(&(strings.len() as u64).to_ne_bytes());
    for s in strings {
        buf.extend_from_slice(&(s.len() as u64).to_ne_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
    let pad = padded_len(buf.len()) - buf.len();
    for _ in 0..pad {
        buf.push(0);
    }
}
