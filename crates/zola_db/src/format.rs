use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};

pub const COLUMN_MAGIC: u32 = 0x5A4F_4C41; // "ZOLA"
pub const SIDECAR_MAGIC: u32 = 0x5A53_4944; // "ZSID"
pub const VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct ColumnHeader {
    pub magic: u32,
    pub version: u32,
    pub col_type: u32,
    pub _pad: u32,
    pub row_count: u64,
}

pub const HEADER_SIZE: usize = std::mem::size_of::<ColumnHeader>();

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct PartedEntry {
    pub symbol_id: u64,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct SidecarHeader {
    pub magic: u32,
    pub num_symbols: u32,
    pub num_value_cols: u32,
    pub _pad: u32,
}

pub const SIDECAR_HEADER_SIZE: usize = std::mem::size_of::<SidecarHeader>();

/// Size of one sidecar entry in the file: sym_id(8) + timestamp(8) + values(num_value_cols * 8)
pub fn sidecar_entry_size(num_value_cols: u32) -> usize {
    (2 + num_value_cols as usize) * 8
}
