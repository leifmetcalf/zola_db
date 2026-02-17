pub const NULL_I64: i64 = i64::MIN;
pub const NULL_F64: f64 = f64::NAN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ColumnType {
    I64 = 1,
    F64 = 2,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub value_columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Backward,
    Forward,
}

pub struct Probes<'a> {
    pub symbols: &'a [&'a str],
    pub timestamps: &'a [i64],
}

pub enum ColumnSlice<'a> {
    I64(&'a [i64]),
    F64(&'a [f64]),
}

#[derive(Debug)]
pub enum ColumnVec {
    I64(Vec<i64>),
    F64(Vec<f64>),
}
