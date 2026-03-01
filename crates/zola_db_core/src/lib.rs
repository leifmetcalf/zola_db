use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Backward,
    Forward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Market {
    Spot,
    Perp,
}

const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_DAY: i64 = SECONDS_PER_DAY * 1_000_000;

pub const SYMBOL_COL: &str = "symbol";
pub const TIMESTAMP_COL: &str = "timestamp";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EpochDay(pub i32);

impl EpochDay {
    pub fn from_timestamp_us(us: i64) -> Self {
        Self(us.div_euclid(MICROS_PER_DAY) as i32)
    }
}

impl From<EpochDay> for jiff::civil::Date {
    fn from(day: EpochDay) -> Self {
        let ts = jiff::Timestamp::from_second(day.0 as i64 * SECONDS_PER_DAY).unwrap();
        jiff::tz::Offset::UTC.to_datetime(ts).date()
    }
}

impl From<jiff::civil::Date> for EpochDay {
    fn from(date: jiff::civil::Date) -> Self {
        let ts = date.to_zoned(jiff::tz::TimeZone::UTC).unwrap().timestamp();
        Self(ts.as_second().div_euclid(SECONDS_PER_DAY) as i32)
    }
}
