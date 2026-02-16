pub mod client;
pub mod server;

pub use client::Client;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;
    use zola_db::*;

    const JAN15: i64 = 1_705_276_800 * 1_000_000;

    fn two_col_schema() -> Schema {
        Schema {
            value_columns: vec![
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnType::F64,
                },
                ColumnDef {
                    name: "size".into(),
                    col_type: ColumnType::I64,
                },
            ],
        }
    }

    fn start_server(dir: &std::path::Path) -> (std::net::SocketAddr, Arc<RwLock<Db>>) {
        let db = Db::open(dir).unwrap();
        let db = Arc::new(RwLock::new(db));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let db_clone = Arc::clone(&db);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let db = Arc::clone(&db_clone);
                std::thread::spawn(move || {
                    server::handle_conn(stream, db);
                });
            }
        });
        (addr, db)
    }

    #[test]
    fn test_write_and_asof_roundtrip() {
        let dir = TempDir::new().unwrap();
        let (addr, _db) = start_server(dir.path());
        let mut client = Client::connect(&addr.to_string()).unwrap();

        let schema = two_col_schema();
        let timestamps = vec![JAN15 + 36_000_000_000, JAN15 + 36_001_000_000];
        let symbols = vec![1_i64, 1];
        let prices = vec![100.5_f64, 101.0];
        let sizes = vec![500_i64, 600];

        client
            .write(
                "trades",
                &schema,
                &timestamps,
                &symbols,
                &[ColumnSlice::F64(&prices), ColumnSlice::I64(&sizes)],
            )
            .unwrap();

        // Backward asof at 10:00:00.5 → finds 10:00:00
        let result = client
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[JAN15 + 36_000_500_000],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], JAN15 + 36_000_000_000);
        match &result.columns[0] {
            ColumnVec::F64(v) => assert_eq!(v[0], 100.5),
            _ => panic!("expected F64"),
        }
        match &result.columns[1] {
            ColumnVec::I64(v) => assert_eq!(v[0], 500),
            _ => panic!("expected I64"),
        }
    }

    #[test]
    fn test_forward_asof_via_network() {
        let dir = TempDir::new().unwrap();
        let (addr, _db) = start_server(dir.path());
        let mut client = Client::connect(&addr.to_string()).unwrap();

        let schema = Schema {
            value_columns: vec![ColumnDef {
                name: "price".into(),
                col_type: ColumnType::F64,
            }],
        };
        let timestamps = vec![JAN15 + 36_000_000_000, JAN15 + 36_001_000_000];
        let symbols = vec![1_i64, 1];
        let prices = vec![100.0_f64, 101.0];

        client
            .write(
                "trades",
                &schema,
                &timestamps,
                &symbols,
                &[ColumnSlice::F64(&prices)],
            )
            .unwrap();

        // Forward asof at 10:00:00.5 → finds 10:00:01
        let result = client
            .asof(
                "trades",
                &Probes {
                    symbols: &[1],
                    timestamps: &[JAN15 + 36_000_500_000],
                },
                Direction::Forward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], JAN15 + 36_001_000_000);
        match &result.columns[0] {
            ColumnVec::F64(v) => assert_eq!(v[0], 101.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn test_null_result_for_missing_symbol() {
        let dir = TempDir::new().unwrap();
        let (addr, _db) = start_server(dir.path());
        let mut client = Client::connect(&addr.to_string()).unwrap();

        let schema = Schema {
            value_columns: vec![ColumnDef {
                name: "price".into(),
                col_type: ColumnType::F64,
            }],
        };
        client
            .write(
                "trades",
                &schema,
                &[JAN15 + 36_000_000_000],
                &[1_i64],
                &[ColumnSlice::F64(&[100.0])],
            )
            .unwrap();

        // Query symbol 99 which doesn't exist
        let result = client
            .asof(
                "trades",
                &Probes {
                    symbols: &[99],
                    timestamps: &[JAN15 + 36_000_000_000],
                },
                Direction::Backward,
            )
            .unwrap();

        assert_eq!(result.timestamps[0], NULL_I64);
        match &result.columns[0] {
            ColumnVec::F64(v) => assert!(v[0].is_nan()),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn test_error_on_missing_table() {
        let dir = TempDir::new().unwrap();
        let (addr, _db) = start_server(dir.path());
        let mut client = Client::connect(&addr.to_string()).unwrap();

        let result = client.asof(
            "nonexistent",
            &Probes {
                symbols: &[1],
                timestamps: &[JAN15],
            },
            Direction::Backward,
        );

        assert!(result.is_err());
    }
}
