mod binance;

use std::sync::{Arc, RwLock};

use reqwest::Client;
use tokio::net::TcpListener;
use zola_db::Db;
use zola_db_proto::{Request, Response};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("usage: {} <db-path> [bind-addr]", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    let bind = args.get(2).map_or("127.0.0.1:9867", |s| s.as_str());

    let db = Db::open(db_path).expect("failed to open database");
    let db = Arc::new(RwLock::new(db));
    let client = Client::new();

    let listener = TcpListener::bind(bind).await.expect("failed to bind");
    eprintln!("listening on {bind}");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };
        let db = Arc::clone(&db);
        let client = client.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, db, client).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}

/// Handles a single request-response exchange on `stream`.
///
/// A panic inside `spawn_blocking` poisons the `RwLock`, which is intentional:
/// subsequent requests will fail rather than operate on potentially corrupt state.
async fn handle(
    mut stream: tokio::net::TcpStream,
    db: Arc<RwLock<Db>>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;

    let request = zola_db_proto::read_request(&mut stream).await?;

    match request {
        Request::JoinAsof {
            table,
            symbol,
            direction,
            timestamps,
        } => {
            let batch = tokio::task::spawn_blocking(move || {
                let db = db.read().unwrap();
                db.join_asof(&table, &symbol, &timestamps, direction)
            })
            .await??;

            zola_db_proto::write_response(&mut stream, &Response::JoinAsof(batch)).await?;
        }
        Request::IngestBinance { market, day } => {
            let symbols = binance::list_symbols(&client, market).await?;
            let fetch_result = binance::fetch(&client, market, &symbols, day).await;
            let response = tokio::task::spawn_blocking(move || {
                let epoch_day = zola_db::EpochDay::from(day);
                match fetch_result {
                    Ok(Some(batch)) => {
                        let table = binance::table_name(market);
                        let mut db = db.write().unwrap();
                        match db.ingest(table, epoch_day, batch) {
                            Ok(()) => Response::IngestBinance,
                            Err(e) => Response::Error(e.to_string()),
                        }
                    }
                    Ok(None) => Response::IngestBinance,
                    Err(e) => Response::Error(e.to_string()),
                }
            })
            .await?;

            zola_db_proto::write_response(&mut stream, &response).await?;
        }
    }

    Ok(())
}
