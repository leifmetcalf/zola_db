use std::net::TcpListener;
use std::sync::{Arc, RwLock};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <data_dir> <bind_addr>", args[0]);
        std::process::exit(1);
    }

    let db = zola_db::Db::open(&args[1]).expect("failed to open database");
    let db = Arc::new(RwLock::new(db));

    let listener = TcpListener::bind(&args[2]).expect("failed to bind");
    eprintln!("listening on {}", args[2]);

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            zola_db_server::server::handle_conn(stream, db);
        });
    }
}
