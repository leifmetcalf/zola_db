use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use zola_db_proto::{Request, Response};

pub use zola_db_proto::{Direction, Market};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("server error: {0}")]
    Server(String),

    #[error(transparent)]
    Proto(#[from] zola_db_proto::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct Client {
    addr: String,
}

impl Client {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    async fn request(&self, req: &Request) -> Result<Response, Error> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;
        zola_db_proto::write_request(&mut stream, req).await?;
        stream.shutdown().await?;
        let resp = zola_db_proto::read_response(&mut stream).await?;
        match resp {
            Response::Error(msg) => Err(Error::Server(msg)),
            other => Ok(other),
        }
    }

    pub async fn join_asof(
        &self,
        table: &str,
        symbol: &str,
        timestamps: &RecordBatch,
        direction: Direction,
    ) -> Result<RecordBatch, Error> {
        let req = Request::JoinAsof {
            table: table.to_string(),
            symbol: symbol.to_string(),
            direction,
            timestamps: timestamps.clone(),
        };
        match self.request(&req).await? {
            Response::JoinAsof(batch) => Ok(batch),
            _ => unreachable!(),
        }
    }

    pub async fn ingest_binance(
        &self,
        market: Market,
        day: jiff::civil::Date,
    ) -> Result<(), Error> {
        let req = Request::IngestBinance { market, day };
        match self.request(&req).await? {
            Response::IngestBinance => Ok(()),
            _ => unreachable!(),
        }
    }
}
