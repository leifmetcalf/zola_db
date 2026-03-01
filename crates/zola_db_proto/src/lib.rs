use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("deserialize: {0}")]
    Postcard(#[from] postcard::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

pub use zola_db_core::{Direction, Market};

pub enum Request {
    JoinAsof {
        table: String,
        symbol: String,
        direction: Direction,
        timestamps: RecordBatch,
    },
    IngestBinance {
        market: Market,
        day: jiff::civil::Date,
    },
}

pub enum Response {
    JoinAsof(RecordBatch),
    IngestBinance,
    Error(String),
}

#[derive(Serialize, Deserialize)]
enum RequestHeader {
    JoinAsof {
        table: String,
        symbol: String,
        direction: Direction,
    },
    IngestBinance {
        market: Market,
        day: jiff::civil::Date,
    },
}

#[derive(Serialize, Deserialize)]
enum ResponseHeader {
    JoinAsof,
    IngestBinance,
    Error(String),
}

async fn write_frame(w: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<(), Error> {
    w.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    w.write_all(bytes).await?;
    Ok(())
}

// No size cap on reads — the client is trusted (same-host only).
async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>, Error> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_postcard(w: &mut (impl AsyncWrite + Unpin), msg: &impl Serialize) -> Result<(), Error> {
    write_frame(w, &postcard::to_allocvec(msg)?).await
}

async fn read_postcard<T: serde::de::DeserializeOwned>(r: &mut (impl AsyncRead + Unpin)) -> Result<T, Error> {
    Ok(postcard::from_bytes(&read_frame(r).await?)?)
}

fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

fn ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch, Error> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batch = reader.next().ok_or_else(|| {
        arrow::error::ArrowError::IpcError("expected one record batch, got none".into())
    })??;
    if reader.next().is_some() {
        return Err(arrow::error::ArrowError::IpcError("expected exactly one record batch".into()).into());
    }
    Ok(batch)
}

async fn write_ipc(w: &mut (impl AsyncWrite + Unpin), batch: &RecordBatch) -> Result<(), Error> {
    write_frame(w, &batch_to_ipc(batch)?).await
}

async fn read_ipc(r: &mut (impl AsyncRead + Unpin)) -> Result<RecordBatch, Error> {
    ipc_to_batch(&read_frame(r).await?)
}

pub async fn write_request(w: &mut (impl AsyncWrite + Unpin), req: &Request) -> Result<(), Error> {
    match req {
        Request::JoinAsof { table, symbol, direction, timestamps } => {
            write_postcard(w, &RequestHeader::JoinAsof {
                table: table.clone(),
                symbol: symbol.clone(),
                direction: *direction,
            }).await?;
            write_ipc(w, timestamps).await?;
        }
        Request::IngestBinance { market, day } => {
            write_postcard(w, &RequestHeader::IngestBinance {
                market: *market,
                day: *day,
            }).await?;
        }
    }
    w.flush().await?;
    Ok(())
}

pub async fn read_request(r: &mut (impl AsyncRead + Unpin)) -> Result<Request, Error> {
    let header: RequestHeader = read_postcard(r).await?;
    match header {
        RequestHeader::JoinAsof { table, symbol, direction } => {
            let timestamps = read_ipc(r).await?;
            Ok(Request::JoinAsof { table, symbol, direction, timestamps })
        }
        RequestHeader::IngestBinance { market, day } => {
            Ok(Request::IngestBinance { market, day })
        }
    }
}

pub async fn write_response(w: &mut (impl AsyncWrite + Unpin), resp: &Response) -> Result<(), Error> {
    match resp {
        Response::JoinAsof(batch) => {
            write_postcard(w, &ResponseHeader::JoinAsof).await?;
            write_ipc(w, batch).await?;
        }
        Response::IngestBinance => {
            write_postcard(w, &ResponseHeader::IngestBinance).await?;
        }
        Response::Error(msg) => {
            write_postcard(w, &ResponseHeader::Error(msg.clone())).await?;
        }
    }
    w.flush().await?;
    Ok(())
}

pub async fn read_response(r: &mut (impl AsyncRead + Unpin)) -> Result<Response, Error> {
    let header: ResponseHeader = read_postcard(r).await?;
    match header {
        ResponseHeader::JoinAsof => {
            let batch = read_ipc(r).await?;
            Ok(Response::JoinAsof(batch))
        }
        ResponseHeader::IngestBinance => Ok(Response::IngestBinance),
        ResponseHeader::Error(msg) => Ok(Response::Error(msg)),
    }
}
