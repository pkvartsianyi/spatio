use bytes::{Buf, BufMut, BytesMut};
use futures::SinkExt;
use spatio::Spatio;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, error, info};

pub mod handler;
pub mod protocol;

use crate::handler::Handler;
use crate::protocol::{Command, ResponsePayload, ResponseStatus};
use std::future::Future;
use tokio::time::{timeout, Duration};

const MAX_FRAME_SIZE: usize = 10 * 1024 * 1024; // 10MB
const CONN_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct SBPServerCodec;

impl Decoder for SBPServerCodec {
    type Item = Command;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let mut buf = std::io::Cursor::new(&src[..]);
        let _tag = buf.get_u8();
        let len = buf.get_u32() as usize;

        if len > MAX_FRAME_SIZE {
            return Err(anyhow::anyhow!(
                "Frame size {} exceeds maximum {}",
                len,
                MAX_FRAME_SIZE
            ));
        }

        if src.len() < 5 + len {
            return Ok(None);
        }

        src.advance(5);
        let payload = src.split_to(len);
        let cmd: Command = bincode::deserialize(&payload)?;

        Ok(Some(cmd))
    }
}

impl Encoder<(ResponseStatus, ResponsePayload)> for SBPServerCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: (ResponseStatus, ResponsePayload),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (status, payload) = item;
        let serialized_payload = bincode::serialize(&payload)?;
        let len = serialized_payload.len() as u32;

        dst.reserve(5 + serialized_payload.len());
        dst.put_u8(status as u8);
        dst.put_u32(len);
        dst.put_slice(&serialized_payload);

        Ok(())
    }
}

pub struct SBPClientCodec;

impl Decoder for SBPClientCodec {
    type Item = (ResponseStatus, ResponsePayload);
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let mut buf = std::io::Cursor::new(&src[..]);
        let status_raw = buf.get_u8();
        let status = if status_raw == 0 {
            ResponseStatus::Ok
        } else {
            ResponseStatus::Error
        };
        let len = buf.get_u32() as usize;

        if len > MAX_FRAME_SIZE {
            return Err(anyhow::anyhow!(
                "Frame size {} exceeds maximum {}",
                len,
                MAX_FRAME_SIZE
            ));
        }

        if src.len() < 5 + len {
            return Ok(None);
        }

        src.advance(5);
        let payload = src.split_to(len);
        let response_payload: ResponsePayload = bincode::deserialize(&payload)?;

        Ok(Some((status, response_payload)))
    }
}

impl Encoder<Command> for SBPClientCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let serialized_payload = bincode::serialize(&item)?;
        let len = serialized_payload.len() as u32;

        dst.reserve(5 + serialized_payload.len());
        dst.put_u8(0x00); // Tag for Command - we can use the CommandType if we want more granular framing
        dst.put_u32(len);
        dst.put_slice(&serialized_payload);

        Ok(())
    }
}

pub struct AppState {
    pub handler: Arc<Handler>,
}

pub async fn run_server(
    addr: SocketAddr,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        handler: Arc::new(Handler::new(db)),
    });

    let listener = TcpListener::bind(addr).await?;
    info!("Spatio RPC Server listening on {}", addr);

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((socket, _)) => {
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(socket, state).await {
                                debug!("Connection closed: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping server...");
                break;
            }
        }
    }

    Ok(())
}

pub async fn handle_connection(socket: TcpStream, state: Arc<AppState>) -> anyhow::Result<()> {
    let mut framed = Framed::new(socket, SBPServerCodec);

    while let Ok(Some(request)) = timeout(IDLE_TIMEOUT, framed.next()).await {
        match request {
            Ok(cmd) => {
                debug!("Received command: {:?}", cmd);
                let response = state.handler.handle(cmd).await;
                timeout(CONN_TIMEOUT, framed.send(response)).await??;
            }
            Err(e) => {
                error!("Failed to decode frame: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}
