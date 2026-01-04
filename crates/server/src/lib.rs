use futures::SinkExt;
use spatio::Spatio;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};

pub mod handler;

use crate::handler::Handler;
pub use spatio_protocol as protocol;
pub use spatio_protocol::{SBPClientCodec, SBPServerCodec};
use std::future::Future;
use tokio::time::{timeout, Duration};

const CONN_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct AppState {
    pub handler: Arc<Handler>,
}

pub async fn run_server(
    addr: SocketAddr,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        handler: Arc::new(crate::handler::Handler::new(db)),
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
