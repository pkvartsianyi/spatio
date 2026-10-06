//! tarpc transport for the Spatio server.

use futures::prelude::*;
use futures::stream::FuturesUnordered;
use spatio::Spatio;

use std::sync::Arc;
use std::time::Duration;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::handler::Handler;
use crate::protocol::SpatioService;

use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum frame size (bytes) in either direction. Bounds per-request
/// allocation from untrusted clients; clients must use the same limit.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Maximum concurrently accepted client connections.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum in-flight requests handled concurrently on a single connection.
const MAX_REQUESTS_PER_CONNECTION: usize = 256;
/// Maximum concurrent blocking DB calls across all connections.
const MAX_BLOCKING_TASKS: usize = 256;
/// Server-side cap on a single request, regardless of the client's deadline.
const MAX_REQUEST_DURATION: Duration = Duration::from_secs(30);
/// Connections with no in-flight request and no traffic for this long are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Run the tarpc RPC server until `shutdown` resolves.
pub async fn run_server(
    listener: tokio::net::TcpListener,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let permits = Arc::new(Semaphore::new(MAX_BLOCKING_TASKS));
    let handler = Handler::new(db, permits.clone());
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut conns = tokio::task::JoinSet::new();

    info!("Spatio RPC Server listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, _)) => {
                        // Bound live connections; if at capacity, drop the freshly
                        // accepted socket rather than pile on.
                        let Ok(permit) = connections.clone().try_acquire_owned() else {
                            error!("Connection limit ({MAX_CONNECTIONS}) reached, rejecting connection");
                            drop(socket);
                            continue;
                        };

                        let server = handler.clone();
                        conns.spawn(async move {
                            let _permit = permit; // held for the connection's lifetime
                            serve_connection(socket, server).await;
                        });
                    }
                    Err(e) => {
                        // Back off briefly so a persistent accept error (e.g. fd
                        // exhaustion) doesn't spin the loop at 100% CPU.
                        error!("Accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            // Reap finished connection tasks so the JoinSet doesn't grow unbounded.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping server...");
                break;
            }
        }
    }

    // Abort in-flight connections, then wait for already-started DB calls to finish.
    conns.shutdown().await;
    let _ = permits.acquire_many(MAX_BLOCKING_TASKS as u32).await;

    Ok(())
}

async fn serve_connection(socket: TcpStream, server: Handler) {
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec();
    let transport = tarpc::serde_transport::new(Framed::new(socket, codec), Json::default());
    let requests = server::BaseChannel::with_defaults(transport).execute(server.serve());
    let mut requests = std::pin::pin!(requests);
    let mut in_flight = FuturesUnordered::new();

    loop {
        tokio::select! {
            next = requests.next(), if in_flight.len() < MAX_REQUESTS_PER_CONNECTION => match next {
                Some(response) => in_flight.push(tokio::time::timeout(MAX_REQUEST_DURATION, response)),
                None => break,
            },
            Some(_) = in_flight.next() => {}
            _ = tokio::time::sleep(IDLE_TIMEOUT), if in_flight.is_empty() => break,
        }
    }
}
