//! tarpc transport for the Spatio server.

use futures::prelude::*;
use spatio::Spatio;

use std::sync::Arc;
use std::time::Duration;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::handler::Handler;
use crate::protocol::SpatioService;

use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum accepted frame size (bytes). Bounds per-request allocation from
/// untrusted clients.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Maximum concurrently accepted client connections.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum in-flight requests handled concurrently on a single connection.
const MAX_REQUESTS_PER_CONNECTION: usize = 256;

/// Run the tarpc RPC server until `shutdown` resolves.
pub async fn run_server(
    listener: tokio::net::TcpListener,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let (write_tx, writer_handle) = crate::writer::spawn_background_writer(db.clone(), 10_000);

    let handler = Handler::new(db, write_tx);
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
                            let codec = LengthDelimitedCodec::builder()
                                .max_frame_length(MAX_FRAME_BYTES)
                                .new_codec();
                            let framed = Framed::new(socket, codec);
                            let transport = tarpc::serde_transport::new(framed, Json::default());

                            server::BaseChannel::with_defaults(transport)
                                .execute(server.serve())
                                // Bound concurrent in-flight requests per connection
                                // rather than spawning an unbounded task per response.
                                .for_each_concurrent(MAX_REQUESTS_PER_CONNECTION, |response| async move {
                                    response.await;
                                })
                                .await;
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

    // Abort in-flight connections, then close the writer's channel and wait for
    // it to drain its queue so durability is preserved on shutdown.
    conns.shutdown().await;
    drop(handler);
    if let Err(e) = tokio::task::spawn_blocking(move || writer_handle.join()).await {
        error!("Failed to join background writer: {e}");
    }

    Ok(())
}
