//! tarpc transport for the Spatio server.

use bytes::Bytes;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use spatio::Spatio;

use std::sync::Arc;
use std::time::Duration;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Semaphore;
use tokio_rustls::{TlsAcceptor, rustls};
use tracing::{error, info, warn};

use crate::handler::Handler;
use crate::protocol::SpatioService;

use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum frame size (bytes) in either direction. Bounds per-request
/// allocation from untrusted clients; clients must use the same limit.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Frame limit before the client has authenticated.
const MAX_HANDSHAKE_FRAME_BYTES: usize = 4 * 1024;
/// Server reply to an accepted handshake.
pub const HANDSHAKE_OK: &[u8] = b"OK";
/// Time a new connection has to finish the TLS and auth handshakes.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum concurrently accepted client connections.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum in-flight requests handled concurrently on a single connection.
const MAX_REQUESTS_PER_CONNECTION: usize = 64;
/// Maximum concurrent blocking DB calls across all connections.
const MAX_BLOCKING_TASKS: usize = 256;
/// Server-side cap on a single request, regardless of the client's deadline.
// ponytail: a timed-out DB call still runs to completion on the blocking pool
// (holding its permit); cancelling it needs deadline checks inside core scans.
const MAX_REQUEST_DURATION: Duration = Duration::from_secs(30);
/// Connections with no in-flight request and no traffic for this long are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long shutdown waits for in-flight DB calls before giving up on them.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Connection security settings.
#[derive(Clone, Default)]
pub struct ServerConfig {
    /// When set, a client's handshake must present exactly this token.
    pub auth_token: Option<String>,
    /// When set, connections are TLS-wrapped.
    pub tls: Option<Arc<rustls::ServerConfig>>,
}

/// Length-delimited framing shared by server and client.
pub fn framed<S: AsyncRead + AsyncWrite>(io: S) -> Framed<S, LengthDelimitedCodec> {
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec();
    Framed::new(io, codec)
}

/// Run the RPC server without auth or TLS until `shutdown` resolves.
pub async fn run_server(
    listener: tokio::net::TcpListener,
    db: Arc<Spatio>,
    shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    run_server_with(listener, db, ServerConfig::default(), shutdown).await
}

/// Run the RPC server until `shutdown` resolves.
pub async fn run_server_with(
    listener: tokio::net::TcpListener,
    db: Arc<Spatio>,
    config: ServerConfig,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let permits = Arc::new(Semaphore::new(MAX_BLOCKING_TASKS));
    let handler = Handler::new(db, permits.clone());
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let config = Arc::new(config);
    let mut conns = tokio::task::JoinSet::new();

    info!("Spatio RPC Server listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, peer)) => {
                        // Bound live connections; if at capacity, drop the freshly
                        // accepted socket rather than pile on.
                        let Ok(permit) = connections.clone().try_acquire_owned() else {
                            error!("Connection limit ({MAX_CONNECTIONS}) reached, rejecting connection");
                            drop(socket);
                            continue;
                        };

                        let server = handler.clone();
                        let config = config.clone();
                        conns.spawn(async move {
                            let _permit = permit; // held for the connection's lifetime
                            let token = config.auth_token.as_deref();
                            match &config.tls {
                                Some(tls) => {
                                    let accept = TlsAcceptor::from(tls.clone()).accept(socket);
                                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, accept).await {
                                        Ok(Ok(stream)) => serve_connection(stream, server, token).await,
                                        _ => warn!("TLS handshake with {peer} failed"),
                                    }
                                }
                                None => serve_connection(socket, server, token).await,
                            }
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

    // Abort in-flight connections, then give already-started DB calls a bounded
    // grace period so a slow call can't keep the caller from closing the DB.
    conns.shutdown().await;
    let drain = permits.acquire_many(MAX_BLOCKING_TASKS as u32);
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        warn!("DB calls still running after {SHUTDOWN_GRACE:?}; shutting down anyway");
    }

    Ok(())
}

fn token_matches(presented: &[u8], expected: &[u8]) -> bool {
    presented.len() == expected.len()
        && presented
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// The client's first frame is its auth token (empty if none); the server
/// answers `HANDSHAKE_OK` or closes the connection.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    token: Option<&str>,
) -> bool {
    framed
        .codec_mut()
        .set_max_frame_length(MAX_HANDSHAKE_FRAME_BYTES);
    let accepted = async {
        let presented = framed.next().await?.ok()?;
        if let Some(t) = token
            && !token_matches(&presented, t.as_bytes())
        {
            return None;
        }
        framed.send(Bytes::from_static(HANDSHAKE_OK)).await.ok()
    };
    let ok = matches!(
        tokio::time::timeout(HANDSHAKE_TIMEOUT, accepted).await,
        Ok(Some(()))
    );
    framed.codec_mut().set_max_frame_length(MAX_FRAME_BYTES);
    ok
}

async fn serve_connection<S>(io: S, server: Handler, token: Option<&str>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = framed(io);
    if !handshake(&mut framed, token).await {
        warn!("Rejected connection: handshake failed");
        return;
    }
    let transport = tarpc::serde_transport::new(framed, Json::default());
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
