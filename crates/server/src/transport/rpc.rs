//! tarpc transport for Spatio server
//!
//! This is the default high-performance RPC transport.

use futures::prelude::*;
use spatio::Spatio;
use std::net::SocketAddr;
use std::sync::Arc;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tracing::{error, info};

use crate::handler::Handler;
use crate::protocol::SpatioService;

/// Run the tarpc RPC server
pub async fn run_server(
    addr: SocketAddr,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let handler = Handler::new(db);

    let mut listener = tarpc::serde_transport::tcp::listen(&addr, Json::default).await?;
    info!("Spatio RPC Server listening on {}", listener.local_addr());

    loop {
        tokio::select! {
            next = listener.next() => {
                match next {
                    Some(Ok(transport)) => {
                        let server = handler.clone();
                        tokio::spawn(async move {
                            server::BaseChannel::with_defaults(transport)
                                .execute(server.serve())
                                .for_each(|response| async move {
                                    tokio::spawn(response);
                                })
                                .await;
                        });
                    }
                    Some(Err(e)) => {
                        error!("Accept error: {}", e);
                    }
                    None => break,
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
