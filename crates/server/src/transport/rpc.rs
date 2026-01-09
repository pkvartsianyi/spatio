//! tarpc transport for Spatio server
//!
//! This is the default high-performance RPC transport.

use futures::prelude::*;
use spatio::Spatio;

use std::sync::Arc;
use tarpc::server::{self, Channel};
use tarpc::tokio_serde::formats::Json;
use tracing::{error, info};

use crate::handler::Handler;
use crate::protocol::SpatioService;

use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Run the tarpc RPC server
pub async fn run_server(
    listener: tokio::net::TcpListener,
    db: Arc<Spatio>,
    mut shutdown: impl Future<Output = ()> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let handler = Handler::new(db);

    info!("Spatio RPC Server listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, _)) => {
                        let server = handler.clone();
                        tokio::spawn(async move {
                            let framed = Framed::new(socket, LengthDelimitedCodec::new());
                            let transport = tarpc::serde_transport::new(
                                framed,
                                Json::default()
                            );

                            server::BaseChannel::with_defaults(transport)
                                .execute(server.serve())
                                .for_each(|response| async move {
                                    tokio::spawn(response);
                                })
                                .await;
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
