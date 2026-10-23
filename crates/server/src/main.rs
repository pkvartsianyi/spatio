use anyhow::{Context, bail};
use clap::Parser;
use spatio::Spatio;
use spatio_server::rustls::pki_types::pem::PemObject;
use spatio_server::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use spatio_server::{ServerConfig, run_server_with, rustls};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long)]
    data_dir: Option<String>,

    /// Token clients must present when connecting.
    #[arg(long, env = "SPATIO_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<String>,

    /// PEM certificate chain; enables TLS together with --tls-key.
    #[arg(long, env = "SPATIO_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<String>,

    /// PEM private key for --tls-cert.
    #[arg(long, env = "SPATIO_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<String>,

    /// Allow binding a non-loopback address without --auth-token.
    #[arg(long)]
    insecure: bool,
}

fn load_tls(cert: &str, key: &str) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let certs = CertificateDer::pem_file_iter(cert)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("reading TLS certificate {cert}"))?;
    let key =
        PrivateKeyDer::from_pem_file(key).with_context(|| format!("reading TLS key {key}"))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(Arc::new(config))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "spatio_server=info,spatio=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    if args.auth_token.is_none() && !addr.ip().is_loopback() {
        if !args.insecure {
            bail!(
                "refusing to listen on {addr} without --auth-token (pass --insecure to override)"
            );
        }
        warn!("Listening on {addr} without authentication");
    }
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(load_tls(cert, key)?),
        _ => None,
    };
    if args.auth_token.is_some() && tls.is_none() {
        warn!("Auth token is sent in plaintext; enable TLS with --tls-cert/--tls-key");
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let result = runtime.block_on(serve(args, addr, tls));
    // Don't wait on DB calls the server already gave up on.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn serve(
    args: Args,
    addr: SocketAddr,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> anyhow::Result<()> {
    let db = if let Some(path) = args.data_dir {
        info!("Opening database at {}", path);
        Spatio::builder().path(path).build()?
    } else {
        info!("Opening in-memory database");
        Spatio::builder().build()?
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    };

    let config = ServerConfig {
        auth_token: args.auth_token,
        tls,
    };
    let db = Arc::new(db);
    run_server_with(listener, db.clone(), config, Box::pin(shutdown)).await?;
    db.close()?;

    Ok(())
}
