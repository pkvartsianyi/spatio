use clap::Parser;
use spatio::Spatio;
use spatio_server::run_server;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long)]
    data_dir: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "spatio_server=info,spatio=info,info".into()),
        )
        .init();

    let args = Args::parse();

    let db = if let Some(path) = args.data_dir {
        info!("Opening database at {}", path);
        Spatio::builder().path(path).build()?
    } else {
        info!("Opening in-memory database");
        Spatio::builder().build()?
    };

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c signal");
    };

    run_server(addr, Arc::new(db), Box::pin(shutdown)).await?;

    Ok(())
}
