use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::info;

mod audit;
mod server;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name    = "nats-lens",
    version,
    about   = "NATS JetStream delivery guarantee monitor",
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// NATS server URL (nats://, tls://, or ws://)
    #[arg(long, global = true, default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// Path to NATS credentials file (.creds) from nsc
    #[arg(long, global = true)]
    creds: Option<String>,

    /// Authentication token
    #[arg(long, global = true)]
    token: Option<String>,

    /// Username for NATS authentication
    #[arg(long, global = true)]
    username: Option<String>,

    /// Password for NATS authentication (use with --username)
    #[arg(long, global = true)]
    password: Option<String>,

    /// Path to CA certificate (PEM) for server certificate verification
    #[arg(long, global = true)]
    tls_ca: Option<String>,

    /// Path to client TLS certificate (PEM) for mTLS
    #[arg(long, global = true)]
    tls_cert: Option<String>,

    /// Path to client TLS private key (PEM) for mTLS (required with --tls-cert)
    #[arg(long, global = true)]
    tls_key: Option<String>,

    /// HTTP port for the web dashboard and API
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Poll interval in seconds
    #[arg(long, default_value_t = 5)]
    interval: u64,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the live web dashboard (default when no subcommand given)
    Run {
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long, default_value_t = 5)]
        interval: u64,
    },
    /// One-shot configuration audit — scan all streams/consumers and report issues
    Init {
        #[arg(long, default_value = "text")]
        format: String,
        /// Exit with code 1 if any critical issues found (CI-friendly)
        #[arg(long, default_value_t = false)]
        fail_on_critical: bool,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nats_lens=info,nats_lens_core=info".parse().unwrap()),
        )
        .compact()
        .init();

    let cli = Cli::parse();

    // Extract all connection parameters before the command match so
    // they remain accessible regardless of which branch runs.
    let nats_url  = cli.nats.clone();
    let auth      = AuthConfig::from_cli(&cli);
    let port      = cli.port;
    let interval  = cli.interval;

    match cli.command {
        Some(Command::Init { format, fail_on_critical }) => {
            let nats = connect(&nats_url, &auth).await?;
            let had_critical = audit::run_audit(nats, &format).await?;
            if fail_on_critical && had_critical { std::process::exit(1); }
        }
        Some(Command::Run { port, interval }) => {
            run_dashboard(&nats_url, &auth, port, interval).await?;
        }
        None => {
            run_dashboard(&nats_url, &auth, port, interval).await?;
        }
    }

    Ok(())
}

// ── Auth configuration ────────────────────────────────────────────────────────

struct AuthConfig {
    creds:    Option<String>,
    token:    Option<String>,
    username: Option<String>,
    password: Option<String>,
    tls_ca:   Option<String>,
    tls_cert: Option<String>,
    tls_key:  Option<String>,
}

impl AuthConfig {
    fn from_cli(cli: &Cli) -> Self {
        Self {
            creds:    cli.creds.clone(),
            token:    cli.token.clone(),
            username: cli.username.clone(),
            password: cli.password.clone(),
            tls_ca:   cli.tls_ca.clone(),
            tls_cert: cli.tls_cert.clone(),
            tls_key:  cli.tls_key.clone(),
        }
    }
}

async fn connect(url: &str, auth: &AuthConfig) -> Result<async_nats::Client> {
    info!("Connecting to NATS at {url}");

    // Start with credentials file if provided — it takes precedence.
    let mut opts = if let Some(ref path) = auth.creds {
        async_nats::ConnectOptions::with_credentials_file(path)
            .await
            .with_context(|| format!("reading credentials file {path}"))?
    } else {
        async_nats::ConnectOptions::new()
    };

    if let Some(ref t) = auth.token {
        opts = opts.token(t.clone());
    }
    if let (Some(ref u), Some(ref p)) = (&auth.username, &auth.password) {
        opts = opts.user_and_password(u.clone(), p.clone());
    }
    // async-nats TLS methods take PathBuf directly — it handles reading internally.
    if let Some(ref ca) = auth.tls_ca {
        opts = opts.add_root_certificates(PathBuf::from(ca));
    }
    if let (Some(ref cert), Some(ref key)) = (&auth.tls_cert, &auth.tls_key) {
        opts = opts.add_client_certificate(PathBuf::from(cert), PathBuf::from(key));
    }

    opts.connect(url)
        .await
        .with_context(|| format!("connecting to NATS at {url}"))
}

// ── Dashboard runner ──────────────────────────────────────────────────────────

async fn run_dashboard(url: &str, auth: &AuthConfig, port: u16, interval: u64) -> Result<()> {
    let nats   = connect(url, auth).await?;
    let engine = Arc::new(nats_lens_core::engine::Engine::new(nats));

    let state       = engine.state();
    let tx          = engine.sender();
    let history     = engine.history_store();
    let nats_client = engine.nats_client_arc();

    let engine_bg = Arc::clone(&engine);
    tokio::spawn(async move { engine_bg.run(Duration::from_secs(interval)).await });

    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    info!("");
    info!("  ┌────────────────────────────────────────────────────────────┐");
    info!("  │  nats-lens dashboard  →  http://localhost:{port}           │");
    info!("  │  Prometheus metrics   →  http://localhost:{port}/metrics   │");
    info!("  │  Health events        →  nats.lens.health.violations.>     │");
    info!("  └────────────────────────────────────────────────────────────┘");
    info!("");

    server::serve(addr, state, tx, history, nats_client).await
}
