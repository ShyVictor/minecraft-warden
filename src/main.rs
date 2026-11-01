
use async_http_proxy::http_connect_tokio_with_basic_auth;
use clap::Parser;
use std::{
    error::Error,
    net::{SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream, lookup_host},
    sync::Semaphore,
    task::JoinSet,
    time::timeout,
};
use tracing::{error, info, warn};

type AnyError = Box<dyn Error + Send + Sync>;

const PROXY_HOST: &str = "";
const PROXY_PORT: u16 = ;
const PROXY_USER: &str = "";
const PROXY_PASSWORD: &str = "";

const MAX_CONNECTIONS: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser, Debug)]
#[command(
    name = "warden",
    version,
    about = "A lightweight TCP tunneling tool for Minecraft Java Edition through authenticated HTTP CONNECT and SOCKS5 proxies"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:25565")]
    listen: SocketAddr,
    #[arg(long)]
    target: String,
}

async fn resolve_target(target: &str) -> Result<SocketAddr, AnyError> {
    let mut addresses = lookup_host(target).await?;
    addresses
        .next()
        .ok_or_else(|| format!("nenhum endereço encontrado para {target}").into())
}

async fn connect_via_proxy(
    target: SocketAddr,
) -> Result<TcpStream, AnyError> {
    let proxy_authority = format!("{PROXY_HOST}:{PROXY_PORT}");

    let mut proxy_addresses = timeout(
        CONNECT_TIMEOUT,
        lookup_host(&proxy_authority),
    )
    .await
    .map_err(|_| "proxy timeout")??;

    let proxy_addr = proxy_addresses
        .next()
        .ok_or("proxy 404")?;

    let mut stream = timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect(proxy_addr),
    )
    .await
    .map_err(|_| "proxy timeout")??;

    stream.set_nodelay(true)?;

    timeout(
        CONNECT_TIMEOUT,
        http_connect_tokio_with_basic_auth(
            &mut stream,
            &target.ip().to_string(),
            target.port(),
            PROXY_USER,
            PROXY_PASSWORD,
        ),
    )
    .await
    .map_err(|_| "HTTP CONNECT timeout")??;

    Ok(stream)
}

async fn handle_client(
    mut client: TcpStream,
    peer: SocketAddr,
    target: SocketAddr,
) -> Result<(), AnyError> {
    client.set_nodelay(true)?;

    let mut upstream = connect_via_proxy(target).await?;
    upstream.set_nodelay(true)?;

    info!(%peer, %target, "túnel estabelecido");

    let (to_upstream, to_client) =
        copy_bidirectional(&mut client, &mut upstream).await?;

    info!(
        %peer,
        bytes_to_server = to_upstream,
        bytes_to_client = to_client,
        "conexão encerrada"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new("info")
                }),
        )
        .init();

    let args = Args::parse();

    if MAX_CONNECTIONS == 0 {
        return Err("MAX_CONNECTIONS < 0".into());
    }

    let target = resolve_target(&args.target).await?;

    let listener = TcpListener::bind(args.listen).await?;
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut tasks = JoinSet::new();

    info!(
        listen = %args.listen,
        target = %target,
        proxy = %format!("{PROXY_HOST}:{PROXY_PORT}"),
        max_connections = MAX_CONNECTIONS,
        "warden started"
    );

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (client, peer) = match result {
                    Ok(value) => value,
                    Err(err) => {
                        error!(error = %err, "accept connection error");
                        continue;
                    }
                };

                while let Some(result) = tasks.try_join_next() {
                    if let Err(err) = result {
                        warn!(error = %err, "error");
                    }
                }

                let permit = match Arc::clone(&slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(%peer, "max connections");
                        drop(client);
                        continue;
                    }
                };

                tasks.spawn(async move {
                    let _permit = permit;

                    if let Err(err) = handle_client(client, peer, target).await {
                        warn!(%peer, error = %err, "error");
                    }
                });
            }

            result = &mut shutdown => {
                if let Err(err) = result {
                    warn!(error = %err, "shutdown error");
                }
                break;
            }

            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(err) = result {
                    warn!(error = %err, "error");
                }
            }
        }
    }

    tasks.abort_all();

    while tasks.join_next().await.is_some() {}

    Ok(())
}
