use std::{
  future::Future,
  sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, TimestampedFragmentCollector, WebSocket};
use http_body_util::Empty;
use hyper::{
  header::{CONNECTION, UPGRADE},
  upgrade::Upgraded,
  Request,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{
  rustls::{client::WebPkiServerVerifier, ClientConfig},
  TlsConnector,
};

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
  Fut: Future + Send + 'static,
  Fut::Output: Send + 'static,
{
  fn execute(&self, fut: Fut) {
    tokio::task::spawn(fut);
  }
}

fn tls_connector() -> Result<TlsConnector> {
  let root_store = tokio_rustls::rustls::RootCertStore {
    roots: webpki_roots::TLS_SERVER_ROOTS.into(),
  };

  let config = ClientConfig::builder()
    .with_webpki_verifier(
      WebPkiServerVerifier::builder(root_store.into()).build()?,
    )
    .with_no_client_auth();

  Ok(TlsConnector::from(Arc::new(config)))
}

fn domain() -> rustls_pki_types::ServerName<'static> {
  static DOMAIN: OnceLock<rustls_pki_types::ServerName> = OnceLock::new();
  DOMAIN
    .get_or_init(|| {
      rustls_pki_types::ServerName::try_from("data-stream.binance.com")
        .context("invalid dnsname")
        .unwrap()
    })
    .clone()
}

async fn connect() -> Result<WebSocket<TokioIo<Upgraded>>> {
  let addr = "data-stream.binance.com:9443";
  let tcp_stream = TcpStream::connect(addr).await?;
  let tls_connector = tls_connector()?;
  let tls_stream = tls_connector.connect(domain(), tcp_stream).await?;

  let req = Request::builder()
    .method("GET")
    .uri(format!("wss://{}/ws/btcusdt@bookTicker", addr)) //stream we want to subscribe to
    .header("Host", addr)
    .header(UPGRADE, "websocket")
    .header(CONNECTION, "upgrade")
    .header(
      "Sec-WebSocket-Key",
      fastwebsockets::handshake::generate_key(),
    )
    .header("Sec-WebSocket-Version", "13")
    .body(Empty::<Bytes>::new())?;

  let (ws, _) =
    fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await?;

  Ok(ws)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
  let mut ws = TimestampedFragmentCollector::new(connect().await?);

  loop {
    let (ts, msg) = match ws.read_frame().await {
      Ok(msg) => msg,
      Err(e) => {
        println!("Error: {}", e);
        ws.write_frame(Frame::close_raw(vec![].into())).await?;
        break;
      }
    };

    match msg.opcode {
      OpCode::Text | OpCode::Binary => {
        let payload =
          String::from_utf8(msg.payload.to_vec()).expect("Invalid UTF-8 data");
        // Normally deserialise from json here, print just to show it works
        println!("{ts}: {:?}", payload);
      }
      OpCode::Continuation => {}
      OpCode::Close => {
        break;
      }
      _ => {}
    }
  }
  Ok(())
}
