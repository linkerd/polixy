use crate::Inbound;
use anyhow::Result;
use bytes::Bytes;
use hyper::{Body, Request, Response};
use serde::Deserialize;
use std::{collections::HashMap, net::IpAddr};
use tokio::sync::watch;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    server_port: u16,
    #[serde(rename = "clientIP")]
    client_ip: IpAddr,
    tls: Option<TlsSpec>,
}

#[derive(Clone, Debug, Deserialize)]
struct TlsSpec {
    #[serde(rename = "clientID")]
    client_id: Option<String>,
}

pub async fn serve(
    ports: &HashMap<u16, watch::Receiver<Inbound>>,
    req: Request<Body>,
) -> Result<Response<Body>> {
    if req.method() != hyper::Method::POST {
        return Ok(Response::builder()
            .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::default())
            .unwrap());
    }

    match req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        Some("application/json") => {
            let body = hyper::body::to_bytes(req.into_body()).await?;
            let Spec {
                server_port,
                client_ip,
                tls,
            } = serde_json::from_slice::<Spec>(body.as_ref())?;
            match ports.get(&server_port) {
                Some(rx) => {
                    let allowed = match tls {
                        Some(TlsSpec { client_id }) => {
                            rx.borrow().check_tls(client_ip, client_id.as_deref())
                        }
                        None => rx.borrow().check_non_tls(client_ip),
                    };

                    let rsp = serde_json::json!({
                        "allowed": allowed,
                    });

                    Ok(Response::builder()
                        .status(hyper::StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "text/plain")
                        .body(Bytes::copy_from_slice(rsp.to_string().as_bytes()).into())
                        .unwrap())
                }

                None => {
                    let msg = format!("unknown port: {}\n", server_port);
                    Ok(Response::builder()
                        .status(hyper::StatusCode::NOT_FOUND)
                        .header(hyper::header::CONTENT_TYPE, "text/plain")
                        .body(Bytes::copy_from_slice(msg.as_bytes()).into())
                        .unwrap())
                }
            }
        }
        Some(ct) => {
            let msg = format!("unsupported content-type: {}", ct);
            Ok(Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .header(hyper::header::CONTENT_TYPE, "text/plain")
                .body(Bytes::copy_from_slice(msg.as_bytes()).into())
                .unwrap())
        }
        None => Ok(Response::builder()
            .status(hyper::StatusCode::BAD_REQUEST)
            .header(hyper::header::CONTENT_TYPE, "text/plain")
            .body(Bytes::from_static(b"content-type must be set\n").into())
            .unwrap()),
    }
}
