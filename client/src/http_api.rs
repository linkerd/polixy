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
            let Spec {
                server_port,
                client_ip,
                tls,
            } = {
                let body = hyper::body::to_bytes(req.into_body()).await?;
                serde_json::from_slice(body.as_ref())?
            };

            match ports.get(&server_port) {
                Some(rx) => {
                    let inbound = rx.borrow();
                    let labels = match tls {
                        Some(TlsSpec { client_id }) => {
                            inbound.check_tls(client_ip, client_id.as_deref())
                        }
                        None => inbound.check_non_tls(client_ip),
                    };

                    let rsp = serde_json::json!({
                        "authorization": labels,
                    });
                    let bytes = serde_json::to_vec_pretty(&rsp).unwrap();

                    Ok(Response::builder()
                        .status(hyper::StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "text/plain")
                        .body(Bytes::copy_from_slice(bytes.as_slice()).into())
                        .unwrap())
                }

                None => {
                    let msg = format!(
                        "not in known ports: {:?}\n",
                        ports
                            .keys()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    );
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
