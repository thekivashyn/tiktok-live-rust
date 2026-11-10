use crate::{
    engine::{
        Code, Connection, Failure, Source, Target, http_failure, room_number, websocket_error,
    },
    protocol,
};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, connect_async_with_config,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, protocol::WebSocketConfig},
};
use zeroize::Zeroizing;

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
const MAX_HTTP_BODY: usize = 1024 * 1024;

pub fn socket_url(room_id: &str) -> Result<url::Url, Failure> {
    room_number(room_id)?;
    let mut url =
        url::Url::parse("wss://webcast-ws.tiktok.com/webcast/im/ws_proxy/ws_reuse_supplement/")
            .map_err(|_| Failure::new(Code::InvalidInput, "socket_url", false))?;
    url.query_pairs_mut().extend_pairs([
        ("version_code", "180800"),
        ("device_platform", "web"),
        ("cookie_enabled", "true"),
        ("screen_width", "1920"),
        ("screen_height", "1080"),
        ("browser_language", "en-US"),
        ("browser_platform", "Win32"),
        ("browser_name", "Mozilla"),
        ("browser_version", &UA[8..]),
        ("browser_online", "true"),
        ("tz_name", "Asia/Saigon"),
        ("app_name", "tiktok_web"),
        ("sup_ws_ds_opt", "1"),
        ("update_version_code", "2.0.0"),
        ("compress", "gzip"),
        ("webcast_language", "en"),
        ("ws_direct", "1"),
        ("aid", "1988"),
        ("live_id", "12"),
        ("app_language", "en"),
        ("client_enter", "1"),
        ("room_id", room_id),
        ("identity", "audience"),
        ("history_comment_count", "6"),
        ("heartbeat_duration", "10000"),
        ("resp_content_type", "protobuf"),
        ("did_rule", "3"),
    ]);
    Ok(url)
}

pub struct DirectSource {
    client: reqwest::Client,
}
impl DirectSource {
    pub fn new() -> Result<Self, Failure> {
        let client = reqwest::Client::builder()
            .user_agent(UA)
            .https_only(true)
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Failure::new(Code::Refused, "http_client", false))?;
        Ok(Self { client })
    }
    async fn resolve(&self, target: &Target) -> Result<String, Failure> {
        target.validate()?;
        let username = match target {
            Target::Room(room) => return Ok(room.clone()),
            Target::User(name) => name,
        };
        let response = self
            .client
            .get("https://www.tiktok.com/api-live/user/room/")
            .query(&[
                ("aid", "1988"),
                ("app_name", "tiktok_web"),
                ("device_platform", "web_pc"),
                ("uniqueId", username.strip_prefix('@').unwrap_or(username)),
                ("sourceType", "54"),
            ])
            .header("Referer", "https://www.tiktok.com/")
            .send()
            .await
            .map_err(|e| request_failure(e, "room_lookup"))?;
        check_status(&response, "room_lookup")?;
        let body = bounded_json(response).await?;
        if body
            .get("statusCode")
            .is_some_and(|v| v.as_u64() != Some(0))
        {
            return Err(Failure::new(Code::Refused, "room_lookup", false));
        }
        if body
            .pointer("/data/liveRoom/status")
            .and_then(|v| v.as_u64())
            != Some(2)
        {
            return Err(Failure::new(Code::Offline, "room_lookup", false));
        }
        let room = body
            .pointer("/data/user/roomId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Failure::new(Code::Protocol, "room_lookup", false))?;
        room_number(room).map_err(|_| Failure::new(Code::Protocol, "room_lookup", false))?;
        Ok(room.into())
    }
    async fn guest_identity(&self) -> Result<Zeroizing<String>, Failure> {
        for endpoint in ["https://www.tiktok.com/", "https://www.tiktok.com/live"] {
            let response = self
                .client
                .get(endpoint)
                .header("Referer", "https://www.tiktok.com/")
                .header(
                    "Accept",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                )
                .header("Accept-Language", "en-US,en;q=0.9")
                .send()
                .await
                .map_err(|e| request_failure(e, "guest_bootstrap"))?;
            check_status(&response, "guest_bootstrap")?;
            for header in response.headers().get_all("set-cookie").iter().take(64) {
                if let Ok(value) = header.to_str()
                    && let Some(cookie) = guest_cookie(value)
                {
                    return Ok(cookie);
                }
            }
            // Only headers are needed. Do not buffer the HTML body.
        }
        Err(Failure::new(Code::Refused, "guest_cookie_missing", false))
    }
}
impl Source for DirectSource {
    type Io = MaybeTlsStream<TcpStream>;
    async fn connect(&self, target: &Target) -> Result<Connection<Self::Io>, Failure> {
        let room_id = self.resolve(target).await?;
        let cookie = self.guest_identity().await?;
        let mut request = socket_url(&room_id)?
            .as_str()
            .into_client_request()
            .map_err(|_| Failure::new(Code::Protocol, "socket_request", false))?;
        let mut header = HeaderValue::from_str(&cookie)
            .map_err(|_| Failure::new(Code::Protocol, "guest_cookie", false))?;
        header.set_sensitive(true);
        request.headers_mut().insert("Cookie", header);
        request
            .headers_mut()
            .insert("User-Agent", HeaderValue::from_static(UA));
        request
            .headers_mut()
            .insert("Origin", HeaderValue::from_static("https://www.tiktok.com"));
        let config = websocket_config();
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| Failure::new(Code::Timeout, "socket_handshake", true))?
        .map_err(|e| websocket_error(e, "socket_handshake"))?;
        Ok(Connection { room_id, socket })
    }
}
fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(16 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(64 * 1024)
        .max_message_size(Some(protocol::MAX_FRAME))
        .max_frame_size(Some(protocol::MAX_FRAME))
}
fn guest_cookie(text: &str) -> Option<Zeroizing<String>> {
    let pair = text.split(';').next()?.trim();
    let (name, value) = pair.split_once('=')?;
    if name != "ttwid"
        || value.is_empty()
        || value.len() > 4096
        || !value
            .bytes()
            .all(|b| (0x21..=0x7e).contains(&b) && !matches!(b, b'"' | b',' | b';' | b'\\'))
    {
        return None;
    }
    Some(Zeroizing::new(format!("ttwid={value}")))
}
fn request_failure(error: reqwest::Error, stage: &'static str) -> Failure {
    Failure::new(
        if error.is_timeout() {
            Code::Timeout
        } else {
            Code::Network
        },
        stage,
        true,
    )
}
fn check_status(response: &reqwest::Response, stage: &'static str) -> Result<(), Failure> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(http_failure(
            response.status().as_u16(),
            response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok()),
            stage,
        ))
    }
}
async fn bounded_json(mut response: reqwest::Response) -> Result<serde_json::Value, Failure> {
    let cap = || Failure::new(Code::Capacity, "http_body", false);
    if response
        .content_length()
        .is_some_and(|n| n > MAX_HTTP_BODY as u64)
    {
        return Err(cap());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| request_failure(e, "http_body"))?
    {
        if chunk.len() > MAX_HTTP_BODY - bytes.len() {
            return Err(cap());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Failure::new(Code::Protocol, "http_json", false))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn smaller_read_buffer_reassembles_large_fragments_with_interleaved_ping() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::{
            WebSocketStream,
            tungstenite::{
                Message,
                protocol::{
                    Role,
                    frame::{
                        Frame,
                        coding::{Data, OpCode},
                    },
                },
            },
        };
        let (client, server) = tokio::io::duplex(2048);
        let sender = tokio::spawn(async move {
            let mut socket = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            for index in 0..8u8 {
                socket
                    .send(Message::Frame(Frame::message(
                        vec![index; 16 * 1024],
                        OpCode::Data(if index == 0 {
                            Data::Binary
                        } else {
                            Data::Continue
                        }),
                        index == 7,
                    )))
                    .await
                    .unwrap();
                if index < 7 {
                    socket
                        .send(Message::Ping(vec![index].into()))
                        .await
                        .unwrap();
                }
            }
            // Keep the peer alive until the client has consumed the complete message.
            while let Some(Ok(message)) = socket.next().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        });
        let mut socket =
            WebSocketStream::from_raw_socket(client, Role::Client, Some(websocket_config())).await;
        let mut pings = 0;
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Ping(_) => pings += 1,
                Message::Binary(bytes) => {
                    assert_eq!(bytes.len(), 128 * 1024);
                    for (index, chunk) in bytes.as_chunks::<{ 16 * 1024 }>().0.iter().enumerate() {
                        assert!(chunk.iter().all(|b| usize::from(*b) == index));
                    }
                    break;
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }
        assert_eq!(pings, 7);
        socket.close(None).await.unwrap();
        sender.await.unwrap();
    }
    #[test]
    fn cookie_selects_only_guest_identity_and_rejects_injection() {
        assert_eq!(
            guest_cookie("ttwid=abc%7C123; Secure")
                .as_deref()
                .map(|s| s.as_str()),
            Some("ttwid=abc%7C123")
        );
        for bad in [
            "sessionid=secret",
            "ttwid=",
            "ttwid=hello\r\nX: secret",
            "ttwid=hello,other=value",
            "ttwid=bad\\value",
        ] {
            assert!(guest_cookie(bad).is_none());
        }
    }
    #[tokio::test]
    async fn chunked_http_body_is_bounded_without_content_length() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            for _ in 0..17 {
                if stream.write_all(b"10000\r\n").await.is_err() {
                    return;
                }
                if stream.write_all(&vec![b' '; 65536]).await.is_err() {
                    return;
                }
                if stream.write_all(b"\r\n").await.is_err() {
                    return;
                }
            }
            let _ = stream.write_all(b"0\r\n\r\n").await;
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("http://{addr}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            bounded_json(response).await.unwrap_err().code,
            Code::Capacity
        );
        server.abort();
    }
}
