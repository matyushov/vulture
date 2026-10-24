//! A tiny scripted HTTP/1.1 server standing in for Telegram and Discord. It records every request (with its
//! arrival time) and answers from a queue of scripted replies, then with a default.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub struct Recorded {
    pub at: Instant,
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Recorded {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Reply {
    pub fn ok() -> Self {
        Self {
            status: 200,
            headers: vec![],
            body: r#"{"ok":true}"#.into(),
        }
    }
    pub fn no_content() -> Self {
        Self {
            status: 204,
            headers: vec![],
            body: String::new(),
        }
    }
    pub fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![],
            body: body.into(),
        }
    }
    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.into(), v.into()));
        self
    }
}

pub struct MockHttp {
    pub addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
    script: Arc<Mutex<VecDeque<Reply>>>,
    default: Arc<Mutex<Reply>>,
}

impl MockHttp {
    pub async fn start(default: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(VecDeque::new()));
        let default = Arc::new(Mutex::new(default));
        tokio::spawn({
            let (requests, script, default) = (requests.clone(), script.clone(), default.clone());
            async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        return;
                    };
                    let (requests, script, default) =
                        (requests.clone(), script.clone(), default.clone());
                    tokio::spawn(async move {
                        let Some(rec) = read_request(&mut sock).await else {
                            return;
                        };
                        requests.lock().unwrap().push(rec);
                        let reply = script
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| default.lock().unwrap().clone());
                        let mut head = format!(
                            "HTTP/1.1 {} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
                            reply.status,
                            reply.body.len()
                        );
                        for (k, v) in &reply.headers {
                            head.push_str(&format!("{k}: {v}\r\n"));
                        }
                        head.push_str("\r\n");
                        let _ = sock.write_all(head.as_bytes()).await;
                        let _ = sock.write_all(reply.body.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    });
                }
            }
        });
        Self {
            addr,
            requests,
            script,
            default,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Queues replies for the next requests.
    pub fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        self.script.lock().unwrap().extend(replies);
    }

    pub fn set_default(&self, reply: Reply) {
        *self.default.lock().unwrap() = reply;
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<Recorded> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.lines();
    let first = lines.next()?;
    let mut parts = first.split(' ');
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < head_end + len {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = String::from_utf8_lossy(&buf[head_end..(head_end + len).min(buf.len())]).to_string();
    Some(Recorded {
        at: Instant::now(),
        method,
        path,
        headers,
        body,
    })
}
