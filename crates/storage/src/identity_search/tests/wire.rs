//! A recording TCP relay between the test's connection and PostgreSQL. It measures what the
//! client really sends: each simple query, and each bound statement with the byte length of
//! every parameter.
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

#[derive(Clone, Debug)]
pub struct Sent {
    pub sql: String,
    /// Byte length of each bound parameter. A simple query has none.
    pub parameters: Vec<usize>,
}

impl Sent {
    /// The `storage:identity_search.<tag>` ownership comment, or the bare statement.
    pub fn tag(&self) -> &str {
        self.sql
            .split_once("/* storage:identity_search.")
            .and_then(|(_, rest)| rest.split_once(" */"))
            .map_or(self.sql.as_str(), |(tag, _)| tag)
    }

    pub fn bound_bytes(&self) -> usize {
        self.parameters.iter().sum()
    }
}

#[derive(Clone, Default)]
pub struct Wire {
    pub port: u16,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Wire {
    pub fn start(host: &str, port: u16) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let wire = Self {
            port: listener.local_addr()?.port(),
            sent: Arc::default(),
        };
        let upstream = (host.to_owned(), port);
        let sent = wire.sent.clone();
        thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let Ok(server) = TcpStream::connect((upstream.0.as_str(), upstream.1)) else {
                    continue;
                };
                // Without this each forwarded message waits for the peer's delayed acknowledgement.
                if client.set_nodelay(true).is_err() || server.set_nodelay(true).is_err() {
                    continue;
                }
                let (Ok(mut replies), Ok(mut back)) = (server.try_clone(), client.try_clone())
                else {
                    continue;
                };
                thread::spawn(move || {
                    let _ = std::io::copy(&mut replies, &mut back);
                    let _ = back.shutdown(Shutdown::Both);
                });
                let sent = sent.clone();
                thread::spawn(move || {
                    let _ = relay(client, &server, &sent);
                    let _ = server.shutdown(Shutdown::Both);
                });
            }
        });
        Ok(wire)
    }

    pub fn take(&self) -> Vec<Sent> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

fn text(bytes: &[u8]) -> (String, &[u8]) {
    let end = bytes.iter().position(|byte| *byte == 0).unwrap();
    (
        String::from_utf8_lossy(&bytes[..end]).into_owned(),
        &bytes[end + 1..],
    )
}

fn int16(bytes: &[u8]) -> (usize, &[u8]) {
    (
        u16::from_be_bytes([bytes[0], bytes[1]]) as usize,
        &bytes[2..],
    )
}

fn relay(
    mut client: TcpStream,
    mut server: &TcpStream,
    sent: &Mutex<Vec<Sent>>,
) -> std::io::Result<()> {
    // The startup packet has no type byte. The test connects without TLS.
    let mut length = [0_u8; 4];
    client.read_exact(&mut length)?;
    let mut startup = vec![0_u8; u32::from_be_bytes(length) as usize - 4];
    client.read_exact(&mut startup)?;
    assert_eq!(startup[..4], [0, 3, 0, 0], "expected a plain 3.0 startup");
    server.write_all(&[length.as_slice(), startup.as_slice()].concat())?;
    let mut statements = HashMap::new();
    loop {
        let mut head = [0_u8; 5];
        client.read_exact(&mut head)?;
        let mut body =
            vec![0_u8; u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4];
        client.read_exact(&mut body)?;
        match head[0] {
            b'P' => {
                let (name, rest) = text(&body);
                statements.insert(name, text(rest).0);
            }
            b'B' => {
                let (_portal, rest) = text(&body);
                let (statement, rest) = text(rest);
                let (formats, rest) = int16(rest);
                let (count, mut rest) = int16(&rest[formats * 2..]);
                let mut parameters = Vec::new();
                for _ in 0..count {
                    let size = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
                    let size = usize::try_from(size).unwrap_or(0);
                    parameters.push(size);
                    rest = &rest[4 + size..];
                }
                sent.lock().unwrap().push(Sent {
                    sql: statements.get(&statement).cloned().unwrap_or_default(),
                    parameters,
                });
            }
            b'Q' => sent.lock().unwrap().push(Sent {
                sql: text(&body).0,
                parameters: Vec::new(),
            }),
            _ => {}
        }
        let mut message = head.to_vec();
        message.extend_from_slice(&body);
        server.write_all(&message)?;
    }
}
