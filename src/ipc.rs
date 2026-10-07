use crate::{Res, STOP, poll};
use js8rs::{
    Submode,
    codec::{BuildFramesOptions, EncodedFrame, build_frames},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::net::{UnixListener, UnixStream},
    },
    sync::{Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

const MAX_REQ: usize = 4096;
const MAX_PEERS: usize = 32;
const MAX_JOBS: usize = 8;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Req {
    Ping {},
    Tx {
        mode: String,
        dial_hz: u64,
        audio_hz: f64,
        message: String,
    },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reply<'a> {
    Queued { frames: usize },
    Ok,
    Error { code: &'a str, message: &'a str },
}

pub struct Job {
    pub peer: UnixStream,
    pub mode: Submode,
    pub dial: u64,
    pub hz: f64,
    pub frames: Vec<EncodedFrame>,
}

struct Peer {
    fd: UnixStream,
    buf: [u8; MAX_REQ],
    len: usize,
    end: Instant,
}

pub fn reply(peer: &mut UnixStream, rsp: Reply<'_>) -> Res<()> {
    peer.set_nonblocking(false)?;
    peer.set_write_timeout(Some(Duration::from_secs(1)))?;
    serde_json::to_writer(&mut *peer, &rsp)?;
    peer.write_all(b"\n")?;
    Ok(())
}

fn encode(mode: &str, dial: u64, hz: f64, msg: &str) -> Res<(Submode, Vec<EncodedFrame>)> {
    let mode = match mode {
        "normal" | "n" => Submode::Normal,
        "fast" | "f" => Submode::Fast,
        "turbo" | "t" => Submode::Turbo,
        "slow" | "s" => Submode::Slow,
        "ultra" | "u" => Submode::Ultra,
        _ => return Err("bad submode".into()),
    };
    if !(100_000..=54_000_000).contains(&dial) {
        return Err("dial Hz outside 100000..54000000".into());
    }
    if !hz.is_finite() || hz < 200.0 || hz + mode.bandwidth_hz() as f64 > 3000.0 {
        return Err("audio tones must fit within 200..3000 Hz".into());
    }
    if msg.len() > 1024 || !msg.bytes().all(|c| c == b' ' || c.is_ascii_graphic()) {
        return Err("message must be printable ASCII, at most 1024 bytes".into());
    }
    let msg = msg.trim();
    if msg.is_empty() {
        return Err("empty message".into());
    }
    let opts = BuildFramesOptions::new(msg, mode).with_data(true);
    let frames = build_frames(&opts).encode()?;
    if frames.is_empty() {
        return Err("empty encoding".into());
    }
    // js8rs can drop unsupported characters so just leave it
    let text: String = frames.iter().map(|f| f.decode().message).collect();
    if text.trim_end() != msg.to_ascii_uppercase() {
        return Err("message cannot be encoded losslessly as JS8 text".into());
    }
    Ok((mode, frames))
}

fn submit(peer: &mut Peer, len: usize, jobs: &Mutex<VecDeque<Job>>) -> Res<()> {
    match serde_json::from_slice(&peer.buf[..len])? {
        Req::Ping {} => reply(&mut peer.fd, Reply::Ok),
        Req::Tx {
            mode,
            dial_hz,
            audio_hz,
            message,
        } => {
            let (mode, frames) = encode(&mode, dial_hz, audio_hz, &message)?;
            let mut queue = jobs.lock().unwrap();
            if STOP.load(Ordering::Relaxed) || queue.len() == MAX_JOBS {
                let code = if STOP.load(Ordering::Relaxed) {
                    "stopping"
                } else {
                    "busy"
                };
                return reply(
                    &mut peer.fd,
                    Reply::Error {
                        code,
                        message: code,
                    },
                );
            }
            let fd = peer.fd.try_clone()?;
            // Hold the queue until ack
            reply(
                &mut peer.fd,
                Reply::Queued {
                    frames: frames.len(),
                },
            )?;
            queue.push_back(Job {
                peer: fd,
                mode,
                dial: dial_hz,
                hz: audio_hz,
                frames,
            });
            Ok(())
        }
    }
}

pub fn serve(sock: &UnixListener, jobs: &Mutex<VecDeque<Job>>) -> io::Result<()> {
    let mut peers: Vec<Peer> = Vec::with_capacity(MAX_PEERS);
    while !STOP.load(Ordering::Relaxed) {
        if poll(sock.as_raw_fd(), libc::POLLIN, 100)? {
            match sock.accept() {
                Ok((mut fd, _)) => {
                    if peers.len() == MAX_PEERS {
                        let _ = reply(
                            &mut fd,
                            Reply::Error {
                                code: "busy",
                                message: "too many clients",
                            },
                        );
                    } else {
                        fd.set_nonblocking(true)?;
                        peers.push(Peer {
                            fd,
                            buf: [0; MAX_REQ],
                            len: 0,
                            end: Instant::now() + Duration::from_secs(2),
                        });
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(e),
            }
        }
        let mut i = 0;
        while i < peers.len() {
            let peer = &mut peers[i];
            let res = (|| -> Res<bool> {
                if Instant::now() >= peer.end {
                    return Err("request timed out".into());
                }
                match peer.fd.read(&mut peer.buf[peer.len..]) {
                    Ok(0) => return Err("incomplete request".into()),
                    Ok(n) => {
                        let old = peer.len;
                        peer.len += n;
                        if let Some(pos) = peer.buf[old..peer.len].iter().position(|&c| c == b'\n')
                        {
                            submit(peer, old + pos, jobs)?;
                            return Ok(true);
                        }
                        if peer.len == MAX_REQ {
                            return Err("request too long".into());
                        }
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) => {}
                    Err(e) => return Err(e.into()),
                }
                Ok(false)
            })();
            match res {
                Ok(false) => i += 1,
                res => {
                    let mut peer = peers.remove(i);
                    if let Err(e) = res {
                        let _ = reply(
                            &mut peer.fd,
                            Reply::Error {
                                code: "invalid_request",
                                message: &e.to_string(),
                            },
                        );
                    }
                }
            }
        }
    }
    for mut peer in peers {
        let _ = reply(
            &mut peer.fd,
            Reply::Error {
                code: "stopping",
                message: "stopping",
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests() {
        for mode in [
            "normal", "n", "fast", "f", "turbo", "t", "slow", "s", "ultra", "u",
        ] {
            assert!(encode(mode, 14_078_000, 1500.0, "CQ CQ DE N0CALL").is_ok());
        }
        for (mode, dial, hz, msg) in [
            ("x", 14_078_000, 1500.0, "HI"),
            ("n", 0, 1500.0, "HI"),
            ("n", 14_078_000, f64::NAN, "HI"),
            ("n", 14_078_000, f64::INFINITY, "HI"),
            ("u", 14_078_000, 2900.0, "HI"),
            ("n", 14_078_000, 1500.0, ""),
            ("n", 14_078_000, 1500.0, "  "),
            ("n", 14_078_000, 1500.0, "HÉ"),
            ("n", 14_078_000, 1500.0, "H\0I"),
            ("n", 14_078_000, 1500.0, "HI\nTX"),
            ("n", 14_078_000, 1500.0, ":HI"),
        ] {
            assert!(
                encode(mode, dial, hz, msg).is_err(),
                "{mode} {dial} {hz} {msg:?}"
            );
        }
        assert!(encode("n", 14_078_000, 1500.0, &"X".repeat(1025)).is_err());
        for line in [
            "{}",
            "null",
            "[]",
            "{",
            r#"{"op":"nope"}"#,
            r#"{"op":"ping","extra":true}"#,
            r#"{"op":"tx","mode":"n"}"#,
            r#"{"op":"tx","mode":"n","dial_hz":"14078000","audio_hz":1500,"message":"HI"}"#,
            r#"{"op":"tx","mode":"n","dial_hz":14078000,"audio_hz":null,"message":"HI"}"#,
        ] {
            assert!(serde_json::from_str::<Req>(line).is_err(), "{line:?}");
        }
        assert!(matches!(
            serde_json::from_str::<Req>(r#"{"op":"ping"}"#).unwrap(),
            Req::Ping {}
        ));
    }
}
