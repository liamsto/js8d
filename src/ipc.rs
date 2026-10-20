use crate::{Res, STOP, poll};
use js8rs::{
    Submode,
    codec::{BuildFramesOptions, EncodedFrame, build_frames, parse_frame},
    command::parse_command,
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
        power_w: Option<u16>,
        message: String,
        call: Option<String>,
        #[serde(default)]
        grid: String,
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
    pub pwr: Option<u16>,
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

fn encode(
    mode: &str,
    dial: u64,
    hz: f64,
    msg: &str,
    call: Option<&str>,
    grid: &str,
) -> Res<(Submode, Vec<EncodedFrame>)> {
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
    let mut raw = build_frames(&opts);
    if raw.frames.is_empty() {
        return Err("empty encoding".into());
    }
    // Check text before framing adds headers and checksums.
    let text: String = raw
        .frames
        .iter()
        .map(|f| parse_frame(&f.encoded, f.flags, mode).message)
        .collect();
    if text.trim_end() != msg.to_ascii_uppercase() {
        return Err("message cannot be encoded losslessly as JS8 text".into());
    }
    if let Some(call) = call {
        let call = call.to_ascii_uppercase();
        let grid = grid.to_ascii_uppercase();
        // Reuse the sender parser for callsign syntax.
        let id = format!("{call}: CQ");
        if !parse_command(&id, None).is_some_and(|p| p.sender == Some(call.as_str())) {
            return Err("bad station call".into());
        }
        if !grid.is_empty()
            && !matches!(
                grid.as_bytes(),
                [b'A'..=b'R', b'A'..=b'R', b'0'..=b'9', b'0'..=b'9']
            )
        {
            return Err("grid must be four chars, e.g. CN89".into());
        }
        let id = build_frames(&BuildFramesOptions::new("HB", mode).with_station(&call, &grid));
        if id.frames.len() != 1
            || parse_frame(&id.frames[0].encoded, id.frames[0].flags, mode).compound != call
        {
            return Err("station call cannot be encoded losslessly".into());
        }
        raw = build_frames(&opts.with_station(call, grid).with_data(false));
    } else if !grid.is_empty() {
        return Err("grid requires call".into());
    }
    if raw.frames.is_empty() {
        return Err("empty encoding".into());
    }
    Ok((mode, raw.encode()?))
}

fn submit(peer: &mut Peer, len: usize, jobs: &Mutex<VecDeque<Job>>) -> Res<()> {
    match serde_json::from_slice(&peer.buf[..len])? {
        Req::Ping {} => reply(&mut peer.fd, Reply::Ok),
        Req::Tx {
            mode,
            dial_hz,
            audio_hz,
            power_w,
            message,
            call,
            grid,
        } => {
            let min = if cfg!(feature = "k3") { 0 } else { 1 };
            if power_w.is_some_and(|w| !(min..=110).contains(&w)) {
                return Err(format!("power_w outside {min}..110 watts").into());
            }
            let (mode, frames) =
                encode(&mode, dial_hz, audio_hz, &message, call.as_deref(), &grid)?;
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
                pwr: power_w,
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
    use js8rs::{
        FrameFlags, FrameType,
        rx::{Decoded, MessageBufferAssembler, ReassemblyEvent},
    };

    fn txfr(msg: &str, call: Option<&str>, grid: Option<&str>) -> Vec<EncodedFrame> {
        let mut req = serde_json::json!({
            "op": "tx", "mode": "n", "dial_hz": 14078000,
            "audio_hz": 1500, "message": msg,
        });
        if let Some(call) = call {
            req["call"] = call.into();
        }
        if let Some(grid) = grid {
            req["grid"] = grid.into();
        }
        let req = serde_json::to_vec(&req).unwrap();
        let (fd, mut cli) = UnixStream::pair().unwrap();
        let mut peer = Peer {
            fd,
            buf: [0; MAX_REQ],
            len: req.len(),
            end: Instant::now() + Duration::from_secs(2),
        };
        peer.buf[..req.len()].copy_from_slice(&req);
        let jobs = Mutex::new(VecDeque::new());
        submit(&mut peer, req.len(), &jobs).unwrap();
        let job = jobs.lock().unwrap().pop_front().unwrap();
        let ack = format!(
            "{{\"status\":\"queued\",\"frames\":{}}}\n",
            job.frames.len()
        );
        let mut buf = vec![0; ack.len()];
        cli.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        cli.read_exact(&mut buf).unwrap();
        assert_eq!(buf, ack.as_bytes());
        let fr = job.frames;
        assert!(fr[0].frame.flags.contains(FrameFlags::FIRST));
        assert!(fr.last().unwrap().frame.flags.contains(FrameFlags::LAST));
        fr
    }

    #[test]
    fn cmds() {
        for (msg, cmd) in [
            ("k2xyz snr?", " SNR?"),
            ("K2XYZ GRID?", " GRID?"),
            ("K2XYZ INFO?", " INFO?"),
            ("K2XYZ ACK", " ACK"),
            ("K2XYZ SNR -12", " SNR"),
            ("K2XYZ HELLO WORLD", " "),
        ] {
            let fr = txfr(msg, Some("k1abc"), None);
            let hdr = fr[0].decode();
            assert_eq!(hdr.frame_type, FrameType::FrameDirected);
            assert_eq!(&hdr.directed[..3], ["K1ABC", "K2XYZ", cmd]);
            if cmd == " SNR" {
                assert_eq!(hdr.directed[3], "-12");
            } else if cmd == " " {
                let txt: String = fr[1..].iter().map(|f| f.decode().message).collect();
                assert_eq!(txt, "HELLO WORLD");
            } else {
                assert_eq!(fr.len(), 1);
            }
            let raw = txfr(msg, None, None);
            assert!(
                raw.iter()
                    .all(|f| f.decode().frame_type == FrameType::FrameData)
            );
            assert_eq!(
                raw.iter().map(|f| f.decode().message).collect::<String>(),
                msg.to_ascii_uppercase()
            );
        }
    }

    #[test]
    fn bufs() {
        let txt = "HELLO THIS IS A LONG MESSAGE THAT NEEDS SEVERAL FRAMES ".repeat(4);
        for cmd in [" MSG ", " CMD ", " QUERY "] {
            let fr = txfr(&format!("K2XYZ{cmd}{txt}"), Some("K1ABC"), None);
            assert!(fr.len() > 2);
            let mut asm = MessageBufferAssembler::new();
            let mut done = None;
            for f in fr {
                let dec = Decoded::new(f.decode(), 0, -10, 0.0, 1500.0, 1.0);
                if let Some(ReassemblyEvent::Completed(rsp)) = asm.push_decoded(&dec) {
                    done = Some(rsp);
                }
            }
            let done = done.expect("buffered cmd did not reassemble");
            assert_eq!(done.from, "K1ABC");
            assert_eq!(done.to, "K2XYZ");
            assert_eq!(done.command.trim(), cmd.trim());
            assert_eq!(done.payload, txt.trim_end());
            let crc = done.checksum.unwrap();
            assert!(crc.valid);
            assert_eq!(crc.size_bits, 16);
        }
    }

    #[test]
    fn bcns() {
        for (msg, alt) in [("HB EM73", false), ("CQ CQ CQ EM73", true)] {
            for mode in ["s", "n", "f", "t", "u"] {
                let (_, fr) = encode(mode, 14_078_000, 1500.0, msg, Some("K1ABC"), "").unwrap();
                assert_eq!(fr.len(), 1);
                let dec = fr[0].decode();
                assert_eq!(dec.frame_type, FrameType::FrameHeartbeat);
                assert_eq!(dec.compound, "K1ABC");
                assert_eq!(dec.extra, "EM73");
                assert_eq!(dec.is_alt, alt);
            }
        }
        let fr = txfr("`K1ABC EM73", Some("K1ABC"), None);
        assert_eq!(fr.len(), 1);
        assert_eq!(fr[0].decode().frame_type, FrameType::FrameCompound);
        assert_eq!(fr[0].decode().compound, "K1ABC");
        assert_eq!(fr[0].decode().extra.trim(), "EM73");
    }

    #[test]
    fn cmpds() {
        for (call, to) in [
            ("F/K1ABC", "K2XYZ"),
            ("K1ABC", "F/K2XYZ"),
            ("K1ABC", "@ALLCALL"),
        ] {
            let fr = txfr(&format!("{to} SNR?"), Some(call), Some("em73"));
            assert_eq!(fr.len(), 2);
            let id = fr[0].decode();
            assert_eq!(id.frame_type, FrameType::FrameCompound);
            assert_eq!(id.compound, call);
            assert_eq!(id.extra.trim(), "EM73");
            let hdr = fr[1].decode();
            assert_eq!(hdr.frame_type, FrameType::FrameCompoundDirected);
            assert_eq!(hdr.directed, ["<....>", to, " SNR?"]);
        }
    }

    #[test]
    fn bad_stn() {
        for call in [
            "",
            " ",
            "ABC",
            "123",
            "@ALLCALL",
            "K1ABC X",
            "K1ABC\n",
            "K1ÉBC",
            "K123456789",
            "K1ABC/TEST",
        ] {
            assert!(
                encode("n", 14_078_000, 1500.0, "K2XYZ SNR?", Some(call), "").is_err(),
                "{call:?}"
            );
        }
        for grid in ["EM7", "EM730", "EM73AA", "ZZ99", "EM7X", " EM73", "EM73\n"] {
            assert!(
                encode("n", 14_078_000, 1500.0, "K2XYZ SNR?", Some("K1ABC"), grid).is_err(),
                "{grid:?}"
            );
        }
        assert!(encode("n", 14_078_000, 1500.0, "HI", None, "EM73").is_err());
        for msg in [
            "",
            "  ",
            "K1ABC:",
            "K2XYZ MSG HÉ",
            "K2XYZ MSG HI\nTX",
            ":HI",
        ] {
            assert!(
                encode("n", 14_078_000, 1500.0, msg, Some("K1ABC"), "").is_err(),
                "{msg:?}"
            );
        }
    }

    #[test]
    fn pwr_reqs() {
        use serde_json::json;

        for (w, ok) in [
            (json!(null), true),
            (json!(0), cfg!(feature = "k3")),
            (json!(1), true),
            (json!(10), true),
            (json!(110), true),
            (json!(111), false),
            (json!(65536), false),
            (json!(-1), false),
            (json!(1.5), false),
            (json!("10"), false),
            (json!(true), false),
        ] {
            let req = serde_json::to_vec(&json!({
                "op": "tx", "mode": "n", "dial_hz": 14078000,
                "audio_hz": 1500, "message": "HI", "power_w": w,
            }))
            .unwrap();
            let (fd, _cli) = UnixStream::pair().unwrap();
            let mut peer = Peer {
                fd,
                buf: [0; MAX_REQ],
                len: req.len(),
                end: Instant::now() + Duration::from_secs(2),
            };
            peer.buf[..req.len()].copy_from_slice(&req);
            let jobs = Mutex::new(VecDeque::new());
            assert_eq!(submit(&mut peer, req.len(), &jobs).is_ok(), ok, "{w}");
            let job = jobs.lock().unwrap().pop_front();
            assert_eq!(job.is_some(), ok, "{w}");
            if let Some(job) = job {
                assert_eq!(job.pwr.map(u64::from), w.as_u64());
            }
        }
    }

    #[test]
    fn requests() {
        for mode in [
            "normal", "n", "fast", "f", "turbo", "t", "slow", "s", "ultra", "u",
        ] {
            assert!(encode(mode, 14_078_000, 1500.0, "CQ CQ DE N0CALL", None, "").is_ok());
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
                encode(mode, dial, hz, msg, None, "").is_err(),
                "{mode} {dial} {hz} {msg:?}"
            );
        }
        assert!(encode("n", 14_078_000, 1500.0, &"X".repeat(1025), None, "").is_err());
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
