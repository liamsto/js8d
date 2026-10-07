#[cfg(not(target_os = "linux"))]
compile_error!("js8d requires Linux");
#[cfg(any(
    all(feature = "k3", feature = "k4"),
    not(any(feature = "k3", feature = "k4"))
))]
compile_error!("select either k3 or k4 not both");

mod cat;
mod ipc;
mod pcm;

use cat::Cat;
use ipc::{Reply, reply};
use js8rs::{
    Submode,
    timing::unix_time_ms,
    tx::{Channel, Modulator},
};
use std::{
    collections::VecDeque,
    env, fs, io,
    os::unix::net::UnixListener,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

type Res<T> = Result<T, Box<dyn std::error::Error>>;
static STOP: AtomicBool = AtomicBool::new(false);

const HELP: &str = r"js8d -- JS8 transmit daemon (Linux, libasound)

Build: cargo build --release                         # K3
       cargo build --release --no-default-features --features k4
Add experimental-time to the features for immediate, unslotted frames.

Environment:
  JS8_PCM       ALSA playback name, required (e.g. plughw:CARD=USB,DEV=0)
  JS8_CAT       CAT tty, required (e.g. /dev/ttyUSB0)
  JS8_SOCK      socket path (default $XDG_RUNTIME_DIR/js8d.sock,
                or /tmp/js8d-<uid>.sock if XDG_RUNTIME_DIR is unset)
  JS8_BAUD      serial baud, 8N1, no flow control (default 38400)
  JS8_PTT_MS    PTT lead time, 0..2000 ms (default 200)
  JS8_GAIN      PCM amplitude, 1..100 percent (default 25)

No terminal or stdin is used. Run under a service manager; see contrib/js8d.service.
Requests and replies are newline-delimited JSON over a private Unix socket.
See README.md for the protocol, client example, and radio setup.
SIGINT/SIGTERM stop audio, send RX, discard queued jobs, and remove the socket.
";

//TODO: is libc even necessary?
extern "C" fn sig(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn ckstop() -> Res<()> {
    if STOP.load(Ordering::Relaxed) {
        Err("stopping".into())
    } else {
        Ok(())
    }
}

// Keeps tty/io interruptable
fn poll(fd: libc::c_int, ev: libc::c_short, ms: i32) -> io::Result<bool> {
    let mut pfd = libc::pollfd {
        fd,
        events: ev,
        revents: 0,
    };
    // SAFETY: only one pollfd for this call
    let n = unsafe { libc::poll(&mut pfd, 1, ms) };
    if n < 0 {
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
    Ok(n > 0)
}

fn wait_ms(end: u64) -> Res<()> {
    let limit = Instant::now() + Duration::from_millis(end.saturating_sub(unix_time_ms()) + 1000);
    loop {
        ckstop()?;
        if Instant::now() >= limit {
            return Err("clock moved backwards".into());
        }
        let left = end.saturating_sub(unix_time_ms());
        if left == 0 {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(left.min(20)));
    }
}

fn slot(mode: Submode, now: u64, lead: u64) -> u64 {
    if cfg!(feature = "experimental-time") {
        now + lead
    } else {
        let period = mode.period_seconds() * 1000;
        ((now + lead) / period + 1) * period
    }
}

fn envnum(name: &str, def: u32, min: u32, max: u32) -> Res<u32> {
    let n = match env::var(name) {
        Ok(s) => s.parse()?,
        Err(env::VarError::NotPresent) => def,
        Err(e) => return Err(e.into()),
    };
    if !(min..=max).contains(&n) {
        return Err(format!("{name} outside {min}..{max}").into());
    }
    Ok(n)
}

struct Sock {
    fd: UnixListener,
    path: PathBuf,
}

impl Drop for Sock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn main() -> Res<()> {
    if let Some(arg) = env::args().nth(1) {
        if arg == "-h" || arg == "--help" {
            print!("{HELP}");
            return Ok(());
        }
        return Err(format!("unknown argument: {arg}").into());
    }

    let dev = env::var("JS8_PCM").map_err(|_| "JS8_PCM is required")?;
    let tty = env::var("JS8_CAT").map_err(|_| "JS8_CAT is required")?;
    let baud = envnum("JS8_BAUD", 38400, 4800, 115200)?;
    let lead = u64::from(envnum("JS8_PTT_MS", 200, 0, 2000)?);
    let gain = envnum("JS8_GAIN", 25, 1, 100)? as i32;
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let path = env::var_os("JS8_SOCK")
        .map(PathBuf::from)
        .or_else(|| env::var_os("XDG_RUNTIME_DIR").map(|p| PathBuf::from(p).join("js8d.sock")))
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/js8d-{uid}.sock")));

    // SAFETY: zero is valid for sigaction and handler is just an atomic bool
    // normal shutdown always reaches Drop
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = sig as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        for signo in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signo, &sa, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        libc::umask(0o077);
    }
    let sock = Sock {
        fd: UnixListener::bind(&path)?,
        path,
    };
    sock.fd.set_nonblocking(true)?;
    let mut cat = Cat::open(&tty, baud)?;
    let pcm = pcm::open(&dev)?;
    let mut md = Modulator::new();
    let jobs = Mutex::new(VecDeque::new());
    eprintln!("js8d: listening on {}", sock.path.display());

    // One socket thread and one radio owner; no thread per client or async runtime.
    thread::scope(|scope| -> Res<()> {
        let server = scope.spawn(|| {
            let res = ipc::serve(&sock.fd, &jobs);
            STOP.store(true, Ordering::Relaxed);
            res
        });
        let res = (|| -> Res<()> {
            while !STOP.load(Ordering::Relaxed) {
                let job = jobs.lock().unwrap().pop_front();
                let Some(mut job) = job else {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                };
                let tx = (|| -> Res<()> {
                    ckstop()?;
                    cat.tune(job.dial)?;
                    for frame in &job.frames {
                        pcm.prepare()?;
                        let at = slot(job.mode, unix_time_ms(), lead);
                        wait_ms(at - lead)?;
                        let key_at = unix_time_ms();
                        cat.tx()?;
                        wait_ms(at.max(key_at + lead))?;
                        let now = unix_time_ms();
                        if !cfg!(feature = "experimental-time")
                            && (now < at || now > at + job.mode.start_delay_ms())
                        {
                            return Err("missed TX slot".into());
                        }
                        md.start(frame, now, job.hz, Duration::ZERO, Channel::Mono);
                        pcm::play(&pcm, &mut md, gain)?;
                        cat.rx()?;
                    }
                    Ok(())
                })();
                // Discard audio before unkey
                let _ = pcm.drop();
                if let Err(e) = cat.rx() {
                    let msg = format!("RX failed: {e}");
                    let _ = reply(
                        &mut job.peer,
                        Reply::Error {
                            code: "rx_failed",
                            message: &msg,
                        },
                    );
                    return Err(msg.into());
                }
                match tx {
                    Ok(()) => {
                        let _ = reply(&mut job.peer, Reply::Ok);
                    }
                    Err(e) => {
                        eprintln!("js8d: {e}");
                        let code = if STOP.load(Ordering::Relaxed) {
                            "stopping"
                        } else {
                            "tx_failed"
                        };
                        let _ = reply(
                            &mut job.peer,
                            Reply::Error {
                                code,
                                message: &e.to_string(),
                            },
                        );
                    }
                }
            }
            Ok(())
        })();
        STOP.store(true, Ordering::Relaxed);
        let net = server.join().expect("socket thread panicked");
        for mut job in jobs.lock().unwrap().drain(..) {
            let _ = reply(
                &mut job.peer,
                Reply::Error {
                    code: "stopping",
                    message: "stopping",
                },
            );
        }
        res?;
        net?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots() {
        for mode in [
            Submode::Slow,
            Submode::Normal,
            Submode::Fast,
            Submode::Turbo,
            Submode::Ultra,
        ] {
            let period = mode.period_seconds() * 1000;
            for now in [0, 1, period - 1, period, period + 1] {
                let at = slot(mode, now, 200);
                assert!(at >= now + 200);
                if cfg!(feature = "experimental-time") {
                    assert_eq!(at, now + 200);
                } else {
                    assert_eq!(at % period, 0);
                    assert!(at - now <= period + 200);
                }
            }
        }
    }
}
