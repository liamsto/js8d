use crate::{Res, poll};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    time::{Duration, Instant},
};

// Elecraft K3 programmer's ref G5: DT/FA/FR p11, MD p16, TQ/TX p27-28.
// Elecraft K4 programmer's ref D5: DT, FA, FT, MD, TQX, TX and VXD.
#[cfg(feature = "k3")]
const INIT: &str = "AI0;K20;K31;TT0;RX;TQ;";
#[cfg(not(feature = "k3"))]
const INIT: &str = "AI0;K41;RX;TQX;";
#[cfg(feature = "k3")]
const RX: &str = "RX;TQ;";
#[cfg(not(feature = "k3"))]
const RX: &str = "RX;TQX;";
#[cfg(feature = "k3")]
const TX: &str = "TX;TQ;";
#[cfg(not(feature = "k3"))]
const TX: &str = "TX;TQX;";
#[cfg(feature = "k3")]
const TQ: &str = "TQ;";
#[cfg(not(feature = "k3"))]
const TQ: &str = "TQX;";

pub struct Cat {
    fd: File,
    keyed: bool,
}

impl Cat {
    pub fn open(path: &str, baud: u32) -> Res<Self> {
        let speed = match baud {
            4800 => libc::B4800,
            9600 => libc::B9600,
            19200 => libc::B19200,
            38400 => libc::B38400,
            #[cfg(feature = "k4")]
            57600 => libc::B57600,
            #[cfg(feature = "k4")]
            115200 => libc::B115200,
            _ => return Err("unsupported CAT baud for this radio".into()),
        };
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_NONBLOCK)
            .open(path)?;
        let raw = fd.as_raw_fd();
        // SAFETY: raw is a live fd; termios and modem bits point to valid storage.
        unsafe {
            if libc::ioctl(raw, libc::TIOCEXCL) < 0 {
                return Err(io::Error::last_os_error().into());
            }
            let mut tio: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(raw, &mut tio) < 0 {
                return Err(io::Error::last_os_error().into());
            }
            libc::cfmakeraw(&mut tio);
            tio.c_cflag &= !(libc::CSTOPB | libc::CRTSCTS);
            tio.c_cflag |= libc::CLOCAL | libc::CREAD;
            tio.c_cc[libc::VMIN] = 0;
            tio.c_cc[libc::VTIME] = 0;
            if libc::cfsetspeed(&mut tio, speed) < 0
                || libc::tcsetattr(raw, libc::TCSANOW, &tio) < 0
            {
                return Err(io::Error::last_os_error().into());
            }
            let bits = libc::TIOCM_DTR | libc::TIOCM_RTS;
            if libc::ioctl(raw, libc::TIOCMBIC, &bits) < 0 {
                let err = io::Error::last_os_error();
                // Pseudo-ttys have no modem lines (also used by the smoke test).
                if err.raw_os_error() != Some(libc::ENOTTY) {
                    return Err(err.into());
                }
            }
            if libc::tcflush(raw, libc::TCIOFLUSH) < 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        let mut cat = Self { fd, keyed: true };
        cat.xchg(INIT, b"TQ0;")?;
        cat.keyed = false;
        Ok(cat)
    }

    // Unsolicited band-change reports may arrive even with AI0 on the K3.
    // Ignore stale reports until the requested readback arrives, or time out.
    fn xchg(&mut self, cmd: &str, want: &[u8]) -> Res<()> {
        let end = Instant::now() + Duration::from_secs(2);
        let mut out = cmd.as_bytes();
        let mut buf = [0u8; 128];
        let mut len = 0;
        let mut last_tq = None;
        while Instant::now() < end {
            if !out.is_empty() {
                if !poll(self.fd.as_raw_fd(), libc::POLLOUT, 100)? {
                    continue;
                }
                match self.fd.write(out) {
                    Ok(0) => return Err("CAT write returned zero".into()),
                    Ok(n) => out = &out[n..],
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) => {}
                    Err(e) => return Err(e.into()),
                }
                continue;
            }
            if !poll(self.fd.as_raw_fd(), libc::POLLIN, 100)? {
                continue;
            }
            match self.fd.read(&mut buf[len..]) {
                Ok(0) => return Err("CAT disconnected".into()),
                Ok(n) => len += n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
            while let Some(pos) = buf[..len].iter().position(|&c| c == b';') {
                let rsp = &buf[..=pos];
                if rsp.contains(&b'?') {
                    return Err(
                        format!("CAT rejected command: {}", String::from_utf8_lossy(rsp)).into(),
                    );
                }
                if rsp == want {
                    return Ok(());
                }
                if want.starts_with(b"TQ") && matches!(rsp, b"TQ0;" | b"TQ1;") {
                    last_tq = Some(rsp[2] as char);
                    // RX/TX may still be settling and AI0 won't send an update, so wait a bit before checking, otherwise we'll get the wrong report
                    std::thread::sleep(Duration::from_millis(100));
                    out = TQ.as_bytes();
                }
                buf.copy_within(pos + 1..len, 0);
                len -= pos + 1;
            }
            if len == buf.len() {
                return Err("CAT response too long".into());
            }
        }
        Err(format!(
            "CAT readback timed out, wanted {}{}",
            String::from_utf8_lossy(want),
            last_tq
                .map(|v| format!(", last TQ{v};"))
                .unwrap_or_default()
        )
        .into())
    }

    pub fn tune(&mut self, hz: u64) -> Res<()> {
        self.xchg(TQ, b"TQ0;")?;
        let freq = format!("FA{hz:011};");
        // G5 p9/p11: FA ignores 1 Hz digits without FINE; UP0 always steps 1 Hz.
        #[cfg(feature = "k3")]
        let cmd = format!(
            "FA{:011};{}FA;",
            hz / 10 * 10,
            "UP0;".repeat((hz % 10) as usize)
        );
        #[cfg(not(feature = "k3"))]
        let cmd = format!("{freq}FA;");
        self.xchg(&cmd, freq.as_bytes())?;
        // Band changes restore per-band settings so apply these AFTER setting FA.
        #[cfg(feature = "k3")]
        self.xchg("FR0;FT;", b"FT0;")?;
        #[cfg(not(feature = "k3"))]
        self.xchg("FT0;FT;", b"FT0;")?;
        // Enter DATA before DT, then force normal sideband for the chosen submode.
        self.xchg("MD6;DT0;MD6;MD;", b"MD6;")?;
        self.xchg("DT;", b"DT0;")?;
        // Mode changes can apply a CW VFO offset, reset freq
        self.xchg(&cmd, freq.as_bytes())?;
        self.xchg("RT0;RT;", b"RT0;")?;
        self.xchg("XT0;XT;", b"XT0;")?;
        #[cfg(feature = "k3")]
        self.xchg("VX0;VX;", b"VX0;")?;
        #[cfg(not(feature = "k3"))]
        self.xchg("VXD0;VXD;", b"VXD0;")?;
        Ok(())
    }

    pub fn tx(&mut self) -> Res<()> {
        // could've keyed even if it didn't work
        self.keyed = true;
        self.xchg(TX, b"TQ1;")
    }

    pub fn rx(&mut self) -> Res<()> {
        if self.keyed {
            self.xchg(RX, b"TQ0;")?;
            self.keyed = false;
        }
        Ok(())
    }
}

impl Drop for Cat {
    fn drop(&mut self) {
        if let Err(e) = self.rx() {
            eprintln!("js8d: RX on close failed: {e}");
        }
    }
}
