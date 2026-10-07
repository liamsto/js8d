use crate::{Res, ckstop};
use alsa::{
    Direction, ValueOr,
    pcm::{Access, Format, HwParams, PCM, State},
};
use js8rs::tx::Modulator;
use std::time::{Duration, Instant};

pub fn open(dev: &str) -> Res<PCM> {
    let pcm = PCM::new(dev, Direction::Playback, true)?;
    {
        let hw = HwParams::any(&pcm)?;
        hw.set_access(Access::RWInterleaved)?;
        hw.set_format(Format::s16())?;
        hw.set_channels(2)?;
        hw.set_rate(48_000, ValueOr::Nearest)?;
        hw.set_period_time_near(10_000, ValueOr::Nearest)?;
        hw.set_buffer_time_near(100_000, ValueOr::Nearest)?;
        pcm.hw_params(&hw)?;
        let sw = pcm.sw_params_current()?;
        // Start with the first block: queuing a whole buffer would shift UTC timing.
        sw.set_start_threshold(1)?;
        sw.set_avail_min(hw.get_period_size()?)?;
        pcm.sw_params(&sw)?;
    }
    Ok(pcm)
}

pub fn play(pcm: &PCM, md: &mut Modulator, gain: i32) -> Res<()> {
    let io = pcm.io_i16()?;
    let mut buf = [0i16; 960 * 2];
    let mut end = Instant::now() + Duration::from_secs(2);
    while !md.is_idle() {
        ckstop()?;
        let n = md.render_stereo(&mut buf) * 2;
        for s in &mut buf[..n] {
            *s = (i32::from(*s) * gain / 100) as i16;
        }
        let mut data = &buf[..n];
        while !data.is_empty() {
            ckstop()?;
            if Instant::now() >= end {
                return Err("PCM stalled".into());
            }
            match io.writei(data) {
                Ok(0) => return Err("PCM write returned zero".into()),
                Ok(n) => {
                    data = &data[n * 2..];
                    end = Instant::now() + Duration::from_secs(2);
                }
                Err(e) if e.errno() == libc::EAGAIN => match pcm.wait(Some(100)) {
                    Ok(_) => {}
                    Err(e) if e.errno() == libc::EINTR => {}
                    Err(e) => return Err(e.into()),
                },
                Err(e) if e.errno() == libc::EINTR => {}
                // An underrun corrupts a JS8 frame. Abort; never resume mid-frame.
                Err(e) => return Err(e.into()),
            }
        }
    }
    // Nonblocking drain retains PTT until the hardware has played the last sample.
    end = Instant::now() + Duration::from_secs(2);
    loop {
        ckstop()?;
        if Instant::now() >= end {
            return Err("PCM drain timed out".into());
        }
        match pcm.drain() {
            Ok(()) => return Ok(()),
            Err(e) if e.errno() == libc::EAGAIN || e.errno() == libc::EINTR => {
                if pcm.state() == State::Setup {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
}
