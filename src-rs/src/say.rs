/*
 * Copyright (C) 2020 Yury Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

use log::{Record, Metadata};
use libc::{c_uchar, c_int, c_uint};

struct Say;
impl log::Log for Say {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // TODO: per-topic filtering
        (metadata.level() as i32) <= unsafe {
            extern "C" { static mut max_level: c_int; }
            max_level // safe, because there are no threads in octopus
        }
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return
        }

        let level = record.level() as i32;
        let mut filename = [0; 32];
        if let Some(f) = record.file() {
            let len = std::cmp::min(filename.len() - 1, f.len());
            filename[..len].copy_from_slice(&f.as_bytes()[..len]);
        }
        let line = record.line().unwrap_or(0);
        let msg = record.args().to_string(); // FIXME: avoid allocation

        unsafe {
            extern "C" { fn _say(level: c_int, filename: *const c_uchar, line: c_uint, format: *const c_uchar, ...); }
            _say(level, filename.as_ptr(), line, "%.*s\0".as_ptr(), msg.len(), msg.as_ptr());
        }
    }

    fn flush(&self) {}
}

static SAY: Say = Say;

#[no_mangle]
extern "C" fn rs_say_init() {
    log::set_logger(&SAY).unwrap();
}

#[no_mangle]
extern "C" fn rs_say_set_max_level(level: log::LevelFilter) {
    log::set_max_level(level);
}
