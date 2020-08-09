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

#![allow(dead_code)]

use paste::paste;
use std::ffi::CString;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;

mod ffi {
    #![allow(dead_code, non_camel_case_types)]
    include!("octopus_ev.rs");
}

macro_rules! ev_common_methods {
    ($typ:ident) => {
        // TODO: full zeriong is not required, only "public" fields must be initialized
        // libev provides ev_$typ_init() initializaion macro, which only inits required feilds
        // it's possible to mimic this behaviour with std::mem::UNINIT
        pub fn new() -> Self {
            unsafe { std::mem::zeroed() }
        }

        pub fn is_pending(&self) -> bool {
            self.active != 0
        }

        pub fn is_active(&self) -> bool {
            self.active != 0
        }

        pub fn priority(&self) -> i32 {
            self.priority
        }

        paste! {
            // FIXME: self must be pined
            //
            // self must be inited before calling start()
            //
            // it's possible to check for initialization by ensuring
            // that cb is Some and panic otherwise; however calling
            // NULL cb will certainly segfault and that is as
            // good as panic()
            pub fn start(&mut self) {
                unsafe { ffi::[<ev_ $typ _start>](self) }
            }

            pub fn stop(&mut self) {
                unsafe { ffi::[<ev_ $typ _stop>](self) }
            }
        }
    }
}

pub type TStamp = ffi::ev_tstamp;
pub type Stat = ffi::ev_stat;

impl Stat {
    ev_common_methods!(stat);

    pub fn init(&mut self, cb: extern "C" fn(*mut Self, libc::c_int), path: &Path, interval: TStamp) {
        self.cb = Some(cb);
        if !self.path.is_null() {
            drop(unsafe { CString::from_raw(self.path as *mut _) });
        }
        self.path = CString::new(path.as_os_str().as_bytes()).unwrap().into_raw();
        self.interval = interval;
    }
}

impl Drop for Stat {
    fn drop(&mut self) {
        self.stop();
        if !self.path.is_null() {
            drop(unsafe { CString::from_raw(self.path as *mut _) });
        }
    }
}
