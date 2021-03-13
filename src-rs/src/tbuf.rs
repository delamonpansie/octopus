/*
 * Copyright (C) 2020, 2021 Yury Vostrikov
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

use libc::{c_void, size_t};
use std::{io, fmt};

use crate::palloc::PallocPool;

#[derive(Debug)]
#[repr(C)]
pub struct TBuf {
    ptr: *mut c_void,
    end: *mut c_void,
    free: u32,
    pool: *mut PallocPool,
}

extern {
    fn tbuf_reserve_aux(buf: *mut TBuf, additional: size_t);
    fn tbuf_reset(buf: *mut TBuf);
    fn tbuf_append(buf: *mut TBuf, data: *const c_void, len: size_t);
}

impl TBuf {
    pub fn len(&self) -> usize {
        self.end as usize - self.ptr as usize
    }

    pub fn cap(&self) -> usize {
        self.len() + self.free as usize
    }

    pub fn reserve(&mut self, additional: usize) {
        if self.cap() - self.len() < additional {
            unsafe { tbuf_reserve_aux(self, additional) }
        }
    }

    pub fn clear(&mut self) {
        unsafe { tbuf_reset(self) }
    }

    pub fn extend_from_slice(&mut self, other: &[u8]) {
        unsafe { tbuf_append(self, other.as_ptr() as *const c_void, other.len()) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len()) }
    }

    pub fn as_bytes_mut(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.len()) }
    }
}

impl io::Write for TBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Write for TBuf {
    fn write_str(&mut self, str: &str) -> fmt::Result {
        self.extend_from_slice(str.as_bytes());
        Ok(())
    }
}
