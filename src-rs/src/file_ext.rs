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

#![allow(non_camel_case_types)]

use bitflags::bitflags;
use libc::{c_int, c_uint};
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum Fadvice {
    DONTNEED = libc::POSIX_FADV_DONTNEED as isize,
    NOREUSE = libc::POSIX_FADV_NOREUSE as isize,
    NORMAL = libc::POSIX_FADV_NORMAL as isize,
    RANDOM = libc::POSIX_FADV_RANDOM as isize,
    SEQUENTIAL = libc::POSIX_FADV_SEQUENTIAL as isize,
    WILLNEED = libc::POSIX_FADV_WILLNEED as isize,
}

bitflags! {
    pub struct SyncFileRange: c_uint {
        const WAIT_BEFORE = libc::SYNC_FILE_RANGE_WAIT_BEFORE;
        const WRITE = libc::SYNC_FILE_RANGE_WRITE;
        const WAIT_AFTER = libc::SYNC_FILE_RANGE_WAIT_AFTER;
    }
}

bitflags! {
    pub struct Flock: c_int {
        const SH = libc::LOCK_SH;
        const EX = libc::LOCK_EX;
        const NB = libc::LOCK_NB;
        const UN = libc::LOCK_UN;
    }
}

type off_t = libc::off_t;

pub trait FileExt {
    fn fadvise(&self, offset: off_t, len: off_t, advice: Fadvice) -> io::Result<()>;
    fn sync_file_range(&self, offset: off_t, len: off_t, flags: SyncFileRange) -> io::Result<()>;
    fn flock(&self, flags: Flock) -> io::Result<()>;
}

impl FileExt for File {
    fn fadvise(&self, offset: off_t, len: off_t, advice: Fadvice) -> io::Result<()> {
        match unsafe { libc::posix_fadvise(self.as_raw_fd(), offset, len, advice as c_int) } {
            0 => Ok(()),
            errno => Err(io::Error::from_raw_os_error(errno))
        }
    }
    fn sync_file_range(&self, offset: off_t, len: off_t, flags: SyncFileRange) -> io::Result<()> {
        match unsafe { libc::sync_file_range(self.as_raw_fd(), offset, len, flags.bits()) } {
            0 => Ok(()),
            _ => Err(io::Error::last_os_error())
        }
    }
    fn flock(&self, flags: Flock) -> io::Result<()> {
        match unsafe { libc::flock(self.as_raw_fd(), flags.bits()) } {
            0 => Ok(()),
            _ => Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn file() -> File {
        let binary = std::env::args().nth(0).unwrap();
        File::open(binary).expect("open")
    }

    #[test]
    fn test_fadvise() {
        let f = file();
        f.fadvise(0, 10, Fadvice::SEQUENTIAL).expect("fadvise");
    }

    #[test]
    fn test_sync_file_range() {
        let f = file();
        f.sync_file_range(0, 4094, SyncFileRange::WRITE).expect("sync_file_range");
    }

    #[test]
    fn test_flock() {
        let f = file();
        f.flock(Flock::EX).expect("flock");
    }

}
