/*
 * Copyright (C) 2021 Yury Vostrikov
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

use std::fs::{File, Metadata};
use std::io;
use std::path::{Path, PathBuf};

use crate::file_ext::{FileExt, Flock};

struct XLogDir {
    fd: File,
    #[allow(dead_code)]
    filetype: &'static str,
    #[allow(dead_code)]
    suffix: &'static str,
    dirname: PathBuf,
}

impl XLogDir {
    fn new_waldir(path: &Path) -> io::Result<Self> {
        Ok(Self {
            fd: File::open(path)?,
            filetype: "XLOG\n",
            suffix: "xlog",
            dirname: path.into(),
        })
    }

    fn new_snapdir(path: &Path) -> io::Result<Self> {
        Ok(Self {
            fd: File::open(path)?,
            filetype: "SNAP\n",
            suffix: "snap",
            dirname: path.into(),
        })
    }

    fn sync(&self) -> io::Result<()> {
        self.fd.sync_all()
    }

    fn lock(&self) -> io::Result<()> {
        self.fd.flock(Flock::EX|Flock::NB)
    }

    fn stat(&self) -> io::Result<Metadata> {
        self.fd.metadata()
    }
}

fn same_dir(a: &XLogDir, b: &XLogDir) -> bool {
    use std::os::linux::fs::MetadataExt;
    let inode_eq : io::Result<bool> = try {
        a.stat()?.st_ino() == b.stat()?.st_ino()
    };
    inode_eq.unwrap_or_else(|_| a.dirname == b.dirname)
}

mod xlog_dir_ffi {
    use libc::{c_char, c_int};
    use std::ffi::CStr;
    use std::path::Path;
    use log::warn;
    use super::{XLogDir,same_dir};

    unsafe fn as_path<'a>(path: *const c_char) -> &'a Path {
        Path::new(CStr::from_ptr(path).to_str().unwrap())
    }

    #[no_mangle]
    unsafe extern "C" fn  xlog_dir_new_waldir(dirname: *const c_char) -> *mut XLogDir {
        let dirname = as_path(dirname);
        let dir = XLogDir::new_waldir(dirname).unwrap();
        Box::into_raw(box dir)
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_new_snapdir(dirname: *const c_char) -> *mut XLogDir {
        let dirname = as_path(dirname);
        let dir = XLogDir::new_snapdir(dirname).unwrap();
        Box::into_raw(box dir)
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_free(dir: *mut XLogDir) {
        drop(Box::from_raw(dir));
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_sync(dir: *const XLogDir) -> c_int {
        match (*dir).sync() {
            Ok(()) => 0,
            Err(e) => {
                warn!("sync: {}", e);
                -1
            }
        }
    }
    #[no_mangle]
    unsafe extern "C" fn xlog_dir_lock(dir: *const XLogDir) -> c_int {
        match (*dir).lock() {
            Ok(()) => 0,
            Err(e) => {
                warn!("lock: {}", e);
                -1
            }
        }
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_same_dir(dir_a: *const XLogDir, dir_b: *const XLogDir) -> c_int {
        same_dir(&*dir_a, &*dir_b).into()
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_fd(dir: *const XLogDir) -> c_int {
        use std::os::unix::io::AsRawFd;
        (*dir).fd.as_raw_fd()
    }

}

#[test]
fn test_lock_returns_error_on_lock_failure() {
    let path = Path::new("testdata");
    let a = XLogDir::new_waldir(&path).unwrap();
    let b = XLogDir::new_waldir(&path).unwrap();
    assert!(a.lock().is_ok());
    assert!(b.lock().is_err());
}

#[test]
fn test_sync() {
    let path = Path::new("testdata");
    let a = XLogDir::new_waldir(&path).unwrap();
    assert!(a.sync().is_ok());
}