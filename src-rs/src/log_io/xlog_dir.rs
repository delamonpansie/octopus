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

use std::fs::{self, File, Metadata};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use crate::file_ext::{FileExt, Flock};
use super::*;

fn find(list: &[(i64, PathBuf)], key: i64) -> Option<&(i64, PathBuf)> {
    for w in list.windows(2) {
        if w[0].0 <= key && key < w[1].0 {
            return Some(&w[0])
        }
    }
    return list.last()
}

extern {
    type XLogObjc;
    type XLogDirObjc;
}

pub struct XLogDir {
    fd: File,
    #[allow(dead_code)]
    pub filetype: &'static str,
    suffix: &'static str,
    dirname: PathBuf,
    #[allow(dead_code)]
    objc_dir: *const XLogDirObjc,
}

impl XLogDir {
    fn new_waldir(path: &Path, objc_dir: *const XLogDirObjc) -> io::Result<Self> {
        Ok(Self {
            fd: File::open(path)?,
            filetype: "XLOG\n",
            suffix: "xlog",
            dirname: path.into(),
            objc_dir,
        })
    }

    fn new_snapdir(path: &Path, objc_dir: *const XLogDirObjc) -> io::Result<Self> {
        Ok(Self {
            fd: File::open(path)?,
            filetype: "SNAP\n",
            suffix: "snap",
            dirname: path.into(),
            objc_dir,
        })
    }
    pub fn new_dummy() -> io::Result<Self> {
        Ok(Self {
            fd: File::open("/dev/null")?,
            filetype: "DUMMY\n",
            suffix: "dummy",
            dirname: "".into(),
            objc_dir: 0 as _
        })
    }

    pub fn sync(&self) -> io::Result<()> {
        self.fd.sync_all()
    }

    fn lock(&self) -> io::Result<()> {
        self.fd.flock(Flock::EX|Flock::NB)
    }

    fn stat(&self) -> io::Result<Metadata> {
        self.fd.metadata()
    }

    fn scan_dir(&self) -> io::Result<Vec<(i64, PathBuf)>> {
        let parse = |name: &PathBuf| -> Option<i64> {
            let mut it = name.to_str()?.splitn(2, '.');
            let lsn = it.next()?;
            let suffix = it.next()?;

            if suffix != self.suffix {
                return None
            }
            match lsn.parse() {
                Ok(lsn) => Some(lsn),
                Err(err) => {
                    log::warn!("skip {:#?}, can't parse `{}': {}", name, lsn, err);
                    None
                }
            }
        };

        let mut ret = Vec::new();
        for entry in fs::read_dir(&self.dirname)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue
            }

            let file_name = entry.file_name().into();
            if let Some(lsn) = parse(&file_name) {
                ret.push((lsn, file_name))
            }
        }

        ret.sort();
        Ok(ret)
    }

    fn scan_dir_scn(&self, shard_id: i32) -> io::Result<Vec<(i64, PathBuf)>> {
        let parse = |file_name: &PathBuf| -> io::Result<Option<i64>> {
            let file_name = self.dirname.join(file_name);
            let file = File::open(file_name)?;
            let mut reader = BufReader::with_capacity(4<<10, file);
            for line in read_headers(&mut reader)? {
                if ! line.starts_with("SCN-") {
                    continue
                }
                match scan_fmt::scan_fmt!(&line, "SCN-{d}: {d}", i32, i64) {
                    Ok((id, scn)) if id == shard_id => return Ok(Some(scn)),
                    Ok(_) => (),
                    Err(err) => log::warn!("failed to parse SCN header {}: {}", line, err)
                }
            }
            Ok(None)
        };

        let mut ret = Vec::new();
        let files = self.scan_dir()?;
        let last = files.last().cloned();
        for (lsn, file_name) in files {
            if let Some(_) = parse(&file_name)? {
                ret.push((lsn, file_name));
            }
        }
        if ret.len() == 0 {
            if let Some((lsn, file_name)) = last {
                ret.push((lsn, file_name));
            }
        }
        Ok(ret)
    }

    fn greatest_lsn(&self) -> io::Result<Option<i64>> {
        let files = self.scan_dir()?;
        Ok(files.last().map(|(lsn, _)| *lsn))
    }

    fn find_with_lsn(&self, lsn: i64) -> io::Result<Option<(i64, PathBuf)>> {
        let files = self.scan_dir()?;
        Ok(find(&files, lsn).cloned())
    }

    fn find_with_scn(&self, shard_id:i32, scn: i64) -> io::Result<Option<(i64, PathBuf)>> {
        let files = self.scan_dir_scn(shard_id)?;
        Ok(find(&files, scn).cloned())
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
    use std::io;
    use std::ffi::{CStr, CString};
    use std::path::{Path, PathBuf};
    use log::warn;
    use super::{XLogObjc, XLogDir, XLogDirObjc, same_dir};

    unsafe fn as_path<'a>(path: *const c_char) -> &'a Path {
        Path::new(CStr::from_ptr(path).to_str().unwrap())
    }

    #[no_mangle]
    unsafe extern "C" fn  xlog_dir_new_waldir(dirname: *const c_char, objc_dir: *const XLogDirObjc) -> *mut XLogDir {
        let dirname = as_path(dirname);
        let dir = XLogDir::new_waldir(dirname, objc_dir).unwrap();
        Box::into_raw(box dir)
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_new_snapdir(dirname: *const c_char, objc_dir: *const XLogDirObjc) -> *mut XLogDir {
        let dirname = as_path(dirname);
        let dir = XLogDir::new_snapdir(dirname, objc_dir).unwrap();
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

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_greatest_lsn(dir: *const XLogDir) -> i64 {
        match (*dir).greatest_lsn() {
            Ok(None) => 0,
            Ok(Some(lsn)) => lsn,
            Err(e) => {
                warn!("greatest_lsn: {}", e);
                -1
            }
        }
    }

    fn open_for_read(caller: &str, dir: *const XLogDirObjc, find: &dyn Fn() -> io::Result<Option<(i64, PathBuf)>>) -> *mut XLogObjc {
        extern {
            fn xlog_dir_open_for_read(dir: *const XLogDirObjc , lsn: i64, filename: *const c_char) -> *mut XLogObjc;
        }

        match find() {
            Ok(None) => 0 as _,
            Ok(Some((file_lsn, file_name))) => {
                use std::os::unix::ffi::OsStrExt;
                let file_name = CString::new(file_name.as_os_str().as_bytes()).unwrap();
                unsafe { xlog_dir_open_for_read(dir, file_lsn, file_name.as_ptr()) }
            }
            Err(e) => {
                warn!("{}: {}", caller, e);
                0 as _
            }
        }
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_find_with_lsn(dir: *const XLogDir, lsn: i64) -> *mut XLogObjc {
        open_for_read("find_with_lsn", (*dir).objc_dir, &|| (*dir).find_with_lsn(lsn))
    }

    #[no_mangle]
    unsafe extern "C" fn xlog_dir_find_with_scn(dir: *const XLogDir, shard_id: i32, scn: i64) -> *mut XLogObjc {
        open_for_read("find_with_scn", (*dir).objc_dir, &|| (*dir).find_with_scn(shard_id, scn))
    }
}

#[cfg(test)]
mod xlog_dir_tests {
    use super::*;
    use std::io::Write;
    use goldenfile::Mint;

    #[allow(non_upper_case_globals)]
    const objc_dir : *const XLogDirObjc = 0usize as _;

    #[test]
    fn test_lock_returns_error_on_lock_failure() {
        let path = Path::new("testdata");
        let a = XLogDir::new_waldir(&path, objc_dir).unwrap();
        let b = XLogDir::new_waldir(&path, objc_dir).unwrap();
        assert!(a.lock().is_ok());
        assert!(b.lock().is_err());
    }

    #[test]
    fn test_sync() {
        let path = Path::new("testdata");
        let a = XLogDir::new_waldir(&path, objc_dir).unwrap();
        assert!(a.sync().is_ok());
    }

    #[test]
    fn test_scan_dir() {
        let mut mint = Mint::new("testdata/golden");
        let mut file = mint.new_goldenfile("scan_dir.txt").unwrap();

        let path = Path::new("testdata");
        let a = XLogDir::new_waldir(&path, objc_dir).unwrap();
        write!(file, "{:?}", a.scan_dir()).unwrap();
    }

    #[test]
    fn test_scan_dir_scn() {
        let mut mint = Mint::new("testdata/golden");
        let mut file1 = mint.new_goldenfile("scan_dir_scn1.txt").unwrap();
        let mut file2 = mint.new_goldenfile("scan_dir_scn2.txt").unwrap();

        let path = Path::new("testdata");
        let a = XLogDir::new_waldir(&path, objc_dir).unwrap();
        write!(file1, "{:?}", a.scan_dir_scn(1)).unwrap();
        write!(file2, "{:?}", a.scan_dir_scn(2)).unwrap();
    }

    #[test]
    fn test_greatest_lsn() {
        let path = Path::new("testdata");
        let a = XLogDir::new_waldir(&path, objc_dir).unwrap();
        assert_eq!(a.greatest_lsn().unwrap(), Some(150));
    }
}
