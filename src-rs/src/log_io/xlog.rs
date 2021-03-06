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

#![allow(dead_code)]

use std::fs::{self, File};
use std::io::{self, Seek, BufReader, BufWriter, Write as BufWrite, BufRead};
use std::path::{Path, PathBuf};
use std::mem;
use std::rc::Rc;

use anyhow::{Result, Context, bail};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::file_ext::{FileExt, Fadvice, SyncFileRange};
use crate::ev;

use super::*;

const DEFAULT_COOKIE : u64 = 0;
const DEFAULT_VERSION : u32 = 12;
const V12 : &'static str = "0.12\n";
const INPROGRESS_SUFFIX : &'static str = ".inprogress";
const MARKER : u32 = 0xba0babed;
const EOF_MARKER : u32 = 0x10adab1e;

pub fn read_headers(reader: &mut BufReader<File>) -> io::Result<Vec<String>> {
    let mut vec = Vec::new();
    loop {
        let mut buf = String::new();
        if reader.read_line(&mut buf)? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF while reading headers"))
        }
        if buf == "\n" || buf == "\r\n" {
            return Ok(vec)
        }
        vec.push(buf);
    }
}

pub struct XlogReader {
    io: BufReader<File>,
//    filename: PathBuf, // DUP?
    stat: ev::Stat,

    //     lsn: i64,
    //     last_read_lsn: i64,
}


pub struct XlogWriter {
    io: BufWriter<File>,
    header_written: bool,
    inprogress: bool,
    truncate_before_write: bool,
    tag_mask: u16,

    next_lsn: i64,
    offset: u64,
    sync_offset: u64,
    wet_rows: Vec<u32>,
}

pub enum IO {
    Read(XlogReader),
    Write(XlogWriter),
}

pub struct XLog {
    pub io: IO,
    filename: PathBuf,
    dir: Rc<XLogDir>,

    headers: Vec<String>,
    rows: usize,

    eof: bool,
}


impl XlogWriter {
    fn write_eof_marker(&mut self) -> io::Result<()> {
        // TODO: invalidate self somehow?
        self.io.write_u32::<LittleEndian>(EOF_MARKER)?;
        self.flush()
    }

    fn flush(&mut self) -> io::Result<()> {
        let rc = self.io.flush();
        if rc.is_err() {
            assert!(self.wet_rows.len() == 0);
            return rc;
        }
        self.io.get_mut().sync_all()
    }

    fn fadvise_dont_need(&mut self) {
        let file = self.io.get_mut();
        let mut end = file.stream_len().unwrap_or(0);
        if end > 128<<10 + 4094 {
            end -= 128<<10 + end % 4096;
            let rc = file.fadvise(0, end as i64, Fadvice::DONTNEED);
            if let Err(err) = rc {
                log::warn!("fadvise: {}", err);
            }
        }
    }

    fn confirm_write(&mut self) -> i64 {
        assert!(self.truncate_before_write == false);

        if self.wet_rows.len() == 0 {
            return self.next_lsn - 1;
        }

        if let Err(flush_err) = self.io.flush() {
            log::warn!("flush: {}", flush_err);
            self.truncate_before_write = true;

            // If stat() failed it's wrong to assume that all rows are
            // failed: if any of rows have been written to disk, then
            // restart without XLog truncation will resurrect them
            let flushed_offset = self.io.get_mut().stream_len().unwrap();

            let mut wet_len = 0;
            for (i, len) in self.wet_rows.iter().enumerate() {
                wet_len += *len as u64;
                if self.offset + wet_len > flushed_offset {
                    self.wet_rows.truncate(i);
                    break
                }
            }
        }

        // self.rows += self.wet_rows.len();
        self.next_lsn += self.wet_rows.len() as i64;
        self.offset += self.wet_rows.drain(..).sum::<u32>() as u64;

        if self.offset -  self.sync_offset > 32 * 4096 {
            let _ = self.io.get_ref().sync_file_range(self.sync_offset as i64, 0, SyncFileRange::WRITE);
            self.sync_offset = self.offset;
        }

        self.next_lsn - 1
    }


    fn write_header(&mut self, dir: &XLogDir) -> io::Result<()> {
        self.io.write_all(dir.filetype.as_bytes())?;
        self.io.write_all(V12.as_bytes())?;
        write!(self.io, "Created-by: octopus\n")?;
        let version = unsafe {
            extern { fn octopus_version() -> *const libc::c_char; }
            std::ffi::CStr::from_ptr(octopus_version()).to_str().unwrap()
        };
	write!(self.io, "Octopus-version: {}\n", version)?;
        write!(self.io, "\n")
    }

    fn append_row(&mut self, row: &mut Row, data: &[u8]) -> io::Result<()> {
        // TODO
        // if self.truncate_before_write {
        //     self.io.seek(self.offset)?;
        //     self.io.get_mut().set_len(self.offset)?;
        //     self.truncate_before_write = false;
        // }

        if (row.tag & !TAG_MASK) == 0 {
            row.tag |= self.tag_mask
        }
        row.lsn = self.next_lsn + self.wet_rows.len() as i64;
        if row.scn == 0 {
            row.scn = row.lsn;
        }
        row.data_crc32c = crc32c(data);
        row.header_crc32c = row.crc32c();

        self.io.write_u32::<LittleEndian>(MARKER)?;
        row.write(&mut self.io)?;
        self.io.write_all(data)?;
        self.wet_rows.push((mem::size_of_val(&MARKER) + mem::size_of_val(&row) + data.len()) as u32);

        Ok(())
    }
}

impl XlogReader {
    fn new(io: BufReader<File>) -> Self {
        Self {
            io,
            stat: ev::Stat::new(),
        }
    }

    pub fn follow(&mut self, filename: &Path, cb: Option<extern "C" fn(*mut ev::Stat, libc::c_int)>, data: *mut libc::c_void) {
        if self.stat.is_active() {
            return
        }
        match cb {
            None => self.stat.stop(),
            Some(cb) => {
                let wal_dir_rescan_delay = 5.0; // FIXME: use cfg
                self.stat.init(cb, filename, wal_dir_rescan_delay / 10.0);
                self.stat.data = data;
                self.stat.start();
            }
        }
    }


    #[allow(deprecated)]
    pub fn read_row(&mut self) -> Result<(Row, Box<[u8]>)> {
        let marker = self.io.read_u32::<LittleEndian>().context("reading row_magic")?;

        if MARKER != marker {
            bail!("invalid row marker: expected 0x{:08x}, got 0x{:08x}", MARKER, marker)
        }

        Row::read(&mut self.io)
    }
}


impl XLog {
    pub fn name(filename: &Path) -> Result<Self> {
        let file = File::open(filename)?;
        let mut reader = BufReader::with_capacity(64<<10, file);

        let mut buf = String::new();
        let n = reader.read_line(&mut buf).context("reading filetype")?;
        if n == 0 {
            bail!("unexpected EOF");
        }

        if buf != "XLOG\n" {
            bail!("invalid filetype")
        }

        buf.clear();
        let n = reader.read_line(&mut buf).context("reading version")?;
        if n == 0 {
            bail!("unexpected EOF");
        }

        if buf != V12 {
            bail!("invalid version")
        }

        let headers = read_headers(&mut reader).context("reading headers")?;

        Ok(Self {
            io: IO::Read(XlogReader::new(reader)),
            filename: filename.into(),
            dir: Rc::new(XLogDir::new_dummy()?),

            headers: headers,
            rows: 0,

            eof: false,
        })

    }


    fn inprogress_rename(&mut self) -> io::Result<()> {
        // assert!(self.inprogress);
        assert!(self.filename.extension().unwrap() == INPROGRESS_SUFFIX);

        let mut new_filename = self.filename.clone();
        new_filename.set_extension("");

        fs::rename(&self.filename, &new_filename)?;
        //self.inprogress = false;
        self.filename = new_filename;

        if let Err(err) = self.dir.sync() {
            log::warn!("failed to sync dir: {}", err);
        }
        Ok(())
    }
}

impl Drop for XLog {
    fn drop(&mut self) {
        match &mut self.io {
            IO::Read(_) => {
                // if self.rows == 0 && File::open(&self.filename).is_ok() {
                //     panic!("no valid rows were read")
                // }

            },
            IO::Write(io) => {
                let rc = io.write_eof_marker();
                if let Err(err) = rc {
                    log::warn!("write_eof_marker: {}", err);
                }
            }
        }
    }

}
