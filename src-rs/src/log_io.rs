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

use std::fs::{self, File, Metadata};
use std::io::{self, Seek, BufReader, BufWriter, Read, Write as BufWrite, BufRead};
use std::path::{Path, PathBuf};
use std::{env, fmt, mem, slice};
use std::rc::Rc;

use once_cell::sync::Lazy;
use anyhow::{Result, Context, bail};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::file_ext::{FileExt, Fadvice, Flock, SyncFileRange};
use crate::ev;

const DEFAULT_COOKIE : u64 = 0;
const DEFAULT_VERSION : u32 = 12;
const V12 : &'static str = "0.12\n";
const INPROGRESS_SUFFIX : &'static str = ".inprogress";
const MARKER : u32 = 0xba0babed;
const EOF_MARKER : u32 = 0x10adab1e;


#[repr(C, packed)]
union RowAux {
    remote_scn: [u8; 6],
    run_crc: u32,
}

#[repr(C, packed)]
struct Row {
    header_crc32c: u32,
    lsn: i64,
    scn: i64,
    tag: u16,
    shard_id: u16,
    aux: RowAux,
    tm: f64,
    len: u32,
    data_crc32c: u32,
}

#[test]
fn test_row_size() {
    assert_eq!(mem::size_of::<Row>(), 46);
}


// for whatever funny reason, crc32c(0, buf, len) from third_party/crc32.c
// is crc32c::crc32c_append(0xffffffff, &buf) ^ 0xffffffff here
fn crc32c(buf: &[u8]) -> u32 {
    crc32c::crc32c_append(0xffffffff, &buf) ^ 0xffffffff
}

#[test]
fn test_crc32() {
    let buf = [ 0x68, 0x65, 0x6c, 0x6c, 0x6f ];
    assert_eq!(crc32c(&buf), 0xdf03cd79);
}

impl Row {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self as *const _ as *const u8, mem::size_of::<Row>())
        }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self as *mut _ as *mut u8, mem::size_of::<Row>())
        }
    }

    fn crc32c(&self) -> u32 {
        crc32c(&self.as_bytes()[4..])
    }

    fn tag(&self) -> Tag {
        match self.tag & TAG_MASK {
            1 => Tag::SnapInitial,
            2 => Tag::SnapData,
            3 => Tag::WalData,
            4 => Tag::SnapFinal,
            5 => Tag::WalFinal,
            6 => Tag::RunCrc,
            7 => Tag::Nop,
            8 => Tag::RaftAppend,
            9 => Tag::RaftCommit,
            10 => Tag::RaftVote,
            11 => Tag::ShardCreate,
            12 => Tag::ShardAlter,
            13 => Tag::ShardFinal,
            14 => Tag::Tlv,
            t if t < 32 => Tag::SysTag(t as u8),
            t => Tag::UserTag(t as u8),
        }
    }
}

#[derive(PartialEq, Eq)]
enum Tag {
    SnapInitial,
    SnapData,
    WalData,
    SnapFinal,
    WalFinal,
    RunCrc,
    Nop,
    RaftAppend,
    RaftCommit,
    RaftVote,
    ShardCreate,
    ShardAlter,
    ShardFinal,
    Tlv,
    SysTag(u8),
    UserTag(u8),
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tag::SnapInitial =>	write!(f, "snap_initial"),
            Tag::SnapData =>	write!(f, "snap_data"),
            Tag::SnapFinal =>	write!(f, "snap_final"),
            Tag::WalData =>	write!(f, "wal_data"),
            Tag::WalFinal =>	write!(f, "wal_final"),
            Tag::ShardCreate =>	write!(f, "shard_create"),
            Tag::ShardAlter =>	write!(f, "shard_alter"),
            Tag::ShardFinal =>	write!(f, "shard_final"),
            Tag::RunCrc =>	write!(f, "run_crc"),
            Tag::Nop =>		write!(f, "nop"),
            Tag::RaftAppend =>	write!(f, "raft_append"),
            Tag::RaftCommit =>	write!(f, "raft_commit"),
            Tag::RaftVote =>	write!(f, "raft_vote"),
            Tag::Tlv =>		write!(f, "tlv"),
            Tag::SysTag(n) =>	write!(f, "sys{}", n),
            Tag::UserTag(n) =>	write!(f, "usr{}", n)
        }
    }
}


/* two highest bit in tag encode tag type:
   00 - invalid
   01 - snap
   10 - wal
   11 - system wal */

const TAG_MASK: u16 = 0x3fff;
const TAG_SIZE: usize = 14;

enum TagType {
    SNAP = 0x4000,
    WAL = 0x8000,
    SYS = 0xc000,
}

impl fmt::Display for TagType {
    fn fmt(&self, f:  &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TagType::SNAP => write!(f, "snap"),
            TagType::WAL => write!(f, "wal"),
            TagType::SYS => write!(f, "sys"),
        }
    }
}

struct XlogReader {
    io: BufReader<File>,
//    filename: PathBuf, // DUP?
    stat: ev::Stat,

    //     lsn: i64,
    //     last_read_lsn: i64,
}


struct XlogWriter {
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

enum IO {
    Read(XlogReader),
    Write(XlogWriter),
}

struct XLog {
    io: IO,
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
        self.io.write_all(row.as_bytes())?; // FIXME: nasty and unportable
        self.io.write_all(data)?;
        self.wet_rows.push((mem::size_of_val(&MARKER) + row.as_bytes().len() + data.len()) as u32);

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
    fn read_row(&mut self) -> Result<(Row, Box<[u8]>)> {
        let marker = self.io.read_u32::<LittleEndian>().context("reading row_magic")?;

        if MARKER != marker {
            bail!("invalid row marker: expected 0x{:08x}, got 0x{:08x}", MARKER, marker)
        }

        let mut row : Row = unsafe { std::mem::zeroed() }; // meh
        self.io.read_exact(row.as_bytes_mut()).context("reading header")?;

        if row.crc32c() != row.header_crc32c {
            bail!("header crc32c mismatch: expected 0x{:08x}, got 0x{:08x}", {row.header_crc32c}, row.crc32c());
        }

        let mut data = Vec::new();
        data.resize(row.len as usize, 0);
        self.io.read_exact(&mut data).context("reading body")?;
        let data = data.into_boxed_slice();

        if crc32c(&data) != row.data_crc32c {
            bail!("data crc32c mismatch");
        }

        log::debug!("read row LSN:{}", {row.lsn});

        Ok((row, data))
    }
}


impl XLog {
    fn name(filename: &Path) -> Result<Self> {
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



// TODO: switch to byteordered?
struct LittleEndianReader<'a> (std::io::Cursor<&'a[u8]>);
impl<'a> LittleEndianReader<'a> {
    fn new(buf: &'a[u8]) -> Self { Self(std::io::Cursor::new(buf)) }
    fn read_u8(&mut self) -> u8 { self.0.read_u8().unwrap() }
    fn read_u16(&mut self) -> u16 { self.0.read_u16::<LittleEndian>().unwrap() }
    fn read_u32(&mut self) -> u32 { self.0.read_u32::<LittleEndian>().unwrap() }
    fn read_i64(&mut self) -> i64 { self.0.read_i64::<LittleEndian>().unwrap() }
    fn read_u64(&mut self) -> u64 { self.0.read_u64::<LittleEndian>().unwrap() }
}

fn print_row(buf: &mut dyn std::fmt::Write, row: &Row, row_data: &[u8], handler: fn(buf: &mut dyn std::fmt::Write, data: &[u8])) {
    // let row_data = unsafe { slice::from_raw_parts(data, row.len as usize) };

    fn int_flag(name: &str) -> Option<usize> {
        let val = env::var(name).ok()?;
        val.parse().ok()
    }
    static PRINT_HEADER : Lazy<bool> = Lazy::new(|| { int_flag("OCTOPUS_CAT_ROW_HEADER") == Some(1) });
    static PRINT_RUN_CRC : Lazy<bool> = Lazy::new(|| { int_flag("OCTOPUS_CAT_RUN_CRC") == Some(1) });

    let tag = row.tag();

    if *PRINT_HEADER {
        write!(buf, "lsn:{}", {row.lsn}).unwrap();
        if row.scn != -1 || tag == Tag::RaftVote || tag == Tag::SnapData {
            write!(buf, " shard:{}", {row.shard_id}).unwrap();

            if *PRINT_RUN_CRC {
                write!(buf, " run_crc:{:#08x}", unsafe {row.aux.run_crc}).unwrap();
            }
        }

        write!(buf, " scn:{:#08x} tm:{:.3} t:{} ", {row.scn}, {row.tm}, row.tag()).unwrap();
    }

    use mem::size_of;
    let mut reader = LittleEndianReader::new(row_data);
    match row.tag() {
        Tag::SnapInitial => {
            if row_data.len() == size_of::<u32>() * 3 {
                let count = reader.read_u32();
                let crc_log = reader.read_u32();
                let crc_mod = reader.read_u32();
                write!(buf, "count:{} run_crc_log:0x{:#08x} run_crc_mod:0x{:#08x}",
		       count, crc_log, crc_mod).unwrap();
            } else if row.scn == -1 {
                let ver = reader.read_u8();
		let count = reader.read_u32();
		let flags = reader.read_u32();
		write!(buf, "ver:{} count:{} flags:0x{:#08x}", ver, count, flags).unwrap();
	    } else {
		write!(buf, "unknow format").unwrap();
            }
        },
        Tag::RunCrc => {
	    let mut scn = -1;
	    if row_data.len() == size_of::<i64>() + 2 * size_of::<u32>() {
		scn = reader.read_i64();
            }
	    let crc_log = reader.read_u32();
	    let _ = reader.read_u32(); /* ignore run_crc_mod */
	    write!(buf, "SCN:{} log:0x{:#08x}", scn, crc_log).unwrap();
        },

        Tag::SnapData | Tag::WalData | Tag::UserTag(_) => handler(buf, row_data),
        Tag::SnapFinal | Tag::WalFinal | Tag::Nop => (),
        Tag::SysTag(_) => (),
        Tag::RaftAppend | Tag::RaftCommit | Tag::RaftVote => todo!(),
        Tag::ShardCreate | Tag::ShardAlter | Tag::ShardFinal => todo!(),
        Tag::Tlv => todo!(),
    }
}

// #[test]
fn test_print_row() {
    println!("current dir {:?}", env::current_dir().unwrap());
    let mut xlog = XLog::name(Path::new("testdata/00000000000000000002.xlog")).unwrap();

    let mut buf = String::new();

    fn hexdump(buf: &mut dyn std::fmt::Write, data: &[u8]) {
        for b in data {
            write!(buf, "{:02x} ", b).unwrap();
        }
    }

    if let IO::Read(reader) = &mut xlog.io {
        loop {
            match reader.read_row() {
                Ok((row, data)) => {
                    buf.clear();
                    print_row(&mut buf, &row, &data, hexdump);
                    println!("{}", buf);
                },
                Err(err) => {
                    println!("{:?}", err);
                    break;
                }
            }
        }
    }
}

fn read_headers(reader: &mut BufReader<File>) -> io::Result<Vec<String>> {
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

struct XLogDir {
    fd: File,
    #[allow(dead_code)]
    filetype: &'static str,
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
    fn new_dummy() -> io::Result<Self> {
        Ok(Self {
            fd: File::open("/dev/null")?,
            filetype: "DUMMY\n",
            suffix: "dummy",
            dirname: "".into(),
            objc_dir: 0 as _
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
