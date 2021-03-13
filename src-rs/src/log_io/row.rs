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

use std::{alloc::{alloc, dealloc, Layout}, env, fmt, mem, slice, io, io::Read, ops::{Deref, DerefMut}};

use once_cell::sync::Lazy;
use byteorder::{LittleEndian, ReadBytesExt};
use anyhow::{bail, Context, Result};

use super::*;

#[repr(C, packed)]
pub union RowAux {
    pub remote_scn: [u8; 6],
    pub run_crc: u32,
}

extern {
    type RowData;
}

#[repr(C, packed)]
pub struct Row {
    pub header_crc32c: u32,
    pub lsn: i64,
    pub scn: i64,
    pub tag: u16,
    shard_id: u16,
    aux: RowAux,
    tm: f64,
    pub len: u32,
    pub data_crc32c: u32,
    _data: RowData,
}

const ROW_LAYOUT : Layout = unsafe { Layout::from_size_align_unchecked(46, 16) };


pub struct BoxRow {
    ptr: *mut Row
}

impl Deref for BoxRow {
    type Target = Row;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl DerefMut for BoxRow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr }
    }
}

impl Drop for BoxRow {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr as *mut _, Row::layout(self.len));
        }
    }
}

impl Row {
    fn layout(len: u32) -> Layout {
        assert!(len < 2<<10);
        let data = Layout::from_size_align(len as usize, 1).unwrap();
        ROW_LAYOUT.extend_packed(data).unwrap()
    }

    fn alloc(len: u32) -> *mut Row {
        unsafe {
            let ptr = alloc(Self::layout(len)) as *mut Row;
            (*ptr).len = len;
            ptr
        }
    }

    fn data_ptr(&self) -> *const u8 {
        let ptr = self as *const _ as *const u8;
        unsafe { ptr.add(ROW_LAYOUT.size()) }
    }

    pub fn data(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.data_ptr(), self.len as usize)
        }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.data_ptr() as *mut _, self.len as usize)
        }
    }

    pub fn read(io: &mut dyn Read) -> Result<BoxRow> {
        let mut header =  [0; ROW_LAYOUT.size()];
        io.read_exact(&mut header).context("reading header")?;

        let header_crc32c = (&header[0..4]).read_u32::<LittleEndian>().unwrap();
        let header_crc32c_calculated = crc32c(&header[4..]);
        if header_crc32c_calculated != header_crc32c {
            bail!("header crc32c mismatch: expected 0x{:08x}, calculated 0x{:08x}",
                  header_crc32c, header_crc32c_calculated);
        }

        let len = (&header[ROW_LAYOUT.size() - 8..]).read_u32::<LittleEndian>().unwrap();
        let mut row = BoxRow { ptr: Self::alloc(len) };

        row.as_bytes_mut().copy_from_slice(&header);
        debug_assert!(row.len == len);
        io.read_exact(row.data_mut()).context("reading body")?;

        if crc32c(row.data()) != row.data_crc32c {
            bail!("data crc32c mismatch: expected 0x{:08x}, calculated 0x{:08x}",
                  {row.data_crc32c}, crc32c(row.data()));
        }

        log::debug!("read row LSN:{}", {row.lsn});
        Ok(row)
    }

    pub fn write(&self, io: &mut dyn io::Write) -> io::Result<usize> {
        io.write_all(self.as_bytes())?; // FIXME: nasty and unportable
        io.write_all(self.data())?;
        Ok(ROW_LAYOUT.size() + self.data().len())
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self as *const _ as *const u8, ROW_LAYOUT.size())
        }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self as *mut _ as *mut u8, ROW_LAYOUT.size())
        }
    }

    pub fn update_crc(&mut self) {
        self.data_crc32c = crc32c(self.data());
        self.header_crc32c = crc32c(&self.as_bytes()[4..])
    }


    fn tag(&self) -> Tag {
        Tag::new(self.tag & TAG_MASK)
    }

    fn tag_type(&self) -> TagType {
        TagType::new(self.tag & !TAG_MASK)
    }
}

#[derive(Debug)]
#[derive(PartialEq, Eq)]
pub enum Tag {
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

impl Tag {
    fn new(repr: u16) -> Self {
        match repr & TAG_MASK {
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
            t => Tag::UserTag((t >> 5) as u8),
        }
    }

    fn as_u16(&self) -> u16 {
        match self {
            Tag::SnapInitial => 1,
            Tag::SnapData => 2,
            Tag::WalData => 3,
            Tag::SnapFinal => 4,
            Tag::WalFinal => 5,
            Tag::RunCrc => 6,
            Tag::Nop => 7,
            Tag::RaftAppend => 8,
            Tag::RaftCommit => 9,
            Tag::RaftVote => 10,
            Tag::ShardCreate => 11,
            Tag::ShardAlter => 12,
            Tag::ShardFinal => 13,
            Tag::Tlv => 14,
            Tag::SysTag(t) => *t as u16,
            Tag::UserTag(t) => *t as u16,
        }
    }
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

pub const TAG_MASK: u16 = 0x3fff;
const TAG_SIZE: usize = 14;

enum TagType {
    SNAP = 0x4000,
    WAL = 0x8000,
    SYS = 0xc000,
    INVALID = 0,
}

impl TagType {
    fn new(repr: u16) -> TagType {
        match repr & !TAG_MASK {
            0x4000 => TagType::SNAP,
            0x8000 => TagType::WAL,
            0xc000 => TagType::SYS,
            _ => TagType::INVALID,
        }
    }
}

impl fmt::Display for TagType {
    fn fmt(&self, f:  &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TagType::SNAP => write!(f, "snap"),
            TagType::WAL => write!(f, "wal"),
            TagType::SYS => write!(f, "sys"),
            TagType::INVALID => write!(f, "invalid"),
        }
    }
}

#[derive(PartialEq, Eq)]
enum ShardType {
    POR,
    RAFT,
    PART
}

impl ShardType {
    fn new(repr: u8) -> Result<Self> {
        match repr {
            0 => Ok(ShardType::POR),
            1 => Ok(ShardType::RAFT),
            2 => Ok(ShardType::PART),
            _ => bail!("invalid shard type {}", repr)
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
    fn read_str(&mut self, len: usize) -> &str {
        let pos = self.0.position() as usize;
        let raw = &self.0.get_ref()[pos..pos+len];
        let (raw, _) = raw.split_at(raw.iter().position(|&x| x == 0).unwrap_or(len));
        let str = std::str::from_utf8(raw).unwrap();
        self.0.set_position((pos+len) as u64);
        str
    }
    fn into_cursor(self) -> std::io::Cursor<&'a[u8]> { self.0 }
}

pub fn print_row(buf: &mut dyn std::fmt::Write, row: &Row,
                 handler: fn(buf: &mut dyn std::fmt::Write, tag: Tag, data: &[u8])) -> Result<()> {

    fn int_flag(name: &str) -> Option<usize> {
        let val = env::var(name).ok()?;
        val.parse().ok()
    }
    static PRINT_HEADER : Lazy<bool> = Lazy::new(|| { int_flag("OCTOPUS_CAT_ROW_HEADER") == Some(1) });
    static PRINT_RUN_CRC : Lazy<bool> = Lazy::new(|| { int_flag("OCTOPUS_CAT_RUN_CRC") == Some(1) });

    let tag = row.tag();

    if *PRINT_HEADER {
        write!(buf, "lsn:{}", {row.lsn})?;
        if row.scn != -1 || tag == Tag::RaftVote || tag == Tag::SnapData {
            write!(buf, " shard:{}", {row.shard_id})?;

            if *PRINT_RUN_CRC {
                write!(buf, " run_crc:{:#08x}", unsafe {row.aux.run_crc})?;
            }
        }

        write!(buf, " scn:{} tm:{:.3} t:{} ", {row.scn}, {row.tm}, row.tag())?;
    }

    use mem::size_of;
    let mut reader = LittleEndianReader::new(row.data());
    match row.tag() {
        Tag::SnapInitial => {
            if row.data().len() == size_of::<u32>() * 3 {
                let count = reader.read_u32();
                let crc_log = reader.read_u32();
                let crc_mod = reader.read_u32();
                write!(buf, "count:{} run_crc_log:0x{:#08x} run_crc_mod:0x{:#08x}",
		       count, crc_log, crc_mod)?;
            } else if row.scn == -1 {
                let ver = reader.read_u8();
		let count = reader.read_u32();
		let flags = reader.read_u32();
		write!(buf, "ver:{} count:{} flags:0x{:#08x}", ver, count, flags)?;
	    } else {
		write!(buf, "unknow format")?;
            }
        },
        Tag::RunCrc => {
	    let mut scn = -1;
	    if row.data().len() == size_of::<i64>() + 2 * size_of::<u32>() {
		scn = reader.read_i64();
            }
	    let crc_log = reader.read_u32();
	    let _ = reader.read_u32(); /* ignore run_crc_mod */
	    write!(buf, "SCN:{} log:0x{:#08x}", scn, crc_log)?;
        },

        Tag::SnapData | Tag::WalData | Tag::UserTag(_) => handler(buf, tag, row.data()),
        Tag::SnapFinal | Tag::WalFinal | Tag::Nop => (),
        Tag::SysTag(_) => (),
        Tag::RaftAppend | Tag::RaftCommit => {
            let flags = reader.read_u16();
            let term = reader.read_u64();
            let inner_tag = Tag::new(reader.read_u16());
            write!(buf, "term:{} flags:0x{:#02x} it:{} ", flags, term, inner_tag)?;
            match inner_tag {
                Tag::RunCrc => {
                    let scn = reader.read_u64();
                    let log = reader.read_u32();
                    let _ = reader.read_u32(); /* ignore run_crc_mod */
                    write!(buf, "SCN:{} log:0x{:#08x}", scn, log)?;
                },
                Tag::Nop => (),
                _ => {
                    handler(buf, inner_tag, reader.into_cursor().into_inner());
                    return Ok(())
                }
            }
        },
        Tag::RaftVote => {
            let flags = reader.read_u16();
            let term = reader.read_u64();
            let peer_id = reader.read_u8();
            write!(buf, "term:{} flags:0x{:#02x} peer:{}", term, flags, peer_id)?;
        },
        Tag::ShardCreate | Tag::ShardAlter => {
            let ver = reader.read_u8();
	    if ver != 1 {
		bail!("unknow version: {}", ver);
	    }

	    let shard_type = ShardType::new(reader.read_u8())?;
	    let estimated_row_count = reader.read_u32();

            match row.tag() {
                Tag::ShardCreate => write!(buf,"SHARD_CREATE")?,
                Tag::ShardAlter => write!(buf, "SHARD_ALTER")?,
                _ => unreachable!(),
            }

	    write!(buf, " shard_id:{}", {row.shard_id})?;

	    match shard_type {
	        ShardType::RAFT => write!(buf, " RAFT")?,
	        ShardType::POR => write!(buf, " POR")?,
	        ShardType::PART => write!(buf, " PART")?,
	    }

	    let mod_name = reader.read_str(16);
            write!(buf, " {}", mod_name)?;

	    write!(buf, " count:{} run_crc:0x{:#08x}", estimated_row_count, unsafe { row.aux.run_crc })?;

	    write!(buf, " master:{}", reader.read_str(16))?;
            for _ in 0..4 {
                let peer_name = reader.read_str(16);
                if peer_name.len() > 0 {
                    write!(buf, " repl:{}", peer_name)?;
                }
            }
	    let aux_len = reader.read_u16();
            write!(buf, "aux:")?;
            for _ in 0..aux_len {
                let b = reader.read_u8();
                write!(buf, "{:#02} ", b)?;
            }
        },
        Tag::ShardFinal => (),
        Tag::Tlv => todo!(),

        // tag => handler(buf, tag, row.data())
    }
    // let cursor = reader.into_cursor();
    // println!("pos: {}, data len: {}", cursor.position(), cursor.get_ref().len());
    // assert!(.len() == 0);
    Ok(())
}

#[test]
fn test_print_row() {
    use std::path::Path;
    println!("current dir {:?}", env::current_dir().unwrap());
    let mut xlog = XLog::name(Path::new("testdata/00000000000000000002.xlog")).unwrap();

    let mut buf = String::new();

    env::set_var("OCTOPUS_CAT_ROW_HEADER", "1");

    fn hexdump(buf: &mut dyn std::fmt::Write, tag: Tag, data: &[u8]) {
        write!(buf, "tag:{}", tag).unwrap();
        for b in data {
            write!(buf, " {:02x}", b).unwrap();
        }
    }

    if let IO::Read(reader) = &mut xlog.io {
        loop {
            match reader.read_row() {
                Ok(Some(row)) => {
                    buf.clear();
                    print_row(&mut buf, &row, hexdump).unwrap();
                    println!("row {}", buf);
                },
                Ok(None) => break,
                Err(err) => {
                    println!("fail {:?}", err);
                    break;
                }
            }
        }
    }
}
