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

use std::{env, fmt, mem, slice};

use once_cell::sync::Lazy;
use byteorder::{LittleEndian, ReadBytesExt};

use super::*;

#[repr(C, packed)]
pub union RowAux {
    pub remote_scn: [u8; 6],
    pub run_crc: u32,
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
}

#[test]
fn test_row_size() {
    assert_eq!(mem::size_of::<Row>(), 46);
}

impl Row {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self as *const _ as *const u8, mem::size_of::<Row>())
        }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self as *mut _ as *mut u8, mem::size_of::<Row>())
        }
    }

    pub fn crc32c(&self) -> u32 {
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

pub fn print_row(buf: &mut dyn std::fmt::Write, row: &Row, row_data: &[u8], handler: fn(buf: &mut dyn std::fmt::Write, data: &[u8])) {
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
// fn test_print_row() {
//     use std::path::Path;
//     println!("current dir {:?}", env::current_dir().unwrap());
//     let mut xlog = XLog::name(Path::new("testdata/00000000000000000002.xlog")).unwrap();

//     let mut buf = String::new();

//     fn hexdump(buf: &mut dyn std::fmt::Write, data: &[u8]) {
//         for b in data {
//             write!(buf, "{:02x} ", b).unwrap();
//         }
//     }

//     if let IO::Read(reader) = &mut xlog.io {
//         loop {
//             match reader.read_row() {
//                 Ok((row, data)) => {
//                     buf.clear();
//                     print_row(&mut buf, &row, &data, hexdump);
//                     println!("{}", buf);
//                 },
//                 Err(err) => {
//                     println!("{:?}", err);
//                     break;
//                 }
//             }
//         }
//     }
// }
