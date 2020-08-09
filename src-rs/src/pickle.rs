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

use std::convert::TryInto;
use std::error;
use std::fmt;
use std::result;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Error {
    Short,
    Overflow,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Short => write!(f, "data is too short"),
            Error::Overflow => write!(f, "overflow decoding varint"),
        }
    }
}
impl error::Error for Error {}

pub type Result<T> = result::Result<T, Error>;

trait Pickle {
    fn read_u8(&mut self) -> Result<u8>;
    fn read_u16(&mut self) -> Result<u16>;
    fn read_u32(&mut self) -> Result<u32>;
    fn read_v32(&mut self) -> Result<u32>;
}

impl Pickle for &[u8] {
    fn read_u8(&mut self) -> Result<u8> {
        if self.len() >= 1 {
            let v = self[0] as u8;
            *self = &self[1..];
            Ok(v)
        } else {
            Err(Error::Short)
        }
    }
    fn read_u16(&mut self) -> Result<u16> {
        if self.len() >= 2 {
            let (hd, tl) = self.split_at(2);
            let v = u16::from_le_bytes(hd.try_into().unwrap());
            *self = tl;
            Ok(v)
        } else {
            Err(Error::Short)
        }
    }
    fn read_u32(&mut self) -> Result<u32> {
        if self.len() >= 4 {
            let (hd, tl) = self.split_at(4);
            let v = u32::from_le_bytes(hd.try_into().unwrap());
            *self = tl;
            Ok(v)
        } else {
            Err(Error::Short)
        }
    }
    fn read_v32(&mut self) -> Result<u32> {
        let mut v = 0;
        let mut iter = self.iter();
        let mut i = 5; // at most 5 bytes
        while let Some(&b) = iter.next() {
            let bits = (b & 0x7f) as u32;
            let last = b & 0x80 == 0;
            i -= 1;
            if i == 0 {
                if last && v & 0xfe000000 == 0 { // upper 7 bits clear
                    *self = iter.as_slice();
                    return Ok(v << 7 | bits)
                }
                return Err(Error::Overflow)
            }
            v = v << 7 | bits;
            if last {
                *self = iter.as_slice();
                return Ok(v)
            }
        }
        Err(Error::Short)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_u8() {
        let mut data : &[u8] = &[1, 2];
        assert_eq!(Ok(1), data.read_u8());
        assert_eq!(Ok(2), data.read_u8());
        assert_eq!(Err(Error::Short), data.read_u8());
        assert_eq!(0, data.len());
    }

    #[test]
    fn test_read_u16() {
        let mut data : &[u8] = &[1, 2, 3, 4];
        assert_eq!(Ok(0x0201), data.read_u16());
        assert_eq!(Ok(0x0403), data.read_u16());
        assert_eq!(Err(Error::Short), data.read_u16());
        assert_eq!(0, data.len());
    }


    #[test]
    fn test_read_u32() {
        let mut data : &[u8] = &[1, 2, 3, 4];
        assert_eq!(Ok(0x04030201), data.read_u32());
        assert_eq!(Err(Error::Short), data.read_u32());
        assert_eq!(0, data.len());
    }

    #[test]
    fn test_read_v32_1() {
        let mut data : &[u8] = &[1, 2];
        assert_eq!(Ok(0x01), data.read_v32());
        assert_eq!(Ok(0x02), data.read_v32());
        assert_eq!(Err(Error::Short), data.read_v32());
        assert_eq!(0, data.len());
    }

    #[test]
    fn test_read_v32_2() {
        let mut data : &[u8] = &[0xff, 0xff, 0xff, 0xff];
        assert_eq!(Err(Error::Short), data.read_v32());
    }

    #[test]
    fn test_read_v32_3() {
        let mut data : &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(Err(Error::Overflow), data.read_v32());
    }

    #[test]
    fn test_read_v32_4() {
        let mut data : &[u8] = &[0xff, 0xff, 0xff, 0xff, 0x80];
        assert_eq!(Err(Error::Overflow), data.read_v32());
    }

    #[test]
    fn test_read_v32_6() {
        let mut data : &[u8] = &[0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(Err(Error::Overflow), data.read_v32());
    }

    #[test]
    fn test_read_v32_7() {
        let mut data : &[u8] = &[0x80, 0x80, 0x80, 0x80, 0];
        assert_eq!(Ok(0), data.read_v32());
    }

    #[test]
    fn test_read_v32_8() {
        let mut data : &[u8] = &[0x8f, 0xff, 0xff, 0xff, 0x7f];
        assert_eq!(Ok(0xffffffff), data.read_v32());
    }
}
