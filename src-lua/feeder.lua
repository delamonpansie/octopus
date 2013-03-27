local ffi = require("ffi")
local print, tostring = print, tostring

replication_filter = replication_filter or {}
local replication_filter = replication_filter
module(...)
ffi.cdef([[
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

struct row_v12 {
	u32 header_crc32c;
	i64 lsn;
	i64 scn;
	u16 tag;
	u64 cookie;
	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[];
} __attribute__((packed));
]])

local row_v12 = ffi.typeof("struct row_v12 *")

tag = { snap_initial = 1,
        snap = 2,
        wal = 3,
        snap_final = 4,
        wal_final = 5 }

pass_tag = { [tag.snap_initial] = 1,
             [tag.snap] = 1,
             [tag.snap_final] = 1,
             [tag.wal_final] = 1 }

function crow(obj)
        if obj ~= nil then
                return ffi.cast(row_v12, obj)
        else
                error("attempt to dereference nil row")
        end
end

decode = {}
function decode.string(obj, offt, len)
        local row = crow(obj)
        if (offt < 0 or offt + len > row.len) then
                error(string.format("out of bounds: offt:%i len:%i\n%s", offt, row.len, debug.traceback()))
        end
        return ffi.string(row.data + offt, len)
end

local u8_ptr, u16_ptr, u32_ptr = ffi.typeof("uint8_t *"), ffi.typeof("uint16_t *"), ffi.typeof("uint32_t *")

function decode.u8(obj, offt)
        local row = crow(obj)
        if (offt < 0 or offt + 1 > row.len) then
                error(string.format("out of bounds: offt:%i len:%i\n%s", offt, row.len, debug.traceback()))
        end
        return ffi.cast(u8_ptr , row.data + offt)[0]
end

function decode.u16(obj, offt)
        local row = crow(obj)
        if (offt < 0 or offt + 2 > row.len) then
                error(string.format("out of bounds: offt:%i len:%i\n%s", offt, row.len, debug.traceback()))
        end
        return ffi.cast(u16_ptr , row.data + offt)[0]
end

function decode.u32(obj, offt)
        local row = crow(obj)
        if (offt < 0 or offt + 4 > row.len) then
                error(string.format("out of bounds: offt:%i len:%i\n%s", offt, row.len, debug.traceback()))
        end
        return ffi.cast(u32_ptr , row.data + offt)[0]
end

function replication_filter.id_log(obj)
        local row = crow(obj)
        print("row lsn:" .. tostring(row.lsn) ..
              " scn:" .. tostring(row.scn) ..
              " tag:" .. row.tag ..
              " cookie:" .. tostring(row.cookie) ..
              " tm:" .. row.tm)

        if row.tag ~= tag.wal then
                return nil
        end

        local box_nop = "\01\00\00\00\00\00"
        return nil
end
