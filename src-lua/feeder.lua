local ffi = require("ffi")
local bit = require('bit')
local TAG_MASK = 0x3fff
local TAG_SIZE = 14
local TAG_SNAP = 0x4000
local TAG_WAL = 0x8000
local TAG_SYS = 0xc000

require("wal") -- struct row_v12 cdef

local print = print
local format = string.format
local assert = assert
local type = type

replication_filter = replication_filter or {}
local replication_filter = replication_filter

local row_v12 = ffi.typeof("struct row_v12 *")
function __feederentrypoint(f, ptr, arg)
    local row = f(ptr and ffi.cast(row_v12, ptr), arg)
    assert(type(row) == 'nil' or
           type(row) == 'boolean' or
           (type(row) == 'cdata' and ffi.typeof(row) == row_v12))
    return row
end

module(...)

function replication_filter.id_log(obj)
        print(obj)
        return true
end

function replication_filter.tag_wal(row)
    return bit.band(row.tag, bit.bnot(TAG_MASK)) == TAG_WAL
end
