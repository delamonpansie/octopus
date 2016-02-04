local ffi = require("ffi")
local bit = require('bit')
local TAG_MASK = 0x3fff
local TAG_SIZE = 14
local TAG_SNAP = 0x4000
local TAG_WAL = 0x8000
local TAG_SYS = 0xc000

local snap_initial = 1
local snap_data = 2
local wal_data = 3
local snap_final = 4
local wal_final = 5
local run_crc = 6
local nop = 7
local paxos_promise = 8
local paxos_accept = 9
local paxos_nop = 10
local shard_tag = 11
local user_tag = 32

require("wal") -- struct row_v12 cdef

local print = print
local format = string.format
local assert = assert
local type = type
local tonumber = tonumber

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

local shard_id
function replication_filter.changer(row, arg)
    if row == nil then
        shard_id = tonumber(arg)
        return
    end

    if row.lsn == 0 and row.scn == 0 then return true end
    if row.scn == -1 then return true end
    if row.shard_id ~= shard_id then return false end

    local tag = bit.band(row.tag, TAG_MASK)
    if tag == paxos_promise then return false end
    if tag == paxos_accept  then return false end
    if tag == paxos_nop     then return false end
    return true
end
