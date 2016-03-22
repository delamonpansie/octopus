local ffi = require("ffi")
local bit = require('bit')

local wal_tag = require("wal").tag

local print = print
local format = string.format
local assert = assert
local type = type
local tonumber = tonumber
local tostring = tostring

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
    return wal_tag.type(row.tag) == "wal"
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

    local tag = wal_tag.name(row.tag)
    if tag == "paxos_promise" then return false end
    if tag == "paxos_accept"  then return false end
    if tag == "paxos_nop"     then return false end
    return true
end
