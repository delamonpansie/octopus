local ffi = require("ffi")
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
