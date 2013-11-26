local ffi = require("ffi")
local bit = require("bit")
local print, tostring = print, tostring
local ipairs = ipairs
local format = string.format

replication_filter = replication_filter or {}
local replication_filter = replication_filter
ffi.cdef([[
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
function __feederentrypoint(f, ptr)
    local row = f(ffi.cast(row_v12, ptr))
    assert(type(row) == 'nil' or
           type(row) == 'boolean' or
           (type(row) == 'cdata' and ffi.typeof(row) == row_v12))
    return row
end
module(...)

tag_mask = 0x3fff
tag = {
    "snap_initial_tag",  -- SNAP
    "snap_tag",          -- SNAP
    "wal_tag",           -- WAL
    "snap_final_tag",    -- SNAP
    "wal_final_tag",     -- WAL
    "run_crc",           -- WAL
    "nop",               -- WAL
    "snap_skip_scn",     -- SNAP
    "paxos_prepare",     -- SYS
    "paxos_promise",     -- SYS
    "paxos_propose",     -- SYS
    "paxos_accept",      -- SYS
    "paxos_nop",         -- SYS
}

for id, name in ipairs(tag) do
    tag[name] = id
end

local mt = {__index={}}
function mt.__index:unmasktag()
    return bit.band(self.tag, tag_mask)
end
function mt:__tostring()
    return format("lsn:%s scn:%s tm:%s cookie:%s t:%d/%s",
			 self.lsn, self.scn, self.tm, self.cookie, self.tag, tag[self:unmasktag()])
end
ffi.metatype('struct row_v12', mt)

function replication_filter.id_log(obj)
        print(row)
        return true
end
