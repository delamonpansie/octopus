local ffi, bit = require('ffi'), require('bit')
local setmetatable, ipairs, error = setmetatable, ipairs, error
local assertarg, type = assertarg, type
local palloc = palloc
module(...)

local tag_mask = 0x3fff
local tag_code = setmetatable({
    "snap_initial_tag",
    "snap_tag",
    "wal_tag",
    "snap_final_tag",
    "wal_final_tag",
    "run_crc",
    "nop",
    "snap_skip_scn",
    "paxos_prepare",
    "paxos_promise",
    "paxos_propose",
    "paxos_accept",
    "paxos_nop",
}, {__index = function(t, k) return "usr" .. bit.rshift(k, 5) end})

for id, name in ipairs(tag_code) do
    tag_code[name] = id
end

local function tag_value(tag) assertarg(tag, 'number', 1); return bit.band(tag, tag_mask) end
local function tag_name(tag) assertarg(tag, 'number', 1); return tag_code[tag_value(tag)] end
local function tag_type(tag)
    assertarg(tag, 'number', 1);
    local t = bit.band(tag, bit.bnot(tag_mask))
    if t == 0x8000 then return "wal"
    elseif t == 0x4000 then return "snap"
    elseif t == 0xc000 then return "sys"
    else error(("Bad tag type 0x%x"):format(t), 2) end
end

tag = { mask = tag_mask,
        code =  tag_code,
        value = tag_value,
        name = tag_name,
        type = tag_type }


ffi.cdef([[
struct row_v12 {
	u32 header_crc32c;
	i64 lsn;
	i64 scn;
	u16 tag;
	union {
		u64 cookie;
		u16 shard_id;
	};
	double tm;
	u32 len;
	u32 data_crc32c;
	u8 data[];
} __attribute__((packed));
]])

ffi.cdef[[u16 fix_tag_v2(u16 tag);]]

local v12ptr = ffi.typeof('struct row_v12 *')
local v12size = ffi.sizeof('struct row_v12')

local m = { tag_value = function(self) return tag_value(self.tag) end,
            tag_type = function(self) return tag_type(self.tag) end,
            tag_name = function(self) return tag_name(self.tag) end,
            tag_to_v2 = function(self)
                self.tag = ffi.C.fix_tag_v2(bit.band(self.tag, tag_mask))
            end,
            update_data = function(self, data, len)
                if not len and type(data) == 'string' then
                    len = #data
                end

                if self.len < len then
                    local new = ffi.cast(v12ptr, palloc(v12size + len))
                    ffi.copy(new, self, v12size)
                    self = new
                end

                ffi.copy(self.data, data, len)
                self.len = len
                return self
            end}
local meta = { __index = m,
               __tostring = function(self)
                   local tmpl = "lsn:%s scn:%s tm:%s cookie:%s t:%s/%s"
                   return tmpl:format(self.lsn, self.scn, self.tm, self.cookie,
                                      tag_type(self.tag), tag_name(self.tag))
               end }

ffi.metatype('struct row_v12', meta)
