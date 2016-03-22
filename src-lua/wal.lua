local ffi, bit = require('ffi'), require('bit')
local setmetatable, ipairs, error = setmetatable, ipairs, error
local assertarg, type = assertarg, type
local palloc = palloc
module(...)

local tag_mask = 0x3fff
local tag_code = setmetatable({
        [ffi.C.snap_initial] = "snap_initial",
        [ffi.C.snap_data] = "snap_data",
        [ffi.C.wal_data] = "wal_data",
        [ffi.C.snap_final] = "snap_final",
        [ffi.C.wal_final] = "wal_final",
        [ffi.C.run_crc] = "run_crc",
        [ffi.C.nop] = "nop",
        [ffi.C.paxos_promise] = "paxos_promise",
        [ffi.C.paxos_accept] = "paxos_accept",
        [ffi.C.paxos_nop] = "paxos_nop",
        [ffi.C.shard_create] = "shard_create",
        [ffi.C.shard_alter] = "shard_alter",
        [ffi.C.shard_final] = "shard_final",
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
        type = tag_type,
        WAL = 0x8000,
        SNAP = 0x4000,
        SYS = 0xc000,
    }

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
                   local tmpl = "lsn:%s scn:%s shard:%i tm:%s cookie:%s t:%s/%s"
                   return tmpl:format(self.lsn, self.scn, self.shard_id,
                                      self.tm, self.cookie,
                                      tag_type(self.tag), tag_name(self.tag))
               end }

ffi.metatype('struct row_v12', meta)
