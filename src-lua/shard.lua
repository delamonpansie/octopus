local ffi = require 'ffi'
local bit = require 'bit'
local net = require 'net'
local packer = packer

ffi.cdef [[
  const char * iproto_shard_luacb(struct iproto_0 *req);
]]

shard = {}

function shard:help ()
    return "create(id, <paxos|por>, master, [slave1, slave2])"
end

local msg_shard = 0xff02

function shard:create(id, mode, module, master, slave1, slave2)
    local p = packer()

    local function s16(str)
        if str == nil then
            str = ""
        end
        if #str > 16 then
            error("bad module/hostname")
        end
        p:string(str)
        for i = 0, 16 - #str - 1 do
            p:u8(0)
        end
    end

    p:u32(bit.bor(bit.lshift(id, 16), msg_shard))
    p:u32(0)
    p:u32(0)

    p:u8(0) -- version
    p:u8(0) -- op = create
    if mode == "por" then
        p:u8(0)
    elseif mode == "paxos" then
        p:u8(1)
    else
        error("bad mode")
    end
    p:u32(0) -- row_count
    p:u32(0) -- crc
    if module ~= "Box" then
        error("bad module")
    end
    s16(module)
    if not master then
        error("bad master")
    end
    s16(master)
    s16(slave1)
    s16(slave2)

    local msg, msg_len = p:pack()
    local iproto = ffi.cast('struct iproto_0 *', msg)
    iproto.data_len = msg_len - 12

    return ffi.string(ffi.C.iproto_shard_luacb(iproto))
end
