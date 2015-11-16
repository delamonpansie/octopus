local ffi, bit = require('ffi'), require('bit')
local packer, unpacker = packer, unpacker
local tonumber, ipairs, select, error = tonumber, ipairs, select, error
local table, setmetatable, type = table, setmetatable, type
local assertarg = assertarg
local assert = assert
local print = print

local tuple = require('box.dyn_tuple').new
local wal = require 'wal'

module(...)

local op = { NOP = 1,
             INSERT = 13,
             UPDATE_FIELDS = 19,
             DELETE_1_3 = 20,
             DELETE = 21 }
_M.op = op

pack = {}

local req_packer = packer()
local function cached_packer()
    req_packer:reset()
    return req_packer
end

local function insert(flags, n, ...)
        local req = cached_packer()

        assert(n ~= nil)

        req:u32(n)
        req:u32(flags)

        if select('#', ...) == 1 and
            type(select(1, ...)) == 'table'
        then
            local t = select(1, ...)
            req:u32(#t)
            for _, v in ipairs(t) do
                req:field(v)
            end
        else
            req:u32(select('#', ...))
            for i = 1, select('#', ...) do
                req:field(select(i, ...))
            end
        end
        return op.INSERT, req:pack()
end

function pack.replace(flags, n, ...)
        -- flags == 1 => return tuple
        return insert(flags, n, ...)
end
function pack.add(flags, n, ...)
    -- flags == 3 => return tuple + add tuple flags
    return insert(bit.bor(flags, 2), n, ...)
end

function pack.delete(flags, n, key)
        local req = cached_packer()

        req:u32(n)
        req:u32(flags)
        if type(key) == 'table' then
            local key_cardinality = #key
            req:u32(key_cardinality)
            for i, val in ipairs(key) do
                req:field(val)
            end
        else
            local key_cardinality = 1
            req:u32(key_cardinality)
            req:field(key)
        end
        return op.DELETE, req:pack()
end

local function pack_mop(req, op)
        req:u32(op[1])
        if (op[2] == "set") then
                req:u8(0)
                req:field(op[3])
        elseif (op[2] == "set16") then
                req:u8(0)
                req:field_u16(op[3])
        elseif (op[2] == "set32") then
                req:u8(0)
                req:field_u32(op[3])
        elseif (op[2] == "set64") then
            req:u8(0)
                req:field_u64(op[3])
        elseif (op[2] == "add") then
                req:u8(1)
                req:field_u32(op[3])
        elseif (op[2] == "and") then
                req:u8(2)
                req:field_u32(op[3])
        elseif (op[2] == "or") then
                req:u8(3)
                req:field_u32(op[3])
        elseif (op[2] == "xor") then
                req:u8(4)
                req:field_u32(op[3])
        elseif (op[2] == "add16") then
                req:u8(1)
                req:field_u16(op[3])
        elseif (op[2] == "and16") then
                req:u8(2)
                req:field_u16(op[3])
        elseif (op[2] == "or16") then
                req:u8(3)
                req:field_u16(op[3])
        elseif (op[2] == "xor16") then
                req:u8(4)
                req:field_u16(op[3])
        elseif (op[2] == "add32") then
                req:u8(1)
                req:field_u32(op[3])
        elseif (op[2] == "and32") then
                req:u8(2)
                req:field_u32(op[3])
        elseif (op[2] == "or32") then
                req:u8(3)
                req:field_u32(op[3])
        elseif (op[2] == "xor32") then
                req:u8(4)
                req:field_u32(op[3])
        elseif (op[2] == "add64") then
                req:u8(1)
                req:field_u64(op[3])
        elseif (op[2] == "and64") then
                req:u8(2)
                req:field_u64(op[3])
        elseif (op[2] == "or64") then
                req:u8(3)
                req:field_u64(op[3])
        elseif (op[2] == "xor64") then
                req:u8(4)
                req:field_u64(op[3])
        elseif (op[2] == "splice") then
                req:u8(5)
                local s = packer()
                if (op[3] ~= nil) then
                        s:field_u32(op[3])
                else
                        s:u8(0)
                end
                if (op[4] ~= nil) then
                        s:field_u32(op[4])
                else
                        s:u8(0)
                end
                s:field(op[5])
                local buf, len = s:pack()
                req:varint32(len)
                req:raw(buf, len)
        elseif (op[2] == "delete") then
                req:string("\006\000")
        elseif (op[2] == "insert") then
                req:u8(7)
                req:field(op[3])
        end
end

function pack.update(flags, n, key, ...)
        local tabarg = nil
        local nmops = select('#', ...)
        if nmops == 1 then
            local arg = select(1, ...)
            if type(arg[1]) == 'table' then
                tabarg = arg
                nmops = #arg
            end
        end

        local req = cached_packer()

        req:u32(tonumber(n))
        req:u32(flags)
        if type(key) == 'table' then
            local key_cardinality = #key
            req:u32(key_cardinality)
            for i, val in ipairs(key) do
                req:field(val)
            end
        else
            local key_cardinality = 1
            req:u32(key_cardinality)
            req:field(key)
        end
        req:u32(nmops)

        if tabarg then
                for _, op in ipairs(tabarg) do
                    pack_mop(req, op)
                end
        else
                for k = 1, nmops do
                    pack_mop(req, select(k, ...))
                end
        end
        return op.UPDATE_FIELDS, req:pack()
end


function wal_parse(tag, data, len)
    assertarg(tag, 'number', 1)
    -- assertarg(data, 'ctype', 2)
    assertarg(len, 'number', 3)

    local u = unpacker(data, len)
    u:raise_on_error()

    local cmd = { }

    if wal.tag.name(tag) == "wal_tag" then
        cmd.op = u:u16()
    elseif wal.tag.name(tag) == "nop" then
        cmd.op = op.NOP
        u:u16()
    elseif wal.tag.type(tag) == "wal" then
        if wal.tag.value(tag) <= wal.tag.code.paxos_nop then
            return nil
        end
        cmd.op = bit.rshift(wal.tag.value(tag), 5)
    elseif wal.tag.name(tag) == "snap_tag" then
        cmd.op = op.INSERT
    else
        return nil
    end

    if wal.tag.name(tag) ~= "nop" then
        cmd.n = u:u32()
        cmd.namespace = cmd.n
    end

    local function tuple_skip(u, n)
        local ptr, pos  = u.ptr, u:pos()
        for i = 1, n do
            u:field_skip()
        end
        return ptr, u:pos() - pos
    end

    if cmd.op == op.INSERT then
        cmd.flags = u:u32()
        local cardinality = u:u32()
        local ptr, bsize = tuple_skip(u, cardinality)
        cmd.tuple = tuple(nil, cardinality, ptr, bsize)
    elseif cmd.op == op.DELETE then
        cmd.flags = u:u32()
        local key_cardinality = u:u32()
        local ptr, key_bsize = tuple_skip(u, key_cardinality)
        cmd.key = tuple(nil, key_cardinality, ptr, key_bsize)
    elseif cmd.op == op.DELETE_1_3 then
        cmd.flags = 0
        local key_cardinality = u:u32()
        local ptr, key_bsize = tuple_skip(u, key_cardinality)
        cmd.key = tuple(nil, key_cardinality, ptr, key_bsize)
    elseif cmd.op == op.UPDATE_FIELDS then
        cmd.flags = u:u32()
        local key_cardinality = u:u32()
        local ptr, key_bsize = tuple_skip(u, key_cardinality)
        cmd.key = tuple(nil, key_cardinality, ptr, key_bsize)
        local op_count = u:u32()
        local ops = {}
        local update_op = {[0] = "set", "add", "and", "xor", "or", "splice", "delete", "insert"}
        for i = 1, op_count do
            local op = {}
            op[1] = u:u32()
            op[2] = update_op[u:u8()]
            if op[2] == "add" or op[2] == "and" or op[2] == "xor" or op[2] == "or" then
                local pos = u:pos()
                local n = u:u8() -- actually ber()
                if n == 2 then
                    op[3] = u:u16()
                elseif n == 4 then
                    op[3] = u:u32()
                elseif n == 8 then
                    op[3] = u:u64()
                else
                    error("Bad UPDATE_FIELDS arg: arith op arg must be 2, 4 or 8 bytes long")
                end
            else
                op[3] = u:field_string()
            end
            table.insert(ops, op)
        end
        cmd.update_mops = ops
    elseif cmd.op == op.NOP then
    else
        error(("unknown op %x"):format(cmd.op))
    end

    if #u ~= 0 then
        error(("Unable to parse row: %d bytes unparsed"):format(#u))
    end

    return cmd
end
