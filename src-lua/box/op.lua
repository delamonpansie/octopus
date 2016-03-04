local ffi, bit = require('ffi'), require('bit')
local packer, unpacker = packer, unpacker
local tonumber, ipairs, select, error = tonumber, ipairs, select, error
local pairs = pairs
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
                if op[3] then
                        s:field_u32(op[3])
                else
                        s:u8(0)
                end
                if op[4] then
                        s:field_u32(op[4])
                else
                        s:u8(0)
                end
                s:field(op[5])
                local buf, len = s:pack()
                req:field_raw(buf, len)
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

local function tuple_skip(u, n)
    local ptr, pos  = u.ptr, u:pos()
    for i = 1, n do
        u:field_skip()
    end
    return ptr, u:pos() - pos
end

function wal_parse(tag, data, len)
    assertarg(tag, 'number', 1)
    -- assertarg(data, 'ctype', 2)
    assertarg(len, 'number', 3)

    local u = unpacker(data, len)
    u:raise_on_error()

    local cmd = { }

    local tag_name = wal.tag.name(tag)
    if tag_name == "snap_tag" then
        cmd.op = -1
    elseif wal.tag.type(tag) ~= "wal" then
        return nil
    elseif wal.tag.value(tag) > wal.tag.code.paxos_nop then
        cmd.op = bit.rshift(wal.tag.value(tag), 5)
    elseif tag_name == "wal_tag" then
        cmd.op = u:u16()
    else
        return nil
    end

    cmd.n = u:u32()
    cmd.namespace = cmd.n

    if cmd.op == -1 then
        cmd.op = op.INSERT
        cmd.op_name = 'insert'
        cmd.flags = 0
        local cardinality = u:u32()
        local bsize = u:u32()
        assert(bsize == u.len)
        cmd.tuple = tuple(nil, cardinality, u.ptr, bsize)
        u:skip(bsize)
    elseif cmd.op == op.INSERT then
        cmd.op_name = 'insert'
        cmd.flags = u:u32()
        local cardinality = u:u32()
        local ptr, bsize = tuple_skip(u, cardinality)
        cmd.tuple = tuple(nil, cardinality, ptr, bsize)
    elseif cmd.op == op.DELETE then
        cmd.op_name = 'delete'
        cmd.flags = u:u32()
        local key_cardinality = u:u32()
        local ptr, key_bsize = tuple_skip(u, key_cardinality)
        cmd.key = tuple(nil, key_cardinality, ptr, key_bsize)
    elseif cmd.op == op.DELETE_1_3 then
        cmd.op_name = 'delete'
        cmd.flags = 0
        local key_cardinality = u:u32()
        local ptr, key_bsize = tuple_skip(u, key_cardinality)
        cmd.key = tuple(nil, key_cardinality, ptr, key_bsize)
    elseif cmd.op == op.UPDATE_FIELDS then
        cmd.op_name = 'update'
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
                    op[2] = op[2].."16"
                    op[3] = u:u16()
                elseif n == 4 then
                    op[3] = u:u32()
                elseif n == 8 then
                    op[2] = op[2].."64"
                    op[3] = u:u64()
                else
                    error("Bad UPDATE_FIELDS arg: arith op arg must be 2, 4 or 8 bytes long")
                end
            elseif op[2] == "splice" then
                local u = u:field_unpacker()
                local l = u:u8()
                if l == 0 then
                    op[3] = false
                elseif l == 4 then
                    op[3] = u:u32()
                else
                    error("splice format error")
                end
                local l = u:u8()
                if l == 0 then
                    op[4] = false
                elseif l == 4 then
                    op[4] = u:u32()
                else
                    error("splice format error")
                end
                op[5] = u:field_string()
            else
                op[3] = u:field_string()
            end
            table.insert(ops, op)
        end
        cmd.update_mops = ops
    else
        error(("unknown op %x"):format(cmd.op))
    end

    if #u ~= 0 then
        error(("Unable to parse row: %d bytes unparsed"):format(#u))
    end

    return cmd
end

local function snap_insert(n, t, p)
    local l = p:len()
    p:u32(n)
    p:u32(#t)
    local bsize_ix = p:need(4)
    for _, v in ipairs(t) do
        p:field(v)
    end
    local len = p:len() - bsize_ix - 4
    ffi.cast("u32*", p.ptr+bsize_ix)[0] = len
    return p:len() - l
end

local function wal_pack(r)
    local p = packer()
    local row = ffi.cast("struct row_v12*", p:need_ptr(ffi.sizeof("struct row_v12")))
    row.lsn = r.lsn
    row.scn = r.scn
    row.cookie = r.cookie
    row.tm = r.tm

    local cmd = r.cmd
    if cmd == nil then
        row.len = r.len
        row.tag = r.tag
        p:raw(r.data, r.len)
        return ffi.cast("struct row_v12*", p.ptr)
    end

    if wal.tag.type(r.row.tag) ~= "wal" then
        assert(wal.tag.name(r.row.tag) == "snap_tag")
        if cmd == op.NOP then
            return nil
        end
        assert(cmd.op == op.INSERT)
        row.tag = bit.bor(wal.tag.code.snap_tag, wal.tag.SNAP)
        local len = snap_insert(cmd.n, cmd.tuple, p)
        row = ffi.cast("struct row_v12*", p.ptr)
        row.len = len
        return row
    end

    if cmd == op.NOP then
        row.tag = bit.bor(wal.tag.code.nop, wal.tag.WAL)
        row.len = 2
        p:u16(0)
        return ffi.cast("struct row_v12*", p.ptr)
    end

    row.tag = bit.bor(bit.lshift(cmd.op, 5), wal.tag.WAL)

    local _, ptr, len
    if cmd.op == op.INSERT then
        _, ptr, len = insert(cmd.flags, cmd.n, cmd.tuple)
    elseif cmd.op == op.DELETE then
        _, ptr, len = pack.delete(cmd.flags, cmd.n, cmd.key)
    elseif cmd.op == op.UPDATE_FIELDS then
        _, ptr, len = pack.update(cmd.flags, cmd.n, cmd.key, cmd.update_mops)
    else
        error("unknown cmd.op = "..cmd.op)
    end
    row.len = len
    p:raw(ptr, len)
    return ffi.cast("struct row_v12*", p.ptr)
end

local rmethods = {
    replace = function (self, n, tuple)
        assert(self.cmd ~= nil)
        local cmd = {flags = 0}
        cmd.op = op.INSERT
        cmd.n, cmd.namespace = n, n
        cmd.tuple = tuple
        self.cmd = cmd
        return self
    end,
    add = function (self, n, tuple)
        assert(self.cmd ~= nil)
        local cmd = {flags = 2}
        cmd.op = op.INSERT
        cmd.n, cmd.namespace = n, n
        cmd.tuple = tuple
        self.cmd = cmd
        return self
    end,
    delete = function (self, n, tuple)
        assert(self.cmd ~= nil)
        local cmd = {flags = 0}
        cmd.op = op.DELETE
        cmd.n, cmd.namespace = n, n
        cmd.key = key
        self.cmd = cmd
        return self
    end,
    update = function (self, n, key, ops)
        assert(self.cmd ~= nil)
        local cmd = {flags = 0}
        cmd.op = op.UPDATE_FIELDS
        cmd.n, cmd.namespace = n, n
        cmd.key = key
        cmd.update_mops = ops
        self.cmd = cmd
        return self
    end,
    nop = function (self, n, key, ops)
        assert(self.cmd ~= nil)
        self.cmd = op.NOP
        return self
    end,
    copy = function (self)
        local r = {}
        for k, v in pairs(self) do
            r[k] = v
        end
        return setmetatable(r, rmeta)
    end,
    tag_name = function (self)
        return wal.tag.name(self.tag)
    end,
    tag_type = function (self)
        return wal.tag.type(self.tag)
    end,
}
local rmeta = { __index = rmethods }

function wal_filter(f)
    return function(row, arg)
        if row == nil then
            f(nil, arg)
            return
        end
        local r = {}
        r.row = row
        r.lsn = row.lsn
        r.scn = row.scn
        r.tag = row.tag
        r.cookie = row.cookie
        r.tm = row.tm
        r.data = row.data
        r.len = row.len
        r.cmd = wal_parse(r.tag, row.data, row.len)
        setmetatable(r, rmeta)
        local res = f(r, arg)
        if res == nil or res == true or res == false or type(res) == 'cdata' then
            return res
        else
            return wal_pack(res)
        end
    end
end
