
local error, print, type, pairs, ipairs, table, setmetatable =
      error, print, type, pairs, ipairs, table, setmetatable

local string, tostring =
      string, tostring

local tou32, tofield = string.tou32, string.tofield
local netmsg = netmsg

local ffi, bit = require("ffi"), require("bit")

module(...)

user_proc = {}

-- make useful aliases
space = object_space
for n, v in pairs(object_space) do
        object_space[tostring(n)] = v
        v.index = {}
        v.index.mt = {}
        v.index.mt.__index = function (table, i) return index(n, i) end
        setmetatable(v.index, v.index.mt)
end

function select(n, ...)
        local index = object_space[n].index[0]
        local result = {}
        for k, v in pairs({...}) do
                result[k] = index[v]
        end
        return result
end

function replace(n, ...)
        local tuple = {...}
        local flags = 0
        local req = {}

        table.insert(req, tou32(n))
        table.insert(req, tou32(flags))
        table.insert(req, tou32(#tuple))
        for k, v in pairs(tuple) do
                table.insert(req, tofield(v))
        end
        dispatch(13, table.concat(req))
end

function delete(n, key)
        local key_len = 1
        local req = {}

        table.insert(req, tou32(n))
        table.insert(req, tou32(key_len))
        table.insert(req, tofield(key))
        dispatch(20, table.concat(req))
end

function update(n, key, ...)
        local ops = {...}
        local flags, key_cardinality = 0, 1
        local req = {}

        table.insert(req, tou32(n))
        table.insert(req, tou32(flags))
        table.insert(req, tou32(key_cardinality))
        table.insert(req, tofield(key))
        table.insert(req, tou32(#ops))
        for k, op in ipairs(ops) do
                table.insert(req, tou32(op[1]))
                if (op[2] == "set") then
                        table.insert(req, "\000")
                        table.insert(req, tofield(op[3]))
                elseif (op[2] == "add") then
                        table.insert(req, "\001\004")
                        table.insert(req, tou32(op[3]))
                elseif (op[2] == "and") then
                        table.insert(req, "\002\004")
                        table.insert(req, tou32(op[3]))
                elseif (op[2] == "or") then
                        table.insert(req, "\003\004")
                        table.insert(req, tou32(op[3]))
                elseif (op[2] == "xor") then
                        table.insert(req, "\004\004")
                        table.insert(req, tou32(op[3]))
                elseif (op[2] == "splice") then
                        table.insert(req, "\005")
                        local s = {}
                        table.insert(s, "\004")
                        table.insert(s, tou32(op[3]))
                        table.insert(s, "\004")
                        table.insert(s, tou32(op[4]))
                        table.insert(s, tofield(op[5]))
                        table.insert(req, tofield(table.concat(s)))
                elseif (op[2] == "delete") then
                        table.insert(req, "\006\000")
                elseif (op[2] == "insert") then
                        table.insert(req, "\007")
                        table.insert(req, tofield(op[3]))
                end
        end
        dispatch(19, table.concat(req))
end

function wrap(proc_body)
        if type(proc_body) == "string" then
                proc_body = loadstring(code)
        end
        if type(proc_body) ~= "function" then
                return nil
        end

        local function proc(out, object_space, ...)
                local retcode, result = proc_body(object_space, ...)

                if type(result) == "table" then
                        netmsg.add_iov(out, tou32(#result))

                        for k, v in pairs(result) do
                                netmsg.add_iov(out, v)
                        end
                elseif type(result) == "number" then
                        netmsg.add_iov(out, tou32(result))
                else
                        error("unexpected type of result:" .. type(result))
                end

                return retcode
        end

        return proc
end

function tuple(...)
        local f, bsize = {...}, 0
        for k, v in ipairs(f) do
                f[k] = string.tofield(v)
                bsize = bsize + #f[k]
        end
        table.insert(f, 1, string.tou32(#f))
        table.insert(f, 1, string.tou32(bsize))
        return table.concat(f)
end


ffi.cdef[[
struct box_tuple {
	uint32_t bsize;
	uint32_t cardinality;
	uint8_t data[0];
} __attribute__((packed));
]]

function decode_varint32(ptr, offt)
        local result = 0
        local byte
        
        byte = ptr[offt]
        offt = offt + 1
        result = bit.band(byte, 0x7f)
        if bit.band(byte, 0x80) ~= 0 then
                byte = ptr[offt]
                offt = offt + 1
                result = bit.bor(bit.lshift(result, 7),
                                 bit.band(byte, 0x7f))
                if bit.band(byte, 0x80) ~= 0 then
                        byte = ptr[offt]
                        offt = offt + 1
                        result = bit.bor(bit.lshift(result, 7),
                                         bit.band(byte, 0x7f))
                        if bit.band(byte, 0x80) ~= 0 then
                                byte = ptr[offt]
                                offt = offt + 1
                                result = bit.bor(bit.lshift(result, 7),
                                                 bit.band(byte, 0x7f))
                                if bit.band(byte, 0x80) ~= 0 then
                                        byte = ptr[offt]
                                        offt = offt + 1
                                        result = bit.bor(bit.lshift(result, 7),
                                                         bit.band(byte, 0x7f))
                                end
                        end
                end
        end
        return result, offt
end

function ctuple(obj)
        obj = ffi.cast("struct tnt_object **", obj)
        if obj[0].type == 1 then
                return ffi.cast("struct box_tuple *", obj[0].data)
        else
                error("not a box tuple")
        end
end
