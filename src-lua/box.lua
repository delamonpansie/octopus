
local assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable =
      assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable

local string, tostring, tonumber =
      string, tostring, tonumber

local rawget = rawget

local tou32, tofield = string.tou32, string.tofield

local ffi, bit, debug = require("ffi"), require("bit"), require("debug")
local net = require("net")
module(...)

user_proc = {}

-- make useful aliases
space = object_space

local object_space_mt = getmetatable(object_space) or {}
assert(not object_space_mt.__index)
object_space_mt.__index = function(table, i) return rawget(table, tonumber(i)) end
setmetatable(object_space, object_space_mt)

for n, v in pairs(object_space) do
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
        return dispatch(13, table.concat(req))
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
                        if (op[3] ~= nil) then
                                table.insert(s, "\004")
                                table.insert(s, tou32(op[3]))
                        else
                                table.insert(s, "\000")
                        end
                        if (op[4] ~= nil) then
                                table.insert(s, "\004")
                                table.insert(s, tou32(op[4]))
                        else
                                table.insert(s, "\000")
                        end
                        table.insert(s, tofield(op[5]))
                        table.insert(req, tofield(table.concat(s)))
                elseif (op[2] == "delete") then
                        table.insert(req, "\006\000")
                elseif (op[2] == "insert") then
                        table.insert(req, "\007")
                        table.insert(req, tofield(op[3]))
                end
        end
        return dispatch(19, table.concat(req))
end

ffi.cdef[[
struct box_tuple {
	uint32_t bsize;
	uint32_t cardinality;
	uint8_t data[0];
} __attribute__((packed));
]]


ffi.cdef [[void object_incr_ref(struct tnt_object *obj);]]
local tnt_object_ref = ffi.typeof("struct tnt_object **") -- userdata holding pointer to tnt_obj, hence double ptr
local tuple_t = ffi.typeof("struct box_tuple *")


function ctuple(obj)
   assert(obj ~= nil)
   obj = tnt_object_ref(obj)[0]
   assert(obj.type == 1)
   return ffi.cast(tuple_t, obj.data)
end


function wrap(proc_body)
        if type(proc_body) == "string" then
                proc_body = loadstring(code)
        end
        if type(proc_body) ~= "function" then
                return nil
        end

        local function proc(out, request, ...)
                local retcode, result = proc_body(...)
		local out = net.conn(out)
		local header = out:add_iov_iproto_header(request)
		local bytes = out:bytes()

                if type(result) == "table" then
                        out:add_iov_string(tou32(#result))

                        for k, v in pairs(result) do
			   if type(v) == "string" then
			      out:add_iov_string(v)
			   elseif type(v) == "userdata" then
			      local obj = tnt_object_ref(v)[0]
			      local tuple = ffi.cast(tuple_t, obj.data)
			      ffi.C.object_incr_ref(obj)
			      out:add_iov_ref(obj.data, tuple.bsize + 8, ffi.cast('uintptr_t', obj))
			   else
			      error("unexpected type of result: " .. type(v))
			   end
			end
                elseif type(result) == "number" then
                        out:add_iov_string(tou32(result))
                else
                        error("unexpected type of result: " .. type(result))
                end

		header.data_len = header.data_len + out:bytes() - bytes
		header.ret_code = retcode
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



function decode_varint32(ptr, offt)
        local initial_offt = offt
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

decode = {}

function decode.varint32(obj, offt)
        local tuple = ctuple(obj)
        if (offt < 0 or offt + 1 > tuple.bsize) then
                error(string.format("out of bounds: offt:%i bsize:%i\n%s", offt, tuple.bsize, debug.traceback()))
        end
        local result, offt = decode_varint32(tuple.data, offt)
        if (offt > tuple.bsize) then
                error("out of bounds\n" .. debug.traceback())
        end
        return result, offt
end

function decode.string(obj, offt, len)
        local tuple = ctuple(obj)
        if (offt < 0 or offt + len > tuple.bsize) then
                error(string.format("out of bounds: offt:%i bsize:%i\n%s", offt, tuple.bsize, debug.traceback()))
        end
        return ffi.string(tuple.data + offt, len)
end

local u8_ptr, u16_ptr, u32_ptr = ffi.typeof("uint8_t *"), ffi.typeof("uint16_t *"), ffi.typeof("uint32_t *")

function decode.u8(obj, offt)
        local tuple = ctuple(obj)
        if (offt < 0 or offt + 1 > tuple.bsize) then
                error(string.format("out of bounds: len:1 offt:%i bsize:%i\n%s", offt, tuple.bsize, debug.traceback()))
        end
        return ffi.cast(u8_ptr , tuple.data + offt)[0]
end

function decode.u16(obj, offt)
        local tuple = ctuple(obj)
        if (offt < 0 or offt + 2 > tuple.bsize) then
                error(string.format("out of bounds: len:2 offt:%i bsize:%i\n%s", offt, tuple.bsize, debug.traceback()))
        end
        return ffi.cast(u16_ptr , tuple.data + offt)[0]
end

function decode.u32(obj, offt)
        local tuple = ctuple(obj)
        if (offt < 0 or offt + 4 > tuple.bsize) then
                error(string.format("out of bounds: len:4 offt:%i bsize:%i\n%s", offt, tuple.bsize, debug.traceback()))
        end
        return ffi.cast(u32_ptr , tuple.data + offt)[0]
end

cast = {}
function cast.u32(str)
   if type(str) ~= 'string' then
      error('string expected')
   end
   return ffi.cast('uint32_t *', str)[0]
end
