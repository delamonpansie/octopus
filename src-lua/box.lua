local assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable =
      assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable

local string, tostring, tonumber =
      string, tostring, tonumber

local rawget, rawset = rawget, rawset
local printf = printf


local ffi, bit, debug = require("ffi"), require("bit"), require("debug")
local net, index = require("net"), require('index')
local object, object_cast, varint32, packer = object, object_cast, varint32, packer
local safeptr, assertarg = safeptr, assertarg
local lselect = select

local dyn_tuple = require 'box.dyn_tuple'
local box_op = require 'box.op'

-- legacy, slow because of string interning
ffi.cdef[[ typedef union { char ch[8]; u8 u8; u16 u16; u32 u32; u64 u64;} pack_it_gently ]]
local ptg = ffi.new 'pack_it_gently'
string.tou8 = function(i) ptg.u8 = tonumber(i); return ffi.string(ptg.ch, 1) end
string.tou16 = function(i) ptg.u16 = tonumber(i); return ffi.string(ptg.ch, 2) end
string.tou32 = function(i) ptg.u32 = tonumber(i); return ffi.string(ptg.ch, 4) end
string.tou64 = function(i)
    if type(i) == 'string' then
        ptg.u64 = ffi.C.atoll(i)
    else
        ptg.u64 = i
    end
    return ffi.string(ptg.ch, 8)
end
string.tovarint32 = function(i)
   local n = varint32.write(ptg.ch, tonumber(i))
   return ffi.string(ptg.ch, n)
end
local bufn = 1024
local buf = ffi.new('char[?]', bufn)
string.tofield = function(s)
   local need = 5 + #s
   if bufn < need then
       bufn = need
       buf = ffi.new('char[?]', bufn)
   end
   local n = varint32.write(buf, #s)
   ffi.copy(buf + n, s, #s)
   return ffi.string(buf, n + #s)
end

module(...)

user_proc = {}

ffi.cdef[[
struct object_space {
	int n;
	bool enabled, ignored;
	int cardinality;
	struct Index *index[10];
};
extern struct object_space *object_space_registry;
extern const int object_space_count, object_space_max_idx;
]]

local maxidx = ffi.C.object_space_max_idx
local index_registry_mt = {
   __index = function (table, i)
      i = tonumber(i)
      if i < 0 or i >= maxidx or table.__object_space.index[i] == nil then
	 return nil
      end
      if not rawget(table, i) then
	 local legacy, new = index.cast(table.__object_space.index[i])
	 table[i] = legacy
      end
      return rawget(table, i)
   end,
   __call = function (t, object_space, i)
       i = tonumber(i)
       if i < 0 or i >= maxidx or object_space.__ptr.index[i] == nil then
	   return nil
       end
       if not rawget(t, i + maxidx) then
	   local legacy, new = index.cast(object_space.__ptr.index[i])
	   t[i + maxidx] = new
       end

       return rawget(t, i + maxidx)
   end
}

local object_space_mt = {
   __tostring = function(self)
      return tostring(self.__ptr)
   end
}

object_space_registry = setmetatable({}, {
   __index = function(table, k)
      local i
      -- string and starts from digit
      if type(k) == 'string' and 48 <= k:byte(1) and k:byte(1) <= 57 then
	 i = tonumber(k)
      else
	 i = k
      end

      if type(i) ~= 'number' or
	 i >= ffi.C.object_space_count or
	 not ffi.C.object_space_registry[i].enabled or
	 ffi.C.object_space_registry[i].ignored
      then
	 return nil
      end

      local ptr = ffi.C.object_space_registry[i]
      local index_registry = setmetatable({ __object_space = ptr }, index_registry_mt)
      local object_space = setmetatable({ __ptr = ptr,
					  n = ptr.n,
					  cardinality = ptr.cardinality,
					  index = index_registry }, object_space_mt)
      table[k] = object_space
      return object_space
   end
})


-- make useful aliases
space, object_space = object_space_registry, object_space_registry


-- install automatic cast of object() return value
object_cast[dyn_tuple.obj_type] = dyn_tuple.obj_cast


function select(n, ...)
        local index = object_space[n].index[0]
        local result = {}
        for k, v in pairs({...}) do
                result[k] = index[v]
        end
        return result
end

local _dispatch = _dispatch
--- jit.off(_dispatch) not needed, because C API calls are NYI
for _, v in pairs{'add', 'replace', 'delete', 'update'} do
    local pack = box_op.pack[v]
    _M[v] = function (...) return object(_dispatch(pack(...))) end
end

function ctuple(obj)
   assert(obj ~= nil)
   return obj
end

function wrap(proc_body)
        if type(proc_body) ~= "function" then
                return nil
        end

	local function append(out, request, ret_code, result)
	   local header = out:add_iov_iproto_header(request)
	   local bytes = out:bytes()

	   if type(result) == "table" then
	      out:add_iov_string(string.tou32(#result))

	      for k, v in pairs(result) do
		 if type(v) == "string" then
		    out:add_iov_string(v)
		 elseif type(v) == "table" and v.__obj then
		    ffi.C.object_incr_ref(v.__obj)
		    out:add_iov_ref(v.__tuple, v.bsize + 8, ffi.cast('uintptr_t', v.__obj))
		 else
		    error("unexpected type of result: " .. type(v), 2)
		 end
	      end
	   elseif type(result) == "number" then
	      out:add_iov_string(string.tou32(result))
	   else
	      error("unexpected type of result: " .. type(result), 2)
	   end

	   header.data_len = header.data_len + out:bytes() - bytes
	   header.ret_code = ret_code
	end

        local function proc(out, request, ...)
	   -- proc_body may fail and may block in core
	   -- it's unsafe to modify 'struct conn' while blocking in core,
	   -- because of possible concurent updates
           -- so, append to conn atomically via out:apply(f, args)

	   local ret_code, result = proc_body(...)
	   local out = net.conn(out)
	   out:apply(append, request, ret_code, result)
        end

        return proc
end

function tuple(...)
   local p = packer()
   p:string("....----") -- placeholder for bsize, cardinality
   for i = 1, lselect('#', ...) do
      p:field(lselect(i, ...))
   end
   local buf, len = p:pack()
   local u32 = ffi.cast('uint32_t *', buf)
   u32[0] = len - 8 -- bsize adjust
   u32[1] = lselect('#', ...)
   return ffi.string(buf, len)
end


local charbuf = ffi.typeof('char *')
function decode_varint32(ptr, offt) return varint32.read(ffi.cast(charbuf, ptr) + offt) end

decode = {}
function decode.varint32(obj, offt) return obj:datacast('varint32', offt) end
function decode.string(obj, offt, len) return obj:datacast('string', offt, len) end
function decode.u8(obj, offt) return obj:datacast('uint8_t', offt) end
function decode.u16(obj, offt) return obj:datacast('uint16_t', offt) end
function decode.u32(obj, offt) return obj:datacast('uint32_t', offt) end

cast = {}
function cast.u32(str)
   assertarg(str, 'string', 1)
   return ffi.cast('uint32_t *', str)[0]
end
