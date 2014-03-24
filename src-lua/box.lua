local assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable =
      assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable

local string, tostring, tonumber =
      string, tostring, tonumber

local rawget, rawset = rawget, rawset
local printf = printf


local ffi, bit, debug = require("ffi"), require("bit"), require("debug")
local net, index = require("net"), require('index')
local fiber = require("fiber")
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

local add_stat_exec_lua = function(name) end
local add_stat_exec_lua_ok = function(name) end
local add_stat_exec_lua_rcode = function(name, rcode) end
if graphite then
    local add_stat = stat.request_collector{name = 'exec_lua'}
    graphite.add_cb('exec_lua')
    add_stat_exec_lua = add_stat.add_run
    add_stat_exec_lua_ok = add_stat.add_ok
    add_stat_exec_lua_rcode = add_stat.add_rcode
end

local _G = _G
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
local tuple_mt = {}
function tuple(...)
    return setmetatable({...}, tuple_mt)
end


local wrapped = setmetatable({}, {__mode = "k"})
function wrap(proc_body)
        if type(proc_body) ~= "function" then
                return nil
        end
        wrapped[proc_body] = true

        return proc_body
end

local u32ptr = ffi.typeof('uint32_t *')
local uintptr = ffi.typeof('uintptr_t')
local u32buf = ffi.new('uint32_t[1]')
local p = packer()

local function append(result, out)
   local out = net.conn(out)
   p:reset()

   if type(result) == "table" then
       u32buf[0] = #result
       out:add_iov_dup(u32buf, 4)

      for _, v in ipairs(result) do
         if type(v) == "string" then
            out:add_iov_string(v)
         elseif type(v) == "table" and v.__obj then
            ffi.C.object_incr_ref(v.__obj)
            out:add_iov_ref(v.__tuple, v.bsize + 8, ffi.cast(uintptr, v.__obj))
         elseif type(v) == "table" and getmetatable(v) == tuple_mt then
             p:string("....----") -- placeholder for bsize, cardinality
             for i = 1, #v do
                 p:field(v[i])
             end
             local u32 = ffi.cast(u32ptr, p.ptr)
             u32[0] = p:len() - 8 -- bsize adjust
             u32[1] = #v
             out:add_iov_dup(p:pack())
         else
            error("unexpected type of result: " .. type(v), 2)
         end
      end
   elseif type(result) == "number" then
      out:add_iov_string(string.tou32(result))
   else
      error("unexpected type of result: " .. type(result), 2)
   end
end

local fn_cache_mt = {__index = function(t, name)
    local fn = _G
    for k in name:gmatch('[^%.]+') do
       fn = fn[k]
       if not fn then
           error("function '"..name.."' not found")
       end
    end
    if type(fn) ~= 'function' then
       error("'"..name.."' is not a function")
    end
    t[name] = fn
    return fn
end}
local fn_cache = setmetatable({}, fn_cache_mt)
local function clear_cache()
    while true do
        fiber.sleep(1)
        fn_cache = setmetatable({}, fn_cache_mt)
    end
end
fiber.create(clear_cache)

function entry(name, start, out, request, ...)
    add_stat_exec_lua(name)

    local proc = fn_cache[name]
    if wrapped[proc] then
        local rcode, res = proc(...)
        add_stat_exec_lua_rcode(name, rcode)
        return append, rcode, res
    end
    add_stat_exec_lua("NotWrapped")
    add_stat_exec_lua(name..":NotWrapped")
    proc(out, request, ...)
    add_stat_exec_lua_ok(name)
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
