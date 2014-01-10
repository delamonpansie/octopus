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

enum object_type {
	BOX_TUPLE = 1
};

struct box_tuple {
	uint32_t bsize; /* byte size of data[] */
	uint32_t cardinality;
	uint8_t data[0];
} __attribute__((packed));
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

local u16_ptr = ffi.typeof("uint16_t *")
local u32_ptr = ffi.typeof("uint32_t *")
local u64_ptr = ffi.typeof("uint64_t *")


local ptrof = setmetatable({}, {__index = function (t, k) t[k] = ffi.typeof('$ *', k); return t[k]; end})

local datacast_type_cache = setmetatable({}, {
   __index = function(t, k)
      -- k either 'uintXX_t' or ctype<unsigned XX>
      local ctype = ffi.typeof(k)
      t[k] = { ptrof[ctype], ffi.sizeof(ctype) }
      return t[k]
   end
})

local tuple_index = {
   field = function(self, i, level)
      assertarg(i, 'number', 1, level or 0)
      if i < 0 or i >= self.cardinality then
	 error('invalid field index', 2 + level or 0)
      end
      i = i * 2
      local j = #self.__cache - 1
      while j < i do
	 local offt = self.__cache[j] + self.__cache[j + 1]
	 local len, vlen = varint32.read(self.__tuple.data + offt)
	 self.__cache[j + 2], self.__cache[j + 3] = len, offt + vlen
	 j = j + 2
      end

      return self.__cache[i], self.__cache[i + 1]
   end,
   strfield = function(self, i)
      -- fixme: add check
      local len, offt = self:field(i, 1)
      self[i] = ffi.string(self.__tuple.data + offt, len)
      return self[i]
   end,
   numfield = function(self, i)
      local len, offt = self:field(i, 1)
      if len == 2 then
	 return tonumber(ffi.cast(u16_ptr, self.__tuple.data + offt)[0])
      elseif len == 4 then
	 return tonumber(ffi.cast(u32_ptr, self.__tuple.data + offt)[0])
      elseif len == 8 then
	 return ffi.cast(u64_ptr, self.__tuple.data + offt)[0]
      else
	 error('field length not equal to 2, 4 or 8', 2)
      end
   end,
   arrfield = function(self, i, ctype)
      local len, offt = self:field(i, 1)
      ctype = ffi.typeof(ctype)
      if len % ffi.sizeof(ctype) ~= 0 then
	 error('bad field len', 2)
      end
      local ptr = ffi.cast(ptrof[ctype], self.__tuple.data + offt)
      return safeptr(self.__obj, ptr, len / ffi.sizeof(ctype))
   end,
   datacast = function(self, ctype, offt, len)
      if ctype == 'string' then
	 if (offt < 0 or offt + len > self.__tuple.bsize) then
	    error("out of bounds", 2)
	 end
	 return ffi.string(self.__tuple.data + offt, len)
      elseif ctype == 'varint' then
	 if (offt < 0 or offt + 1 > self.__tuple.bsize) then
	    error("out of bounds", 2)
	 end
	 return varint32.read(self.__tuple.data + offt)
      else
	 local ctinfo = datacast_type_cache[ctype]
	 if offt < 0 or offt + ctinfo[2] > self.__tuple.bsize then
	    error("out of bounds", 2)
	 end
	 return ffi.cast(ctinfo[1], self.__tuple.data + offt)[0]
      end
   end
}
local tuple_mt = {
   __index = function(self, key)
      if type(key) == 'number' then
	 return self:strfield(key)
      else
	 return tuple_index[key]
      end
   end,
   __len = function(self)
      return self.cardinality
   end,
   __tostring = function(self)
      return tostring(self.__tuple)
   end
}

local box_tuple = ffi.typeof('struct box_tuple *')

object_cast[ffi.C.BOX_TUPLE] = function(obj)
   local tuple = ffi.cast(box_tuple, obj + 1) --  tuple starts right after 'struct tnt_object'
   local len0, offt0 = varint32.read(tuple.data)
   return setmetatable({ __obj = obj,
			 __tuple = tuple,
			 __cache = {[0] = len0, offt0}, -- {len0, offt0, len1, offt1, ...}
			 cardinality = tonumber(tuple.cardinality),
			 bsize = tonumber(tuple.bsize),},
		       tuple_mt)
end

local _dispatch = _dispatch
function dispatch(...)
    return object(_dispatch(...))
end

function select(n, ...)
        local index = object_space[n].index[0]
        local result = {}
        for k, v in pairs({...}) do
                result[k] = index[v]
        end
        return result
end

function add(n, ...)
    local flags = 3 -- return tuple + add tuple flags
    local req = packer()

    req:u32(n)
    req:u32(flags)
    req:u32(lselect('#', ...))
    for i = 1, lselect('#', ...) do
        req:field(lselect(i, ...))
    end
    return dispatch(13, req:pack())
end

function replace(n, ...)
        local flags = 1 -- return tuple
        local req = packer()

        req:u32(n)
        req:u32(flags)
        req:u32(lselect('#', ...))
        for i = 1, lselect('#', ...) do
            req:field(lselect(i, ...))
        end
        return dispatch(13, req:pack())
end

function delete(n, key)
        local key_len = 1
        local req = packer()

        req:u32(n)
        req:u32(key_len)
        req:field(key)
        dispatch(20, req:pack())
end

function update(n, key, ...)
        local ops = {...}
        local flags, key_cardinality = 1, 1
        local req = packer()

        req:u32(tonumber(n))
        req:u32(flags)
        req:u32(key_cardinality)
        req:field(key)
        req:u32(#ops)
        for k, op in ipairs(ops) do
                req:u32(op[1])
                if (op[2] == "set") then
                        req:u8(0)
                        req:field(op[3])
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
			req:varint(len)
			req:raw(buf, len)
                elseif (op[2] == "delete") then
                        req:string("\006\000")
                elseif (op[2] == "insert") then
                        req:u8(7)
                        req:field(op[3])
                end
        end
        return dispatch(19, req:pack())
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
