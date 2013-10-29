local ffi = require('ffi')
local objc = require('objc')
local object, varint32 = object, varint32

local setmetatable = setmetatable
local assert = assert
local tonumber = tonumber
local error = error
local type = type
local tostring = tostring

local print = print
module(...)

ffi.cdef[[
struct OpaqueIndex;
struct Index {
	struct { void *isa; };
	const struct index_conf conf;
};
struct HashIndex {
	struct { void *isa; };
	const struct index_conf conf;
};
]]

local find = objc.msg_lookup('find:')
local get = objc.msg_lookup('get:')
local iterator_init = objc.msg_lookup("iterator_init")
local iterator_init_with_cardinality = objc.msg_lookup("iterator_init:with_cardinalty:")
local iterator_init_with_object = objc.msg_lookup("iterator_init_with_object:")
local iterator_next = objc.msg_lookup("iterator_next")

local index = ffi.typeof('struct Index *')
local hash = ffi.typeof('struct HashIndex *')
local opaque = ffi.typeof('struct OpaqueIndex *')

local charbuf = ffi.typeof('unsigned char[?]')
local ptr = setmetatable({}, {__index = function(t, k) t[k] = ffi.typeof('$ *', k); return t[k]; end})
local function numkey(key, itype, utype)
   local size = ffi.sizeof(itype)

   local function pack(v)
      local pkey = ffi.new(charbuf, size + 1)
      pkey[0] = size
      ffi.cast(ptr[itype], pkey + 1)[0] = key
      return pkey, size + 1
   end

   if type(key) == 'number' then
      return pack(key)
   elseif type(key) == 'string' then
      -- if first byte is 2, 4 or 8 then string contains packed field representation
      if key:byte(1) == size then
	 if #key ~= size + 1 then
	    error('bad packed repr')
	 end
	 local pkey = ffi.new(charbuf, #key) -- make mutable copy
	 ffi.copy(pkey, key, #key)
	 return pkey, size + 1
      else
	 return pack(tonumber(key))
      end
   elseif type(key) == 'cdata' then
      local ktype = ffi.typeof(key)
      if ktype == itype then
	 return pack(key)
      elseif ktype == utype then
	 return pack(ffi.cast(utype, key))
      else
	 return key, size + 1
      end
   else
      error('bad key type: ' .. tostring(type(key)), 2)
   end
end

local function strkey(key)
   local p = ffi.new(charbuf, 5 + #key)
   local n = varint32.write(p, #key)
   ffi.copy(p + n, key, #key)
   return p, n + #key
end

local int16, int32, int64 = ffi.typeof('int16_t'), ffi.typeof('int32_t'), ffi.typeof('int64_t')
local uint16, uint32, uint64 = ffi.typeof('uint16_t'), ffi.typeof('uint32_t'), ffi.typeof('uint64_t')
local function xkey(index, key)
   if index.conf.field_type[0] == ffi.C.NUM16 then
      return numkey(key, int16, uint16)
   elseif index.conf.field_type[0] == ffi.C.NUM32 then
      return numkey(key, int32, uint32)
   elseif index.conf.field_type[0] == ffi.C.NUM64 then
      return numkey(key, int64, uint64)
   elseif index.conf.field_type[0] == ffi.C.STRING then
      return strkey(key)
   else
      error("unknown index type")
   end
end

local function unlegacy(self)
   assert(ffi.typeof(self) == opaque)
   self = ffi.cast(hash, self)
   if self.conf.type ~= ffi.C.HASH then
      self = ffi.cast(index, self)
   end
   return self
end

local legacy_mt = {
   __index = function(index, key)
      index = unlegacy(index)
      return index:find(key)
   end,
   __metatable = {}
}

local function iter_next(index)
   return object(iterator_next(index))
end

local index_mt = {
   __index = {
      find = function(index, ...)
	 if index.conf.cardinality ~= 1 then
	    error('not implemented', 2)
	 end

	 local key = xkey(index, ...)
	 return object(find(index, key))
      end,
      iter = function(index, init)
	 if type(init) == 'nil' then
	    iterator_init(index)
	 elseif type(init) == 'number' or type(init) == 'string' then
	    if index.conf.cardinality ~= 1 then
	       error('cardinality mismatch', 2)
	    end
	    local key, len = xkey(index, init)
	    local t = ffi.new('struct tbuf', key, ffi.cast('uint8_t *', key) + len)
	    iterator_init_with_cardinality(index, t, ffi.cast('int', 1))
	 elseif type(init) == 'table' and init.__obj then
	    iterator_init_with_object(index, init.__obj)
	 else
	    error("wrong key type", 2)
	 end
	 return iter_next, index
      end
   },
   __metatable = {}
}

local int32_t = ffi.typeof('int32_t')
local hash_mt = {
   __index = {
      find = index_mt.__index.find,
      iter = index_mt.__index.iter,
      get = function(index, i)
	 assert(type(i) == 'number')
	 return object(get(index, ffi.cast(int32_t, i)))
      end,
      slots = function(index)
	 return tonumber(obj.msg_send("slots", index))
      end
   },
   __metatable = {}
}

ffi.metatype('struct OpaqueIndex', legacy_mt)
ffi.metatype('struct Index', index_mt)
ffi.metatype('struct HashIndex', hash_mt)

function cast(ptr)
   if ptr.conf.type == ffi.C.HASH then
      index = ffi.cast(hash, ptr)
   end
   local legacy = ffi.cast('struct OpaqueIndex *', ptr)
   return legacy, index
end

function iter(index, init)
   if ffi.typeof(index) == opaque then
      index = unlegacy(index)
   end
   return index:iter(init)
end