local ffi = require 'ffi'

local setmetatable = setmetatable
local type = type
local error = error
local tonumber, tostring = tonumber, tostring
local assertarg = assertarg
local varint32 = varint32
local safeptr = safeptr

module(...)

ffi.cdef [[
enum object_type {
	BOX_TUPLE = 1
};

struct box_tuple {
	uint32_t bsize; /* byte size of data[] */
	uint32_t cardinality;
	uint8_t data[0];
} __attribute__((packed));

]]

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

local __tuple_index = {
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
	 return __tuple_index[key]
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

obj_type = ffi.C.BOX_TUPLE
obj_cast = function(obj)
    local tuple = ffi.cast(box_tuple, obj + 1) --  tuple starts right after 'struct tnt_object'
    local len0, offt0 = varint32.read(tuple.data)
    return setmetatable({ __obj = obj,
                          __tuple = tuple,
                          __cache = {[0] = len0, offt0}, -- {len0, offt0, len1, offt1, ...}
                          cardinality = tonumber(tuple.cardinality),
                          bsize = tonumber(tuple.bsize),},
                        tuple_mt)
end

