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


local ptrof = setmetatable({}, {__index = function (t, k)
    if type(k) ~= "string" then
        return ffi.typeof('$ *', k)
    end
    t[k] = ffi.typeof('$ *', ffi.typeof(k))
    return t[k]
end})

local datacast_type_cache = setmetatable({}, {
   __index = function(t, k)
      if type(k) ~= "string" then
         return { ffi.typeof('$ *', k), ffi.sizeof(k) }
      end
      -- k either 'uintXX_t' or ctype<unsigned XX>
      local ctype = ffi.typeof(k)
      t[k] = { ptrof[k], ffi.sizeof(ctype) }
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
      return self.__cache[i], self.__cache[i + 1]
   end,
   strfield = function(self, i)
      -- fixme: add check
      local len, offt = self:field(i, 1)
      self[i] = ffi.string(self.data + offt, len)
      return self[i]
   end,
   u16field = function(self, i)
       local len, offt = self:field(i, 1)
       if len ~= 2 then error('field level not equal to 2', 2) end
       return ffi.cast(u16_ptr, self.data + offt)[0]
   end,
   u32field = function(self, i)
       local len, offt = self:field(i, 1)
       if len ~= 4 then error('field level not equal to 4', 2) end
       return ffi.cast(u32_ptr, self.data + offt)[0]
   end,
   u64field = function(self, i)
       local len, offt = self:field(i, 1)
       if len ~= 8 then error('field level not equal to 8', 2) end
       return ffi.cast(u64_ptr, self.data + offt)[0]
   end,
   numfield = function(self, i)
      local len, offt = self:field(i, 1)
      if len == 2 then
	 return tonumber(ffi.cast(u16_ptr, self.data + offt)[0])
      elseif len == 4 then
	 return tonumber(ffi.cast(u32_ptr, self.data + offt)[0])
      elseif len == 8 then
	 return ffi.cast(u64_ptr, self.data + offt)[0]
      else
	 error('field length not equal to 2, 4 or 8', 2)
      end
   end,
   arrfield = function(self, i, ctype)
      local len, offt = self:field(i, 1)
      local ctinfo = datacast_type_cache[ctype]
      if len % ctinfo[2] ~= 0 then
	 error('bad field len', 2)
      end
      local ptr = ffi.cast(ctinfo[1], self.data + offt)
      return safeptr(self.__obj, ptr, len / ctinfo[2])
   end,
   datacast = function(self, ctype, offt, len)
      if ctype == 'string' then
	 if (offt < 0 or offt + len > self.bsize) then
	    error("out of bounds", 2)
	 end
	 return ffi.string(self.data + offt, len)
      elseif ctype == 'varint32' then
	 if (offt < 0 or offt + 1 > self.bsize) then
	    error("out of bounds", 2)
	 end
	 local val, valn = varint32.read(self.data + offt)
	 return val, offt + valn
      else
	 local ctinfo = datacast_type_cache[ctype]
	 if offt < 0 or offt + ctinfo[2] > self.bsize then
	    error("out of bounds", 2)
	 end
	 return ffi.cast(ctinfo[1], self.data + offt)[0]
      end
   end,
   raw_box_tuple = function(self) return self.__tuple end,
   make_long_living = function(self)
       if not self._long_living then
           ffi.gc(self.__obj, ffi.C.object_decr_ref)
           ffi.C.object_incr_ref(self.__obj)
           self._long_living = true
       end
   end,
   make_short_living = function(self)
       if self._long_living then
           ffi.gc(self.__obj, nil)
           ffi.C.object_incr_ref_autorelease(self.__obj)
           self._long_living = nil
       end
   end,
}

local tuple_mt = {
   __index = function(self, key)
      if type(key) == 'number' then
	 return self:strfield(key)
      else
	 return __tuple_index[key]
      end
   end,
   __len = function(self) -- won't work until -DLUAJIT_ENABLE_LUA52COMPAT enabled
      return self.cardinality
   end,
   __tostring = function(self)
      return tostring(self.__tuple or self.data)
   end
}

local box_tuple = ffi.typeof('const struct box_tuple *')
ffi.cdef 'u32 *box_tuple_cache_update(int cardinality, const unsigned char *data);' -- palloc allocated!
obj_type = ffi.C.BOX_TUPLE
obj_cast = function(obj)
    local tuple = ffi.cast(box_tuple, obj + 1) --  tuple starts right after 'struct tnt_object'
    return setmetatable({ __obj = obj,
                          __tuple = tuple,
                          __cache = ffi.C.box_tuple_cache_update(tuple.cardinality, tuple.data),
                          cardinality = tonumber(tuple.cardinality),
                          data = tuple.data,
                          bsize = tonumber(tuple.bsize)},
                        tuple_mt)
end


function new(obj, cardinality, data, bsize)
    local len0, offt0 = varint32.read(data)
    return setmetatable({ __obj = obj,
                          __cache = ffi.C.box_tuple_cache_update(cardinality, data),
                          cardinality = cardinality,
                          data = data,
                          bsize = bsize},
                        tuple_mt)
end
