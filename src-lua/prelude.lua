local ffi = require("ffi")
local bit = require('bit')
require('cdef_base')
require('cdef')

local print_ = print
function print (...)
        for k, v in pairs({...}) do
                print_(tostring(v))
        end
end
function printf(...) return print_(string.format(...)) end

-- prefer external files over bundled ones
package.loaders[2], package.loaders[1] = package.loaders[1], package.loaders[2]
-- .so exensions are currently unused - do not load them.
package.loaders[3] = nil
package.loaders[4] = nil

local type = type
local function assertarg(arg, atype, n)
   if type(arg) == 'cdata' then
      if ffi.typeof(arg) == atype then
	 return
      end
   else
      if type(arg) == atype then
	 return
      end
   end
   local fname = debug.getinfo(2, "n").name
   local etype = type(arg) ~= 'cdata' and type(arg) or tostring(ffi.typeof(arg))
   local msg = string.format("bad argument #%s to '%s' (%s expected, got %s)",
			     n and tostring(n) or '?', fname, atype, etype)
   error(msg, 3)
end

function reloadfile(filename)
	assertarg(filename, 'string', 1)

        local function print_warn(msg)
                print(string.format("reloadfile(\"%s\"): %s", filename, msg))
        end

        local function require(filename)
                local modulename = string.gsub(string.gsub(filename, "^.*/", ""), "%.lua$", "")
                local module, err = loadfile(filename)
                if module == nil then
                        print_warn(err)
                        return
                end
                package.loaded[module] = module(modulename)
        end
        local function reload_loop()
                local tm = 0
                while true do
                        local r, v = pcall(os.ctime, filename)
                        if r then
                                if v > tm then
                                        local r, err = pcall(require, filename)
                                        if r then
                                                tm = v
                                        else
                                                print_warn(err)
                                        end
                                end
                        else
                                print_warn(v)
                        end
                        fiber.sleep(1)
                end
        end
        -- do all loading from fiber, since loaded code may call fiber.sleep()
        return fiber.create(reload_loop)
end

varint32 = {}
function varint32.read(ptr)
   local result = 0
   for i = 0,4 do
      result = bit.bor(bit.lshift(result, 7),
		       bit.band(ptr[i], 0x7f))

      if bit.band(ptr[i], 0x80) == 0 then
	 return result, i + 1
      end
   end
   error('bad varint32')
end

local charptr = ffi.typeof('uint8_t *')
function varint32.write(ptr, value)
   ptr = ffi.cast(charptr, ptr)
   local n = 1
   local c = bit.rshift(value, 7)
   while c > 0 do
      c = bit.rshift(c, 7)
      n = n + 1
   end

   ptr[n - 1] = bit.band(value, 0x7f)
   value = bit.rshift(value, 7)
   for i = n - 2, 0, -1 do
      ptr[i] = bit.bor(bit.band(value, 0x7f), 0x80)
      value = bit.rshift(value, 7)
   end
   return n
end

local safeptr_mt = {
   __index = function(self, i)
      if i < 0 or i >= self.nelem then
	 error('index out of bounds', 2)
      end
      return self.ptr[i]
   end,
   __gc = function(self)
      ffi.C.object_decr_ref(self.obj)
   end
}
local safeptr_ctcache =
   setmetatable({}, {__index = function(t, k)
			t[k] = ffi.typeof('struct { struct tnt_object *obj; $ ptr; int nelem; }', k)
			ffi.metatype(t[k], safeptr_mt)
			return t[k]
			end
		    })

function safeptr(object, ptr, nelem)
   local ctype = safeptr_ctcache[typeof(ptr)]
   ffi.C.object_incr_ref(object)
   return ffi.new(ctype, object, ptr, nelem)
end


require('stat')

object_cast = {}
local object_t = ffi.typeof('struct tnt_object *')
function object(ptr)
   if ptr == nil then
      return nil
   end
   local obj = ffi.cast(object_t, ptr)
   if bit.band(obj.flags, ffi.C.GHOST) ~= 0 then
      return nil
   end

   ffi.gc(obj, ffi.C.object_decr_ref)
   ffi.C.object_incr_ref(obj)

   local ct = object_cast[obj.type]
   if ct then
      return ct(obj)
   else
      return obj
   end
end

local pack_mt = {
   __index = {
      u8 = function(self, i)
	 table.insert(self, ffi.new('uint8_t[1]', tonumber(i)))
	 table.insert(self, 1)
	 self.len = self.len + 1
      end,
      u16 = function(self, i)
	 table.insert(self, ffi.new('uint16_t[1]', tonumber(i)))
	 table.insert(self, 2)
	 self.len = self.len + 2
      end,
      u32 = function(self, i)
	 table.insert(self, ffi.new('uint32_t[1]', tonumber(i)))
	 table.insert(self, 4)
	 self.len = self.len + 4
      end,
      u64 = function(self, i)
	 table.insert(self, ffi.new('uint64_t[1]', tonumber(i)))
	 table.insert(self, 8)
	 self.len = self.len + 8
      end,
      varint32 = function(self, i)
	 local buf = ffi.new('char[5]')
	 local len = varint32.write(buf, tonumber(i))
	 table.insert(self, buf)
	 table.insert(self, len)
	 self.len = self.len + len
      end,
      string = function(self, s)
	 table.insert(self, s)
	 table.insert(self, #s)
	 self.len = self.len + #s
      end,
      field = function(self, s)
	 self:varint32(#s)
	 self:string(s)
      end,
      raw = function(self, v, l)
	 table.insert(self, v)
	 table.insert(self, l)
	 self.len = self.len + l
      end,
      pack = function(self)
	 local buf = ffi.new('char[?]', self.len)
	 local offt = 0
	 for i=1,#self, 2 do
	    ffi.copy(buf + offt, self[i], self[i+1])
	    offt = offt + self[i+1]
	 end
	 return buf, self.len
      end
   }
}

local setmetatable = setmetatable
function packer ()
   return setmetatable({["len"] = 0}, pack_mt)
end

print("Lua prelude initialized.")
