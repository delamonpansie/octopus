local ffi = require("ffi")
local bit = require('bit')
local C = ffi.C
require('cdef_base')
require('cdef')
require('packer')

local print_ = print
function print (...)
        for k, v in pairs({...}) do
                print_(tostring(v))
        end
end
function printf(...) print_(string.format(...)) end

-- prefer external files over bundled ones
package.loaders[2], package.loaders[1] = package.loaders[1], package.loaders[2]
-- .so exensions are currently unused - do not load them.
package.loaders[3] = nil
package.loaders[4] = nil

local type = type
function assertarg(arg, atype, n, level)
   if type(arg) == 'cdata' then
      if ffi.typeof(arg) == atype then
	 return
      end
   else
      if type(arg) == atype then
	 return
      end
   end
   local fname = debug.getinfo(2 + level or 0, "n").name
   local etype = type(arg) ~= 'cdata' and type(arg) or tostring(ffi.typeof(arg))
   local msg = string.format("bad argument #%s to '%s' (%s expected, got %s)",
			     n and tostring(n) or '?', fname, atype, etype)
   error(msg, 3 + level or 0)
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
   if value < 128 then
      ptr[0] = value
      return 1
   elseif value < 128*128 then
      ptr[0] = bit.bor(bit.rshift(value, 7), 0x80)
      ptr[1] = bit.band(value, 0x7f)
      return 2
   else
      local n = 3
      local c = bit.rshift(value, 21)
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
end

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

local safeptr_mt = {
    __index = function(self, i)
        if i < 1 or i > self.nelem then
            error('index out of bounds', 2)
        end
        return self.ptr[i-1]
    end,
    __len = function(self)
        return self.nelem
    end
}

function safeptr(object, ptr, nelem)
    return setmetatable({[0] = object, ptr = ptr, nelem = nelem}, safeptr_mt)
end

require('stat')
print("Lua prelude initialized.")
