local ffi = require("ffi")
local bit = require('bit')
local C = ffi.C
require('cdef_base')
require('cdef')
require('packer')
ddump = require('ddump')
make_repl_env = require 'repl'

function os.ev_time()
    return C.ev_time()
end

function os.ev_now()
    return C.ev_rt_now
end

local print_ = print
function print (...)
    local s = {}
    for i=1,select('#', ...) do
        local v = select(i, ...)
        s[#s+1] = tostring(v) or 'nil'
    end
    print_(table.concat(s, '\t'))
end
local format = string.format
function printf(...) print_(format(...)) end

-- prefer external files over bundled ones
package.loaders[2], package.loaders[1] = package.loaders[1], package.loaders[2]
-- .so exensions are currently unused - do not load them.
package.loaders[3] = nil
package.loaders[4] = nil

local type, getmetatable = type, getmetatable
function assertarg(arg, expected, n, level)
    local argtype = type(arg)
    if argtype == 'cdata' then
      if ffi.typeof(arg) == expected then
	 return
      end
   elseif type(expected) == 'table' then
       if argtype == 'table' and getmetatable(arg) == expected then
           return
       end
   else
      if argtype == expected then
	 return
      end
   end
   local fname = debug.getinfo(2 + (level or 0), "n").name
   local etype = type(arg) ~= 'cdata' and type(arg) or tostring(ffi.typeof(arg))
   local msg = string.format("bad argument #%s to '%s' (%s expected, got %s)",
			     n and tostring(n) or '?', fname, expected, etype)
   error(msg, 3 + (level or 0))
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

function say(level, filename, line, fmt, ...)
    if level < say_level.ERROR or level > say_level.DEBUG3 then
        error('bad log level', 2)
    end
    ffi.C._say(level, filename, line, "%s", format(fmt, ...))
end
for _, levelstr in ipairs({"ERROR", "WARN", "INFO", "DEBUG", "DEBUG2", "DEBUG3"}) do
    local level = ffi.C[levelstr]
    _G[("say_%s"):format(levelstr:lower())] = function (fmt, ...)
        -- 'if' required because debug.getinfo disables JIT
        if ffi.C.max_level >= level then
            local dinfo = debug.getinfo(2, "Sl")
            local filename, line = dinfo.short_src, dinfo.currentline
            ffi.C._say(level, filename, line, "%s", format(fmt, ...))
        end
    end
end

function palloc(size)
    return ffi.C.palloc(ffi.C.fiber.pool, size)
end

function cut_traceback(deep)
    local current = debug.traceback('', 2)
    local last_line_match = #deep
    for i=1, #current do
        if current:byte(-i) ~= deep:byte(-i) then
            break
        end
        if current:byte(-i) == 10 then
            last_line_match = #deep - i
        end
    end
    return deep:sub(1, last_line_match)
end

require('stat')
require('fiber_lock')
require('reloadfile')
print("Lua prelude initialized.")
