local ffi = require 'ffi'
local varint32 = varint32

-- legacy, slow because of string interning
ffi.cdef[[ typedef union {
    char ch[8];
    u8 u8; u16 u16; u32 u32; u64 u64;
    i8 i8; i16 i16; i32 i32; i64 i64;
} pack_it_gently ]]
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
string.getu8 = function(s, i)
    if not i then i = 1 end
    return s:byte(i)
end
string.getu16 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1] = s:byte(i, i+1)
    return ptg.u16
end
string.getu32 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1], ptg.ch[2], ptg.ch[3] = s:byte(i, i+3)
    return ptg.u32
end
string.getu64 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1], ptg.ch[2], ptg.ch[3] = s:byte(i, i+3)
    ptg.ch[4], ptg.ch[5], ptg.ch[6], ptg.ch[7] = s:byte(i+4, i+7)
    return ptg.u64
end
string.geti8 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0] = s:byte(i)
    return ptg.i8
end
string.geti16 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1] = s:byte(i, i+1)
    return ptg.i16
end
string.geti32 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1], ptg.ch[2], ptg.ch[3] = s:byte(i, i+3)
    return ptg.i32
end
string.geti64 = function(s, i)
    if not i then i = 1 end
    ptg.ch[0], ptg.ch[1], ptg.ch[2], ptg.ch[3] = s:byte(i, i+3)
    ptg.ch[4], ptg.ch[5], ptg.ch[6], ptg.ch[7] = s:byte(i+4, i+7)
    return ptg.i64
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
