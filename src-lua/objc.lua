local ffi = require('ffi')

module(...)

ffi.cdef[[
typedef const struct objc_selector *SEL;
typedef struct objc_object *id;
typedef struct objc_method *Method;
typedef id (*IMP)(id, SEL, ...);

SEL sel_registerName (const char *name);
IMP objc_msg_lookup (id receiver, SEL op);
]]

local id = ffi.typeof('id')
local cache = {}

local function selcache(selectorname)
   local sel = cache[selectorname]
   if not sel then
      sel = ffi.C.sel_registerName(selectorname)
      cache[selectorname] = sel
   end
   return sel
end

function msg_send(receiver, selector, ...)
   local sel = selcache(selector)
   receiver = ffi.cast(id, receiver)
   return ffi.C.objc_msg_lookup(receiver, sel)(receiver, sel, ...)
end

function msg_lookup(selector)
   local sel = selcache(selector)
   return function (receiver, ...)
      receiver = ffi.cast(id, receiver)
      return ffi.C.objc_msg_lookup(receiver, sel)(receiver, sel, ...)
   end
end