local ffi = require('ffi')
local ac = require('cdef').autoconf
local setmetatable = setmetatable
local error = error
module(...)

ffi.cdef[[
typedef const struct objc_selector *SEL;
typedef struct objc_object *id;
typedef struct objc_method *Method;
typedef id (*IMP)(id, SEL, ...);

SEL sel_registerName (const char *name);
SEL sel_register_name(const char *name);
IMP objc_msg_lookup (id receiver, SEL op);
id objc_msgSend(id self, SEL op, ...);
]]

local id = ffi.typeof('id')
local ic
if ac.HAVE_OBJC_RUNTIME_H then
    ic = function (t,k)
        t[k] = ffi.C.sel_registerName(k)
        return t[k]
    end
elseif ac.HAVE_OBJC_OBJC_API_H then
    ic = function (t,k)
        t[k] = ffi.C.sel_register_name(k)
        return t[k]
    end
else
    error("unknown ObjC runtime")
end
local cache = setmetatable({}, {__index = ic})

if ac.OBJC_GNU_RUNTIME then
    function msg_send(receiver, selector, ...)
        local sel = cache[selector]
        receiver = ffi.cast(id, receiver)
        return ffi.C.objc_msg_lookup(receiver, sel)(receiver, sel, ...)
    end

    function msg_lookup(selector)
        local sel = cache[selector]
        return function (receiver, ...)
            receiver = ffi.cast(id, receiver)
            return ffi.C.objc_msg_lookup(receiver, sel)(receiver, sel, ...)
        end
    end
elseif ac.OBJC_APPLE_RUNTIME then
    function msg_send(receiver, selector, ...)
        local sel = cache[selector]
        receiver = ffi.cast(id, receiver)
        return ffi.C.objc_msgSend(receiver, sel, ...)
    end

    function msg_lookup(selector)
        local sel = cache[selector]
        return function (receiver, ...)
            receiver = ffi.cast(id, receiver)
            return ffi.C.objc_msgSend(receiver, sel, ...)
        end
    end
else
    error("unknown ObjC runtime")
end