local ffi = require 'ffi'
local setmetatable = setmetatable
local newproxy = newproxy
local pairs, ipairs, error, xpcall = pairs, ipairs, error, xpcall
local traceback, cut_traceback = debug.traceback, cut_traceback
local say_error = say_error

module(...)
local onlineconf = _M

ffi.cdef[[
struct ckv_str {
	char const * str;
	int len;
};
int onlineconf_get(const char* name, const char* key, struct ckv_str* result);
int onlineconf_get_json(const char* name, const char* key, struct ckv_str* result);
int onlineconf_geti(const char* name, const char* key, int _default);
bool onlineconf_get_bool(const char* name, const char* key);
bool onlineconf_registered(const char* name);
]]

local result = ffi.new('struct ckv_str[1]');

function onlineconf.get(key)
    if ffi.C.onlineconf_get(nil, key, result) == 1 then
        return ffi.string(result[0].str, result[0].len)
    else
        return nil
    end
end

function onlineconf.json_raw(key)
    if ffi.C.onlineconf_get_json(nil, key, result) == 1 then
        return ffi.string(result[0].str, result[0].len)
    else
        return nil
    end
end

function onlineconf.geti(key, default)
    return ffi.C.onlineconf_geti(nil, key, default or -1)
end

function onlineconf.bool(key)
    return ffi.C.onlineconf_get_bool(nil, key)
end

local kv_key = newproxy()
local kv_mt = {
    __metatable = "onlineconf",
    __index = {
        get = function(t, key)
            if ffi.C.onlineconf_get(t[kv_key], key, result) == 1 then
                return ffi.string(result[0].str, result[0].len)
            else
                return nil
            end
        end,
        json_raw = function(t, key)
            if ffi.C.onlineconf_get_json(t[kv_key], key, result) == 1 then
                return ffi.string(result[0].str, result[0].len)
            else
                return nil
            end
        end,
        geti = function(t, key, default)
            return ffi.C.onlineconf_geti(t[kv_key], key, default or -1)
        end,
        bool = function(t, key)
            return ffi.C.onlineconf_get_bool(t[kv_key], key)
        end
    },
    __newindex = function(t, k, v)
        error('onlineconf namespace immutable')
    end
}

local callbacks = {}
function onlineconf.register_callback(name, callback_name, callback)
    if name == nil then name = '' end
    if not ffi.C.onlineconf_registered(name) then
        error("onlineconf namespace '"..name.."' is not registered")
    end
    if callbacks[name] == nil then
        callbacks[name] = {}
    end
    callbacks[name][callback_name] = callback
end

function onlineconf.__call_callbacks(name)
    local kv = onlineconf.additional(name)
    local cbs = callbacks[name]
    if cbs == nil then return end
    local cbs_f = {}
    for _, f in pairs(cbs) do
        cbs_f[#cbs_f+1] = f
    end
    for _, f in ipairs(cbs_f) do
        local ok, trace = xpcall(f, traceback, name, kv)
        if not ok then
            say_error(cut_traceback(trace))
        end
    end
end

function onlineconf.additional(name)
    if not ffi.C.onlineconf_registered(name) then
        error("onlineconf namespace '"..name.."' is not registered")
    end
    return setmetatable({[kv_key]=name}, kv_mt)
end

setmetatable(onlineconf, {
    __index = function (t, name)
        if not ffi.C.onlineconf_registered(name) then
            error("onlineconf namespace '"..name.."' is not registered")
        end
        return setmetatable({[kv_key]=name}, kv_mt)
    end,
    __newindex = function (t, name, v)
        error("onlineconf module immutable")
    end
});

return onlineconf
