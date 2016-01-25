local ffi = require 'ffi'
local setmetatable = setmetatable
local newproxy = newproxy
local pairs, ipairs, error, xpcall = pairs, ipairs, error, xpcall
local traceback, cut_traceback = debug.traceback, cut_traceback
local say_error = say_error

module(...)
local ckv = _M

ffi.cdef[[
struct ckv_str {
	char const * str;
	int len;
};
int constant_kv_get(const char* name, const char* key, int key_len, struct ckv_str* result, struct ckv_str* format);
int constant_kv_geti(const char* name, const char* key, int key_len, int _default);
bool constant_kv_registered(const char* name);
]]

local result = ffi.new('struct ckv_str[2]');

function ckv.get(name, key)
    local r = ffi.C.constant_kv_get(name, key, #key, result, result+1)
    if r == 0 then
        return ffi.string(result[0].str, result[0].len),
                ffi.string(result[1].str, result[1].len)
    elseif r == 1 then
        return nil, nil
    else
        error("constant_kv '"..name.."' is not registered")
    end
end

function ckv.geti(name, key, default)
    default = default or -1
    return ffi.C.constant_kv_geti(name, key, #key, default)
end

local kv_key = newproxy()
local kv_mt = {
    __metatable = "ckv",
    __index = {
        get = function(t, k)
            return ckv.get(t[kv_key], k)
        end,
        geti = function(t, k, default)
            return ckv.geti(t[kv_key], k, default)
        end
    },
    __newindex = function(t, k, v)
        error('constant_kv immutable')
    end
}

function ckv.kv(name)
    local t = setmetatable({[kv_key]=name}, kv_mt)
    return t
end

local callbacks = {}
function ckv.register_callback(name, callback_name, callback)
    if not ffi.C.constant_kv_registered(name) then
        error("constant_kv '"..name.."' is not registered")
    end
    if callbacks[name] == nil then
        callbacks[name] = {}
    end
    callbacks[name][callback_name] = callback
end

function ckv.__call_callbacks(name)
    local kv = ckv.kv(name)
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
