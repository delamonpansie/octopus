local sddump = sddump

local assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable =
      assert, error, print, type, pairs, ipairs, table, setmetatable, getmetatable

local string, tostring, tonumber =
      string, tostring, tonumber

local rawget, rawset = rawget, rawset
local printf = printf
local say_error, say_warn = say_error, say_warn

local ffi, bit, debug = require("ffi"), require("bit"), require("debug")
local index = require('index')
local fiber = require("fiber")
local os = require('os')
local netmsg_ptr = require("net").netmsg_ptr
local object, varint32, packer = object, varint32, packer
local safeptr, assertarg = safeptr, assertarg
local lselect = select

local dyn_tuple = require 'box.dyn_tuple'
local box_op = require 'box.op'
require 'box.string_ext'
for k, v in pairs(require('box.cast')) do
    _G[k] = v
end

local add_stat_exec_lua = function(name) end
local add_stat_exec_lua_ok = function(name) end
local add_stat_exec_lua_rcode = function(name, rcode) end
if graphite then
    local add_stat = stat.request_collector{name = 'exec_lua'}
    graphite.add_cb('exec_lua')
    add_stat_exec_lua = add_stat.add_run
    add_stat_exec_lua_ok = add_stat.add_ok
    add_stat_exec_lua_rcode = add_stat.add_rcode
end

local _G = _G

module(...)

local function cache_call(table, k)
    local v = table[k]
    if v then return v end
    v = table.__ctor(k)
    table[k] = v
    return v
end
local function cache(fun)
    return setmetatable({__ctor = fun}, {__call = cache_call })
end

user_proc = {}

ffi.cdef[[
struct object_space {
	int n;
	bool ignored, snap, wal;
	int cardinality;
	size_t obj_bytes;
	size_t slab_bytes;
	const struct BasicIndex *index[10];
};
struct Box;
struct Box *shard_box(int n);
int    box_version(struct Box* box);
struct object_space *object_space(struct Box *box, int n);
bool   box_is_primary(struct Box *box);
int    shard_box_next_primary_n(int n);
extern const int object_space_max_idx;
]]

local _dispatch = _dispatch
local function dispatch(box, op, body, len)
    local obj = _dispatch(box, op, body, len)
    local tuple = object(obj)
    if tuple and ffi.C.object_type(obj) == ffi.C.BOX_TUPLE then
        ffi.C.object_decr_ref(obj)
    end
    return tuple
end

--- jit.off(_dispatch) not needed, because C API calls are NYI

local maxidx = tonumber(ffi.C.object_space_max_idx)

local object_space_mt = {
    __index = {
        index = function (self, i)
            i = tonumber(i)
            if self.__indexes[i] then
                return self.__indexes[i]
            end
            if i == nil or i < 0 or i >= maxidx or self.__ptr.index[i] == nil then
                error("no such index")
            end
            local ind = index.cast(self.__ptr.index[i])
            self.__indexes[i] = ind
            return ind
        end,
        size = function (self)
            return self:index(0):size()
        end,
        bytes = function (self)
            return tonumber(self.__ptr.obj_bytes)
        end,
        slab_bytes = function (self)
            return tonumber(self.__ptr.slab_bytes)
        end,
    },
    __tostring = function(self)
        return tostring(self.__ptr)
    end
}

for _, v in pairs{'add', 'replace', 'delete', 'update'} do
    local pack = box_op.pack[v]
    object_space_mt.__index[v .. '_ret'] = function (object_space, ...)
        return dispatch(object_space.__shard.__ptr, pack(1, object_space.n, ...))
    end
    object_space_mt.__index[v .. '_noret'] = function (object_space, ...)
        dispatch(object_space.__shard.__ptr, pack(0, object_space.n, ...))
    end
end
object_space_mt.__index.add     =     object_space_mt.__index.add_ret
object_space_mt.__index.replace =     object_space_mt.__index.replace_ret
object_space_mt.__index.update  =     object_space_mt.__index.update_ret
object_space_mt.__index.delete  =     object_space_mt.__index.delete_ret

object_space_mt.__index.select = function (self, ...)
    local index = self:object_space(self.n):index(0)
    local result = {}
    for k = 1, lselect('#', ...) do
        result[k] = index[lselect(k, ...)]
    end
    return result
end


local ushard_mt = {
    __index = {
        object_space = function (self, n)
            n = tonumber(n)
            if self.__spaces[n] ~= nil then
                return self.__spaces[n]
            end
            local ptr = ffi.C.object_space(self.__ptr, n)
            if ptr == nil or ptr.ignored then
                error("no such object space");
            end

            local space = setmetatable({__shard = self,
                                 __ptr = ptr,
                                 n = ptr.n,
                                 cardinality = ptr.cardinality,
                                 __indexes = {},
                                },
                object_space_mt)
            self.__spaces[n] = space
            return space
        end,
        is_primary = function(self)
            return ffi.C.box_is_primary(self.__ptr)
        end,
        __tostring = function(self)
            return tostring(self.__ptr)
        end
    }
}
for _, v in pairs{'add', 'replace', 'delete', 'update'} do
    local pack = box_op.pack[v]
    ushard_mt.__index[v..'_ret'] = function (shard, n, ...)
        return dispatch(shard.__ptr, pack(1, n, ...))
    end
    ushard_mt.__index[v..'_noret'] = function (shard, n, ...)
        dispatch(shard.__ptr, pack(0, n, ...))
    end
end

ushard_mt.__index.add     = ushard_mt.__index.add_ret
ushard_mt.__index.replace = ushard_mt.__index.replace_ret
ushard_mt.__index.update  = ushard_mt.__index.update_ret
ushard_mt.__index.delete  = ushard_mt.__index.delete_ret

local ushards = {}
local ushard = function(i)
    i = tonumber(i)
    local ptr = ffi.C.shard_box(i)
    if ptr == nil then
        error("shard "..i.." is not defined")
    end
    local version = ffi.C.box_version(ptr)
    local cached = ushards[i]
    if cached == nil or cached.__version ~= version then
        ushards[i] = setmetatable({
            __ptr = ffi.C.shard_box(i),
            __spaces = {},
            __version = version
        }, ushard_mt)
    end
    return ushards[i]
end
_M.ushard = ushard

function _M.next_primary_ushard_n(n)
    return ffi.C.shard_box_next_primary_n(n)
end

do
    local ts = os.ev_now()
    local function deprecate(name)
        if os.ev_now() > ts + 10 then
            say_warn("box."..name.." is deprecated")
            ts = os.ev_now()
        end
    end
    local function cur_ushard()
        return ushard(ffi.C.current_fiber().ushard)
    end

    local deprecated = {'select', 'add', 'replace', 'delete', 'update',
                        'add_ret', 'replace_ret', 'delete_ret', 'update_ret',
                        'add_noret', 'replace_noret', 'delete_noret', 'update_noret'}
    for _, name in ipairs(deprecated) do
        _M[name] = function (...)
            deprecate(name)
            local sh = cur_ushard()
            return sh[name](sh, ...)
        end
    end
    local object_space = setmetatable({},
        { __index = function (t, i)
            deprecate("object_space")
            local sh = cur_ushard()
            return sh:object_space(i)
        end}
    )
    for _, name in ipairs{'object_space_registry', 'space', 'object_space'} do
        _M[name] = object_space
    end
    function _M.set_ushard(nom)
        ffi.C.current_fiber().ushard = nom
    end
end

function ctuple(obj)
   assert(obj ~= nil and obj.__tuple ~= nil)
   return obj.__tuple
end
local tuple_mt = {}
function tuple(...)
    local farg = lselect(1, ...)
    if (type(farg) == 'table') then
        return setmetatable(farg, tuple_mt)
    else
        return setmetatable({...}, tuple_mt)
    end
end


local wrapped = setmetatable({}, {__mode = "k"})
function wrap(proc_body)
        if type(proc_body) ~= "function" then
                return nil
        end
        wrapped[proc_body] = true

        return proc_body
end

function wrap_old(proc_body)
        if type(proc_body) ~= "function" then
                return nil
        end
        wrapped[proc_body] = 'old'

        return proc_body
end

local u32ptr = ffi.typeof('uint32_t *')
local uintptr = ffi.typeof('uintptr_t')
local u32buf = ffi.new('uint32_t[1]')
local p = packer()

ffi.cdef 'void net_tuple_add(struct netmsg_head *h, struct tnt_object *obj);'

local function append_value(out, v)
    if type(v) == "string" then
        out:add_iov_string(v)
    elseif type(v) == "table" and v.__obj then
        ffi.C.net_tuple_add(out, v.__obj)
    elseif type(v) == "table" and getmetatable(v) == tuple_mt then
        p:string("....----") -- placeholder for bsize, cardinality
        for i = 1, #v do
            p:field(v[i])
        end
        local u32 = ffi.cast(u32ptr, p.ptr)
        u32[0] = p:len() - 8 -- bsize adjust
        u32[1] = #v
        out:add_iov_dup(p:pack())
    else
        error("unexpected type of result: " .. type(v), 2)
    end
end

local function append(result, out)
    out = netmsg_ptr(out)
    p:reset()

    if type(result) == "table" then
        u32buf[0] = #result
        out:add_iov_dup(u32buf, 4)

        if #result == 1 then
            append_value(out, result[1])
        elseif #result == 2 then
            append_value(out, result[1])
            append_value(out, result[2])
        elseif #result == 3 then
            append_value(out, result[1])
            append_value(out, result[2])
            append_value(out, result[3])
        else
            for _, v in ipairs(result) do
                append_value(out, v)
            end
        end
    elseif type(result) == "number" then
        out:add_iov_string(string.tou32(result))
    else
        error("unexpected type of result: " .. type(result), 2)
    end
end

local fn_cache_mt = {__index = function(t, name)
    local fn = _G
    for k in name:gmatch('[^%.]+') do
       fn = fn[k]
       if not fn then
           error("function '"..name.."' not found")
       end
    end
    if type(fn) ~= 'function' then
       error("'"..name.."' is not a function")
    end
    t[name] = fn
    return fn
end}
local fn_cache = setmetatable({}, fn_cache_mt)
local function clear_cache()
    while true do
        fiber.sleep(1)
        fn_cache = setmetatable({}, fn_cache_mt)
    end
end
fiber.create(clear_cache)

function entry(name, wbuf, request, shard_id, ...)
    add_stat_exec_lua(name)

    local proc = fn_cache[name]
    local w = wrapped[proc]
    if w then
        local rcode, res
        if w == true then
            rcode, res = proc(ushard(shard_id), ...)
        else
            rcode, res = proc(...)
        end
        add_stat_exec_lua_rcode(name, rcode)
        return append, rcode, res
    end
    add_stat_exec_lua("NotWrapped")
    add_stat_exec_lua(name..":NotWrapped")
    proc(netmsg_ptr(wbuf), request, ...)
    add_stat_exec_lua_ok(name)
end


