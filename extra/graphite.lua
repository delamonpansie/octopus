--
-- this script will periodically send stats to graphite
-- to enable: put it to cfg.workdir, and add
-- require('graphite.lua').start('1.2.3.4:2003') to init.lua
--

local ffi = require 'ffi'
local fiber = require 'fiber'
require 'net' -- for ffi.cdef
local xpcall, type, collectgarbage = xpcall, type, collectgarbage
local traceback = debug.traceback
local print, tostring, tonumber = print, tostring, tonumber
local string, table, pairs, ipairs = string, table, pairs, ipairs
local stat = stat
local _ev_now = os.ev_now

module(...)

ffi.cdef[[
const char *octopus_version(void);
extern int gethostname(char *name, size_t len);
]]

local function gethostname()
    local buf = ffi.new('char[?]', 64)
    local result = "unknown"
    if ffi.C.gethostname(buf, ffi.sizeof(buf)) ~= -1 then
        result = ffi.string(buf)
    end
    if ffi.C.cfg.primary_addr ~= nil then
        result = result .. ":" .. ffi.string(ffi.C.cfg.primary_addr)
    end
    if ffi.C.cfg.custom_proc_title ~= nil then
        local proctitle = ffi.string(ffi.C.cfg.custom_proc_title)
        if #proctitle > 0 then
            result = result .. proctitle
        end
    end
    return string.gsub(result, '[. ()]+', '_')
end


local callback = {}

local mtu = 1400
local function makemsg()
    local hostname = gethostname()

    local head = ("my.octopus.%s."):format(gethostname())
    local tail = (" %i\n"):format(_ev_now())

    local msgs, msg = {}, {len = 0}
    for n, cb in pairs(callback) do
        local ok, t = xpcall(cb, traceback)
        if ok and type(t) == 'table' then
            local tp = n .. '.'
            local fix_len = #head + #tp + #tail
            for k, v in pairs(t) do
                local _k = tostring(k)
                local _v = v == 0 and ' 0' or (" %.2f"):format(v)
                local _len = fix_len + #_k + #_v
                if msg.len + _len > mtu then
                    table.insert(msgs, table.concat(msg))
                    msg = {len = 0}
                end
                table.insert(msg, head)
                table.insert(msg, tp)
                table.insert(msg, _k)
                table.insert(msg, _v)
                table.insert(msg, tail)
                msg.len = msg.len + _len
            end
        else
            print("error in graphite callback '" .. n .. "':" .. tostring(t))
        end
    end
    if #msg > 0 then
        table.insert(msgs, table.concat(msg))
    end
    return msgs
end


local addrvalid
local sockaddr_in = ffi.new('struct sockaddr_in')
local sockaddr = ffi.cast('struct sockaddr *', sockaddr_in)
local sock = ffi.C.socket(ffi.C.PF_INET, ffi.C.SOCK_DGRAM, 0)

function start(addr)
    local rc = ffi.C.atosin(addr, sockaddr_in)
    if rc >= 0 then
        addrvalid = true
        print('Graphite export to ' .. addr)
    else
        addrvalid = false
    end
end

local function worker_loop()
    fiber.sleep(60)
    if addrvalid then
        for _, msg in ipairs(makemsg()) do
            ffi.C.sendto(sock, msg, #msg, 0, sockaddr, ffi.sizeof(sockaddr_in))
        end
    end
    worker_loop()
end

if graphite_addr then
    start(graphite_addr)
end
fiber.create(worker_loop)

-- common stats
local function statcb(name)
    return stat.collectors[name]:get_periodic()
end

local function gccb()
    return {["count"] = collectgarbage("count")}
end

function add_cb(name, f)
    if not f then
        f = name
    end
    if type(f) == 'string' then
        local _name = f
        f = function() return statcb(_name) end
    end
    callback[name] = f
end

add_cb("gc", gccb)
local version = tonumber(ffi.string(ffi.C.octopus_version()):match('bundle:([0-9]+)'))
if version then
    add_cb("version", function () return {["version"] = version} end)
end
