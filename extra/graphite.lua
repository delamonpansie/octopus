--
-- this script will periodically send stats to graphite
-- to enable: put it to cfg.workdir, and add
-- require('graphite.lua').start('1.2.3.4:2003') to init.lua
--

local ffi = require 'ffi'
local fiber = require 'fiber'
require 'net' -- for ffi.cdef
local pcall, type, pairs, collectgarbage = pcall, type, pairs, collectgarbage
local print, tostring, tonumber = print, tostring, tonumber
local string, table = string, table
local stat = stat

module(...)

ffi.cdef[[
const char *octopus_version(void);
extern int gethostname(char *name, size_t len);
extern char *custom_proc_title;
extern char *primary_addr;

typedef long int time_t;
time_t time(time_t *t);
]]

local function gethostname()
    local buf = ffi.new('char[?]', 64)
    local result = "unknown"
    if ffi.C.gethostname(buf, ffi.sizeof(buf)) ~= -1 then
        result = ffi.string(buf)
    end
    if ffi.C.primary_addr ~= nil then
        result = result .. ":" .. ffi.string(ffi.C.primary_addr)
    end
    if ffi.C.custom_proc_title ~= nil then
        local proctitle = ffi.string(ffi.C.custom_proc_title)
        if #proctitle > 0 then
            result = result .. proctitle
        end
    end
    return string.gsub(result, '[. ()]+', '_')
end


local callback = {}

local function makemsg()
    local hostname = gethostname()
    local time = tostring(tonumber(ffi.C.time(nil)))
    local msg = {}

    local head = ("my.octopus.%s."):format(gethostname())
    local tail = (" %i\n"):format(tonumber(ffi.C.time(nil)))

    for n, cb in pairs(callback) do
        local ok, t = pcall(cb)
        if ok and type(t) == 'table' then
            local type = n .. "."
            for k, v in pairs(t) do
                table.insert(msg, head)
                table.insert(msg, type)
                table.insert(msg, k)
                table.insert(msg, " ")
                table.insert(msg, v)
                table.insert(msg, tail)
            end
        else
            print("error in graphite callback '" .. n .. "'")
        end
    end
    if #msg == 0 then
        return nil
    end
    return table.concat(msg)
end


local addrvalid
local sockaddr_in = ffi.new('struct sockaddr_in')
local sockaddr = ffi.cast('struct sockaddr *', sockaddr_in)
local sock = ffi.C.socket(ffi.C.PF_INET, ffi.C.SOCK_DGRAM, 0)

function add_cb(name, f)
    callback[name] = f
end

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
        local msg = makemsg()
        if msg then
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
local function statcb()
    local stat_ready = pcall(function () pairs(stat.records[0]) end)
    if not stat_ready then
        -- stat module either not loaded or disabled
        return {}
    end
    return stat.records[0]
end
local function gccb()
    return {["count"] = collectgarbage("count")}
end

add_cb("stat", statcb)
add_cb("gc", gccb)
local version = tonumber(ffi.string(ffi.C.octopus_version()):match('bundle:([0-9]+)'))
if version then
    add_cb("version", function () return {["version"] = version} end)
end
