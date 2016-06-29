--
-- this script will periodically send stats to graphite
-- to enable: put it to cfg.workdir, and add
-- require('graphite.lua').start('1.2.3.4:2003') to init.lua
--

local ffi = require 'ffi'
local fiber = require 'fiber'
local xpcall, type, collectgarbage = xpcall, type, collectgarbage
local error, pairs, tostring, tonumber = error, pairs, tostring, tonumber
local traceback = debug.traceback
local stat = stat
local say_info = say_info

module(...)

local callback = {}

local function worker_loop()
    while true do
        fiber.sleep(60)
        if ffi.C.graphite_sock ~= -1 then
            for n, cb in pairs(callback) do
                local ok, t = xpcall(cb, traceback)
                n = tostring(n)
                if ok and type(t) == 'table' then
                    for k, v in pairs(t) do
                        local _k = tostring(k)
                        local _v = tonumber(v)
                        ffi.C.graphite_send2(n, _k, _v)
                    end
                else
                    print("error in graphite callback '" .. n .. "':" .. tostring(t))
                end
            end
        end
    end
end
fiber.create(worker_loop)

-- common stats
local function gccb()
    return {["count"] = collectgarbage("count")}
end

local function is_callable(v)
    return v and
      (type(v) == 'function' or
       type(getmetatable(v)) == 'table' and is_callable(getmetatable(v).__call))
end

function add_cb(name, f)
    if not f or not is_callable(f) then
        if stat.collectors[name] then
            -- just skip for backward compatibility
            return
        else
            error("stat '"..name.."' is not defined")
        end
    end
    callback[name] = f
end

add_cb("gc", gccb)
say_info("graphite.lua loaded")
