local ffi = require 'ffi'
local table_insert, table_remove = table.insert, table.remove
local xpcall, unpack = xpcall, unpack
local traceback, cut_traceback = debug.traceback, cut_traceback
local C = ffi.C

local _yield = fiber.yield

setmetatable(fiber, {
    __index = function(f, k)
        if k == 'current' then
            return C.fiber.fid
        elseif k == 'switch_cnt' then
            return C.coro_switch_cnt
        end
    end
})

local function wake(fid)
    local fib = C.fid2fiber(fid)
    assert(fib ~= nil)
    C.fiber_wake(fib, nil)
end

local locks = {}

local function lock(key)
    if locks[key] == nil then
        locks[key] = {}
    else
        table_insert(locks[key], C.fiber.fid)
        _yield()
    end
end

local function unlock(key)
    local q = locks[key]
    if #q == 0 then
        locks[key] = nil
    else
        wake(table_remove(q, 1))
    end
end

function fiber.locked(key, trans, ...)
    lock(key)
    local results = {xpcall(trans, traceback, ...)}
    unlock(key)
    if results[1] == false then
        error(cut_traceback(results[2]))
    end
    table_remove(results, 1)
    return unpack(results)
end

fiber.wake = wake
fiber._locks = locks
fiber._lock = lock
fiber._unlock = unlock
