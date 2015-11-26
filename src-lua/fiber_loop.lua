local xpcall, type, setmetatable = xpcall, type, setmetatable
local traceback, cut_traceback = debug.traceback, cut_traceback
local ffi = require 'ffi'

fiber.loops = {}
local loops_stat = stat.new_with_graphite('loops')

local Loop = {}
local loop_mt = { __index = Loop }


local function loop_sleep(self, sleep, _while)
    fiber.gc()
    while sleep > 1 do
        sleep = sleep - 1
        fiber.sleep(1)
        if not self.running then
            return
        end
        if _while and not _while() then
            return
        end
    end
    if sleep > 0 then
        fiber.sleep(sleep)
    end
end

local function loop_say_error(self, prefix)
    loops_stat:add1(self.name.."_error")
    say_error("%s in loop '%s' : %s", prefix, self.name, self.error)
    loop_sleep(self, 60, function () return self.error end)
end

local function loop_run(self)
    self.fiber_id = fiber.current
    local name_cnt = self.name .. "_cnt"
    while self.running do
        if self._paused then
            say_warn("Loop '%s' paused", self.name)
            loop_sleep(self, 5, function() return self._paused end)
        elseif self.error then
            loop_say_error(self, "Not resolved error")
        else
            loops_stat:add1(name_cnt)
            local ok, state_or_err, sleep = xpcall(self.func, traceback, self.state)
            if not ok then
                self.error = cut_traceback(state_or_err)
                loop_say_error(self, "Error")
            else
                if sleep == nil then
                    sleep = 1
                end
                if type(sleep) ~= "number" then
                    self.error = "sleep is not a number"
                    loop_say_error(self, "Error")
                else
                    self.state = state_or_err
                    loop_sleep(self, sleep)
                end
            end
        end
    end
    self.running = nil
    self.fiber_id = nil
end

function Loop:run()
    if self.running == nil then
        self.running = true
        fiber.create(function()
            loop_run(self)
        end)
    end
end

function Loop:stop()
    self.running = false
end

function Loop:pause(pause)
    if pause == nil then
        self._paused = true
    else
        self._paused = pause or nil
    end
end

local PRIMARY = tonumber(ffi.C.PRIMARY)
function Loop:paused()
    return self._paused
end

function fiber.loop(name, func)
    local loop = fiber.loops[name]
    if loop == nil then
        if func == nil then
            return nil
        end
        loop = setmetatable({name=name, func=func}, loop_mt)
        fiber.loops[name] = loop
        loop:run()
    else
        if func ~= nil then
            loop.func = func
            if loop.error then
                loop.error = nil
            end
        end
    end
    return loop
end
