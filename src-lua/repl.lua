local ffi = require 'ffi'
local jit = require 'jit'


local function write(fd, str)
    ffi.C.fiber_write(fd, str, #str)
end
jit.off(write) -- fiber_write may yield

local ddump = ddump

local function make_env(fd)
    local function repl_print (...)
        local n = select('#', ...)
        for i = 1, n do
            local str = tostring((select(i, ...))) or 'nil'
            write(fd, str)
            if i ~= n then
                write(fd, "\t")
            else
                write(fd, "\r\n")
            end
        end
    end
    local function writen(...)
        local n = select('#', ...)
        for i = 1, n do
            write(fd, tostring(select(i, ...)))
        end
    end
    local function repl_ddump(v)
        ddump(v, writen)
        write(fd, "\r\n")
    end

    setfenv(0, setmetatable({print = repl_print,
                             ddump = repl_ddump}, {__index = _G}))
end

return make_env
