require("stat")
require("index")
local ffi = require("ffi")

ffi.cdef[[
struct tnt_object {
	int32_t refs;
	uint8_t type;
	uint8_t flags;
	uint8_t data[0];
} __attribute__((packed));
]]

local print_ = print
function print (...)
        for k, v in pairs({...}) do
                print_(tostring(v))
        end
end



function reloadfile(filename)
        if not filename then
                error("reloadfile: bad arguments")
        end

        local function print_warn(msg)
                print(string.format("reloadfile(\"%s\"): %s", filename, msg))
        end

        local function require(filename)
                local modulename = string.gsub(string.gsub(filename, "^.*/", ""), "%.lua$", "")
                local module = loadfile(filename)
                local modulev = module(modulename)

                if modulev then
                        package.loaded[module] = modulev
                end
        end
        local function reload_loop()
                local tm = 0
                while true do
                        fiber.sleep(1)
                        local r, v = pcall(os.ctime, filename)
                        if r then
                                if v > tm then
                                        local r, err = pcall(require, filename)
                                        if r then
                                                tm = v
                                        else
                                                print_warn(err)
                                        end
                                end
                        else
                                print_warn(v)
                        end
                end
        end
        return fiber.create(reload_loop)
end

pcall(dofile, "init.lua")
print("Lua prelude initialized.")
