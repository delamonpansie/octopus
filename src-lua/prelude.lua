require("stat")

function reloadfile(filename, module)
        if not filename or not module then
                error("reloadfile: bad arguments")
        end

        local function print_warn(msg)
                print(string.format("reloadfile(\"%s\", \"%s\"): %s", filename, module, msg))
        end

        local function reload_loop()
                local tm = 0
                while true do
                        fiber.sleep(1)
                        local r, v = pcall(os.ctime, filename)
                        if r then
                                if v > tm then
                                        package.loaded[module] = nil
                                        local r, err = pcall(require, module)
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
