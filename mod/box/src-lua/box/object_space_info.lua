local box = require("box")
local index = require("index")

local print,type,table,string,pairs = print,type,table,string,pairs

sys_proc = sys_proc or {}
local sys_proc = sys_proc

module(...)

sys_proc.object_space_info = function ()
	local batch_size = 1024 * 10
	local function next_chunk(idx, start)
                local count, bytes = 0, 0

                for obj in index.iter(idx, start) do
                        if (count == batch_size) then
                                return tuple, bytes
                        end

                        -- print(type(obj))
                        bytes = bytes + box.ctuple(obj)[0].bsize
                        count = count + 1
                end
                -- the loop is done, indicate that returning false and the residual
                return false, bytes
        end

        local function object_space_size(object_space)
                -- note, this iterator signals end of iteration by returning false, not nil
                -- this is done because otherwise the residual from last iteration is lost
                local total_bytes = 0
                for restart, bytes in next_chunk, object_space.index[0] do
                        total_bytes = total_bytes + bytes

                        if restart == false then
                                break
                        end
                        fiber.sleep(0.001)
                end
                return total_bytes
        end

        local result = {"namespace_usage:\r\n"}
        for k, v in pairs(box.object_space) do
                if type(k) == "string" then
                        table.insert(result, string.format("- %i: %i bytes\r\n", 1, object_space_size(v)))
                end
        end
        return table.concat(result)
end
