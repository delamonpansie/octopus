require("fiber")
require("box")
require("netmsg")

local error, print, pairs, type = error, print, pairs, type
local string, tostring, table, netmsg, box, fiber = string, tostring, table, netmsg, box, fiber

user_proc = user_proc or {}
local user_proc = user_proc

module(...)

-- simple definition via  box.defproc
user_proc.get_all_tuples = box.wrap(function (n)
	local result = {}
        local object_space = box.object_space[n]
        local function next_chunk(index, start)
                local count, batch_size = 0, 3000
                for tuple in box.index.treeiter(index, start) do
                        if (count == batch_size) then
                                -- return last tuple not putting it into result
                                -- outer iteration will use is as a restart point
                                return tuple
                        end
                        table.insert(result, tuple)
                        count = count + 1
                end
                return nil
        end

        -- iterate over all chunks
        for restart in next_chunk, object_space.index[1] do
                fiber.sleep(0.001)
        end

        local retcode = 0
        return retcode, result
end)

-- raw access to internals
user_proc.get_all_pkeys =
        function (out, n, batch_size)
                local object_space = box.space[n]

                if not object_space then
                        error(string.format("no such object_space: %s", n))
                end

                local key_count = 0
                if batch_size == nil or batch_size <= 42 then
                        batch_size = 42
                end

                local key_count_promise = netmsg.add_iov(out, nil) -- extract promise

                local function next_chunk(index, start)
                        local count, keys = 0, {}
                        for tuple in box.index.treeiter(index, start) do
                                if (count == batch_size) then
                                        return tuple, keys
                                end

                                table.insert(keys, string.tofield(tuple[0]))
                                count = count + 1
                        end
                        -- the loop is done, indicate that returning false and the residual
                        return false, keys
                end

                -- note, this iterator signals end of iteration by returning false, not nil
                -- this is done because otherwise the residual from last iteration is lost
                for restart, keys in next_chunk, object_space.index[1] do
                        if #keys > 0 then
                                key_count = key_count + #keys
                                local pack = table.concat(keys)
                                netmsg.add_iov(out, pack, #pack)
                        end
                        if restart == false then
                                break
                        end
                        fiber.sleep(0.001)
                end

                netmsg.fixup_promise(key_count_promise, string.tou32(key_count))

                local retcode = 0
                return retcode
        end

-- periodic processing
user_proc.drop_first_tuple = box.wrap(function ()
	local function drop_first_tuple()
                fiber.sleep(1)
                local object_space = box.object_space[0]
                local index = object_space.index[0]
                while true do
                        print("lookin for a tuple")
                        for i, tuple in box.index.hashpairs(index) do
                                print("found, killin it")
                                box.delete(object_space, tuple[0])
                                break
                        end
                        print("sleepin")
                        fiber.sleep(5)
                end
        end

        fiber.create(drop_first_tuple)
        return 0
end)
