require("fiber")
require("box")

local error, print, pairs, type = error, print, pairs, type
local table, fiber, box = table, fiber, box
local namespace_registry = namespace_registry

module(...)

-- local function drop_first_tuple()
--         fiber.sleep(1)
--         local namespace = namespace_registry[0]
--         local index = namespace.index[0]
--         while true do
--                 print("lookin for a tuple")
--                 for i, tuple in box.index.hashpairs(index) do
--                         print("found, killin it")
--                         box.delete(namespace, tuple[0])
--                         break
--                 end
--                 print("sleepin")
--                 fiber.sleep(5)
--         end
-- end
-- fiber.create(drop_first_tuple)
