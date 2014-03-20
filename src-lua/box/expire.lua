local box = require 'box'
local fiber = require 'fiber'
local print, xpcall, error, traceback = print, xpcall, error, debug.traceback
local ipairs = ipairs
local insert = table.insert

-- example usage

-- local box = require 'box'
-- local exp = require 'box.expire'


-- local function ex(tuple)
--    local n = box.cast.u32(tuple[0])
--    if n < 100 then
--       -- expire all tuples with tuple[0] < 100
--       return true
--    end
-- end

-- exp.start(0, ex)

module(...)

expires_per_second = 1024
batch_size = 1024

local map = {}

local function loop(n)
   local expired = {}
   local object_space = box.object_space[n]
   if object_space == nil then
      error("object space " .. n .. " is not configured")
   end
   local pk = object_space:index(0)

   -- every object_space modification must be done _outside_ of iterator running
   local function delete_batch(batch)
       for _, tuple in ipairs(batch) do
           local r, err = xpcall(box.delete, traceback, n, tuple[0])
           if not r then
               print("delete failed: " .. err)
           end
       end
   end

   if pk:type() == "HASH" then
       local i = 0
       while i < pk:slots() do
           local batch = {}
           for j = 0, batch_size do
               local tuple = pk:get(i + j)
               if tuple ~= nil and map[n](tuple) then
                   insert(batch, tuple)
               end
           end

           delete_batch(batch)

           i = i + batch_size
           fiber.gc()
           fiber.sleep(batch_size / expires_per_second)
       end
   else
       local i = nil
       repeat
           local count = 0
           local batch = {}

           -- must restart iterator after fiber.sleep
           for tuple in pk:iter(i) do
               if count == batch_size then
                   i = tuple
                   break
               else
                   i = nil
               end

               if map[n](tuple) then
                   insert(batch, tuple)
               end
               count = count + 1
           end

           delete_batch(batch)
           fiber.gc()
           fiber.sleep(batch_size / expires_per_second)
       until i == nil
   end
end

function start(n, func)
   if map[n] ~= nil then
      map[n] = func
      return
   end

   map[n] = func
   fiber.create(function ()
                    while true do
                        -- the bellow loop() won't run if object_space is empty,
                        -- so in order to prevent busyloop, sleep at least once per loop() call
                        fiber.sleep(1)
                        loop(n)
		   end
		end)
end


