local box = require 'box'
local index = require 'index'
local fiber = require 'fiber'
local print, pcall, error = print, pcall, error


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

local map = {}

local function loop(n)
   local expired = {}
   local object_space = box.object_space[n]
   if object_space == nil then
      error("object space " .. n .. " is not configured")
   end
   local pk = object_space.index[0]
   local batch_size = 1024

   local function cb(tuple)
      if map[n](tuple) then
	 local r, err = pcall(box.delete, n, tuple[0])
	 if not r then
	    print("delete failed: " .. err)
	 end
      end
   end

   local function next_tree_chunk(idx, ini)
      local count = 0
      for tuple in index.iter(idx, ini) do
	 if (count == batch_size) then
	    -- return last tuple not putting it into result
	    -- outer iteration will use is as a restart point
	    return tuple
	 end
	 cb(tuple)
	 count = count + 1
      end
      return nil
   end

   local function next_hash_chunk(idx, ini)
      for i = 0, batch_size do
	 local tuple = index.hashget(idx, ini + i)
	 if tuple ~= nil then
	    cb(tuple)
	 end
      end
      if ini + batch_size > index.hashsize(idx) then
	 return nil
      else
	 return ini + batch_size
      end
   end

   if pcall(index.hashsize, pk) then
      for _ in next_hash_chunk, pk, 0 do
	 fiber.gc()
	 fiber.sleep(batch_size / expires_per_second)
      end   
   else
      for _ in next_tree_chunk, pk do
	 fiber.gc()
	 fiber.sleep(batch_size / expires_per_second)
      end   
   end

   -- the above for loops won't run if object_space is empty,
   -- so in order to prevent busyloop, sleep at least once per loop()
   fiber.sleep(batch_size / expires_per_second)
end

function start(n, func)
   if map[n] ~= nil then
      map[n] = func
      return
   end

   map[n] = func
   fiber.create(function ()
		   while true do
		      loop(n)
		   end
		end)
end


