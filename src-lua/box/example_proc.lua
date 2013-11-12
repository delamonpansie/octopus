require("fiber")
require("box")
require("index")
local ffi = require("ffi")
local bit = require("bit")
local net = require("net")

local assert, pcall, error, print, ipairs, pairs, type = assert, pcall, error, print, ipairs, pairs, type
local string, tonumber, tostring, table, box, fiber, index = string, tonumber, tostring, table, box, fiber, index

user_proc = user_proc or {}
local user_proc = user_proc

module(...)

-- simple definition via  box.defproc
user_proc.get_all_tuples = box.wrap(function (n, i)
	local result = {}
        local object_space = box.object_space[n]
        local function next_chunk(idx, start)
                local count, batch_size = 0, 3000
                for tuple in index.iter(idx, start) do
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

	if i == nil then i = 0 end
        -- iterate over all chunks
        for restart in next_chunk, object_space.index[i] do
                fiber.sleep(0.001)
        end

        local retcode = 0
        return retcode, result
end)

-- raw access to internals
user_proc.get_all_pkeys =
        function (out, request, n, batch_size)
		local out = net.conn(out)
                local object_space = box.space[n]

                if not object_space then
                        error(string.format("no such object_space: %s", n))
                end

                local key_count = 0
                if batch_size == nil or batch_size <= 42 then
                        batch_size = 42
                end

                local function next_chunk(idx, start)
                        local count, keys = 0, {}

                        for tuple in index.iter(idx, start) do
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

		local packs = {}
                for restart, keys in next_chunk, object_space.index[1] do
                        if #keys > 0 then
                                key_count = key_count + #keys
                                local pack = table.concat(keys)
				table.insert(packs, pack)
                        end
                        if restart == false then
                                break
                        end
                        fiber.sleep(0.001)
                end

		local header = out:add_iov_iproto_header(request)
                local bytes = out:bytes()
		out:add_iov_string(string.tou32(key_count))
		for _, pack in ipairs(packs) do
		   out:add_iov_ref(pack, #pack)
		end
		bytes = out:bytes() - bytes
		header.data_len = header.data_len + bytes
        end

-- periodic processing
user_proc.drop_first_tuple = box.wrap(function ()
	local function drop_first_tuple()
                fiber.sleep(1)
                local object_space = box.object_space[0]
                local idx = object_space.index[0]
                while true do
                        print("lookin for a tuple")
                        for tuple in index.iter(idx) do
                                print("found, killin it")
                                box.delete(0, tuple[0])
                                break
                        end
                        print("sleepin")
                        fiber.sleep(5)
                end
        end

        fiber.create(drop_first_tuple)
        return 0
end)


user_proc.sum_u64 = box.wrap(function (n, pk)
        local object_space = box.object_space[n]
        local obj = object_space.index[0][pk]
        if not obj then
                return 0, {"not found"}
        end

        local t = box.ctuple(obj);
        local f, offt, len = {}, 0
        for i = 0,t[0].cardinality - 1 do
                len, offt = box.decode_varint32(t[0].data, offt)
                if len == 8 then
                        table.insert(f, ffi.cast("uint64_t *", t[0].data + offt)[0])
                elseif len == 4 then
                        table.insert(f, ffi.cast("uint32_t *", t[0].data + offt)[0])
                else
                        table.insert(f, ffi.string(t[0].data + offt, len))
                end
                offt = offt + len
        end

        local sum = 0
        for k, v in ipairs(f) do
                local n = tonumber(v)
                if n ~= nil then
                        sum = sum + n
                end
        end

        return 0, {box.tuple(sum)}
end)

user_proc.truncate = box.wrap(function (n)
	local object_space = box.object_space[n]
	local pk = object_space.index[0]
	local c = 0;
	print("truncating object_space[".. n .. "]")
	for tuple in index.iter(pk) do
	   box.delete(n, tuple[0])
	   c = c + 1
	   if c % 1000000 == 0 then
	      print(" " .. c / 1000000 .. "M tuples")
	   end
	end

	print("truncated " .. c .. " tuples")
        return 0, 0
end)


user_proc.iterator = box.wrap(function (n, key, limit)
	local object_space = box.object_space[n]
	local pk = object_space.index[0]
	local result = {}

	if limit == nil then
	   limit = 1024
	else
	   limit = tonumber(limit)
	end

	for tuple in index.iter(pk, key) do
	   table.insert(result, tuple)
	   limit = limit - 1
	   if limit == 0 then
	      break
	   end
	end

        return 0, result
end)


local function test1()
   return 0, {box.tuple("abc", "defg", "foobar"),
	      box.tuple("abc", "defg"),
	      box.tuple("abc")}
end

local function tos(s)
   return tostring(s):gsub("0x%w+", "0xPTR")
end

local function test2()
   local os = box.object_space[0]
   local legacy_pk = os.index[0]
   local new_pk = os:index(0)

   local a = tos(legacy_pk)
   local b = tos(new_pk)
   return 0, {box.tuple(a, b)}
end

local function test3()
   local os = box.object_space[0]
   local pk = os:index(0)

   local t = pk:find("11")
   return 0, {box.tuple(tos(t), t:strfield(0), t[0])}
end

local function test4()
    local os = box.object_space[0]
    local pk = os:index(0)

    local k = {}
    for i = 0,1000 do
	k[i] = tostring(i)
    end

    local n = 0
    for i = 0,1000 do
	local t = pk:find(k[i])
	if t ~= nil then
	    n = n + 1
	end
    end

    local idx = os:index(1)
    for i = 0,1000 do
	local t = idx:find(k[i], i, k[i])
	if t ~= nil then
	    n = n + 1
	end
    end

    for i = 0,1000 do
	local t = pk:find(k[i], k)
	if t ~= nil then
	    n = n + 1
	end
    end

    return 0, {box.tuple(tostring(n))}
end



for i, f in ipairs({test1, test2, test3, test4}) do
   user_proc["test" .. tostring(i)] = box.wrap(f)
end
