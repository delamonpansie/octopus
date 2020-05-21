require("fiber")
require("box")
local index = require("index")
local ffi = require("ffi")
local bit = require("bit")
local net = require("net")
local require = require

local assert, pcall, error, print, ipairs, pairs, type = assert, pcall, error, print, ipairs, pairs, type
local string, tonumber, tostring, table, box, fiber = string, tonumber, tostring, table, box, fiber
local say_error = say_error

user_proc = user_proc or {}
local user_proc = user_proc

module(...)

-- simple definition via  box.defproc
user_proc.get_all_tuples = box.wrap(function (ushard, n, i)
	local result = {}
        local object_space = ushard:object_space(n)
        local function next_chunk(index, start)
                local count, batch_size = 0, 3000
                for tuple in index:iter(start) do
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
        for restart in next_chunk, object_space:index(i) do
                fiber.sleep(0.001)
        end

        local retcode = 0
        return retcode, result
end)

-- raw access to internals
user_proc.get_all_pkeys =
    function (out, request, n, batch_size)
        	local object_space = box.ushard(0):object_space(n)

                if not object_space then
                        error(string.format("no such object_space: %s", n))
                end

                local key_count = 0
                if batch_size == nil or batch_size <= 42 then
                        batch_size = 42
                end

                local function next_chunk(index, start)
                        local count, keys = 0, {}

                        for tuple in index:iter(start) do
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
                for restart, keys in next_chunk, object_space:index(1) do
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
                local bytes = out.bytes
		out:add_iov_string(string.tou32(key_count))
		for _, pack in ipairs(packs) do
		   out:add_iov_ref(pack, #pack)
		end
		bytes = out.bytes - bytes
		header.data_len = header.data_len + bytes
        end

-- periodic processing
user_proc.drop_first_tuple = box.wrap(function (ushard)
	local function drop_first_tuple()
                fiber.sleep(1)
                local object_space = ushard:object_space(0)
                local pk = object_space:index(0)
                while true do
                        print("lookin for a tuple")
                        for tuple in pk:iter() do
                                print("found, killin it")
                                ushard:delete(0, tuple[0])
                                break
                        end
                        print("sleepin")
                        fiber.sleep(5)
                end
        end

        fiber.create(drop_first_tuple)
        return 0
end)


local typemap = { [8] = ffi.typeof('uint64_t *'),
                  [4] = ffi.typeof('uint32_t *'),
                  [2] = ffi.typeof('uint16_t *')}

user_proc.sum_u64 = box.wrap(function (ushard, n, pk)
        local object_space = ushard:object_space(n)
        local obj = object_space:index(0):find(pk)
        if not obj then
            return 0x202, {"not found"}
        end

        -- think twise before doing raw access to box_tuple
        local t = obj:raw_box_tuple() -- "raw" pointer to slab allocated 'struct box_tuple'
        local f, offt, len = {}, 0
        for i = 0, t.cardinality - 1 do
            len, offt = box.decode_varint32(t.data, offt)
            local v
            if typemap[len] then
                v = ffi.cast(typemap[len], t.data + offt)[0]
            else
                v = ffi.string(t.data + offt, len)
            end
            table.insert(f, v)
            offt = offt + len
        end

        local sum = 0
        for k, v in ipairs(f) do
                local n = tonumber(v)
                if n ~= nil then
                        sum = sum + n
                end
        end

        return 0, {box.tuple(tostring(sum))}
end)

user_proc.sum_u64_v2 = box.wrap(function (ushard, n, pk)
    local object_space = ushard:object_space(n)
    local tuple = object_space:index(0):find(pk)
    if not tuple then
        return 0x202, {"not found"}
    end

    local sum = 0
    for i = 0, tuple.cardinality - 1 do
        local ok, v = pcall(tuple.numfield, tuple, i) -- method call
        if not ok then
            v = tonumber(tuple:strfield(i))
        end
        if v then
            sum = sum + v
        end
    end
    return 0, {box.tuple(tostring(sum))}
end)

user_proc.truncate = box.wrap(function (ushard, n)
	local object_space = ushard:object_space(n)
	local pk = object_space:index(0)
	local c = 0;
	print("truncating object_space[".. n .. "]")
	for tuple in pk:iter() do
	   object_space:delete(tuple[0])
	   c = c + 1
	   if c % 1000000 == 0 then
	      print(" " .. c / 1000000 .. "M tuples")
	   end
	end

	print("truncated " .. c .. " tuples")
        return 0, 0
end)


user_proc.iterator = box.wrap(function (ushard, n, key, limit, dir, offset)
	local os = ushard:object_space(n)
        local pk = os:index(0)
        local result = {}
        local off = offset and tonumber(offset) or 0

	if limit == nil then
	   limit = 1024
	else
	   limit = tonumber(limit)
	end

        local next, state
        if pk:type() == 'TREE' then
            next, state = pk:diter(dir or 'forward', key)
        else
            next, state = pk:iter(key)
        end

        for tuple in index.offset(off, next, state) do
	   table.insert(result, tuple)
	   limit = limit - 1
	   if limit == 0 then
	      break
	   end
	end

        return 0, result
end)

user_proc.iterator2 = box.wrap(function (ushard, n, key)
	local os = ushard:object_space(0)
        local pk = os:index(n)
        local result = {}

        for tuple in pk:iter(tonumber(key)) do
	   table.insert(result, tuple)
	end

        return 0, result
end)

user_proc.iterator2r = box.wrap(function (ushard, n, key)
	local os = ushard:object_space(0)
        local pk = os:index(n)
        local result = {}

        for tuple in pk:riter(tonumber(key)) do
	   table.insert(result, tuple)
	end

        return 0, result
end)

user_proc.iterator3 = box.wrap(function (ushard)
	local os = ushard:object_space(0)
        local pk = os:index(0)
        for tuple in pk:iter() do
            fiber.sleep(0.000001)
        end
        return 0, nil
end)


local function test1()
   return 0, {box.tuple("abc", "defg", "foobar"),
	      box.tuple("abc", "defg"),
	      box.tuple("abc")}
end

local function test0()
   return 0, {box.tuple("abc", "defg", "foobar"),
	      box.tuple("abc", "defg"),
	      box.tuple("abc")}
end

local function tos(s)
   return tostring(s):gsub("0x%w+", "0xPTR")
end

local function test2(ushard)
   local os = ushard:object_space(0)
   local legacy_pk = os:index(0)
   local new_pk = os:index(0)

   local a = tos(legacy_pk)
   local b = tos(new_pk)
   return 0, {box.tuple(a, b)}
end

local function test3(ushard)
   local os = ushard:object_space(0)
   local pk = os:index(0)

   local t = pk:find("11")
   return 0, {box.tuple(tos(t), t:strfield(0), t[0])}
end

local function test4(ushard)
    local os = ushard:object_space(0)
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
	local t = idx:find(k[i], i)
	if t ~= nil then
	    n = n + 1
	end
    end

    return 0, {box.tuple(tostring(n))}
end

local function test5(ushard)
    local os = ushard:object_space(1)
    local ret = {}
    table.insert(ret, os:index(0):find(0))
    table.insert(ret, os:index(1):find(0))
    table.insert(ret, os:index(2):find(0))
    table.insert(ret, os:index(3):find(0))

    return 0, ret
end

local function test6(ushard)
    return 0, {ushard:replace(0, "\0\0\0\0", "\0\0\0\0", "\0\0\0\0")}
end

local function test7(ushard)
    local ret = {}
    table.insert(ret, ushard:object_space(0):index(0):find("99"))
    table.insert(ret, ushard:update(0, "99", {2,"set","9999"}))
    table.insert(ret, ushard:delete(0, "99"))
    table.insert(ret, box.tuple{ tostring(ushard:object_space(0):index(0):find("99")) })

    return 0, ret
end

local function test8(ushard)
    local ret = {}
    table.insert(ret, ushard:object_space(2):index(0):find(0, 0))
    table.insert(ret, ushard:update(2, {"\0\0\0\0","\0\0\0\0"}, {2,"set","9999"}))
    table.insert(ret, ushard:delete(2, {"\0\0\0\0","\0\0\0\0"}))
    table.insert(ret, box.tuple{ tostring(ushard:object_space(2):index(0):find(0, 0)) })
    return 0, ret
end

local function test9(ushard)
    local os = ushard:object_space(2)
    local ret = {}
    table.insert(ret, os:replace("\0\0\0\0", "\0\0\0\0", "","",""))
    table.insert(ret, os:index(0):find(0, 0))
    table.insert(ret, os:update({"\0\0\0\0","\0\0\0\0"}, {2,"set16",0}, {3,"set32", 0}, {4,"set64", 0}))
    table.insert(ret, os:update({"\0\0\0\0","\0\0\0\0"}, {2,"add16", 0}, {3, "or32", 14}, {4, "xor64", 99}))
    table.insert(ret, os:index(0):find(0, 0))

    return 0, ret
end

local function test10(ushard)
    fiber.sleep(1)
    return 0, {ushard:replace(0, "\0\0\0\0", "dead", "beef")}
end

local function test11(ushard)
    ushard:replace(0, "\0\0\0\0", "dead", "beef")
    error("oops")
end

for i, f in ipairs({test0, test1, test2, test3, test4, test5, test6, test7, test8, test9, test10, test11}) do
   user_proc["test" .. tostring(i - 1)] = box.wrap(f)
end

user_proc.position = box.wrap(function(ushard, ind, i)
    local pos = ushard:object_space(0):index(tonumber(ind)):position(tonumber(i))
    return 0, {box.tuple{tostring(pos)}}
end)

user_proc.start_expire = box.wrap(function(ushard, n, ind)
    local loop = require 'box.loop'
    local expire = require 'box.expire'
    loop.testing = true
    ind = tonumber(ind) or 0
    expire.start{
        space = tonumber(n),
        filter = function(tuple)
            return tuple:strfield(1) > '0'
        end,
        index = ind,
        batch_size = 4,
    }
    return 0, {}
end)

user_proc.multi_insert_for_partial_shard = box.wrap(function(ushard, from, to, second)
    local space = ushard:object_space(0)
    for i = tonumber(from), tonumber(to) do
        space:replace{tostring(i), second}
    end
    return 0, {}
end)
