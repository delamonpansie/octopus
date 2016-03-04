local box = require 'box'
local fiber = require 'fiber'
local xpcall, error, traceback = xpcall, error, debug.traceback
local say_warn, say_error = say_warn, say_error
local ipairs = ipairs
local insert = table.insert
local tostring = tostring
local unpack = unpack

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

expires_per_second = 1000
batch_size = 100

-- every space modification must be done _outside_ of iterator running
local function delete_batch(space, pk, batch)
    local key = {}
    for _, tuple in ipairs(batch) do
        for i,pos in ipairs(pk.__field_indexes) do
            key[i] = tuple:strfield(pos)
        end
        local r, err = xpcall(space.delete_noret, traceback, space, key)
        if not r then
            say_warn("delete failed: %s", err)
        end
    end
end

local function loop_inner(n, func, state)
    local space = box.ushard(state.ushardn):object_space(n)
    local pk = space:index(0)
    local key = state.key
    state.key = nil
    local batch = {}

    if pk:type() == "HASH" then
        local i, j = key or 0, batch_size
        while i < pk:slots() and j > 0 do
            local tuple = pk:get(i)
            if tuple ~= nil and func(tuple) then
                insert(batch, tuple)
            end
            i, j = i + 1, j - 1
        end

        if i < pk:slots() then
            state.key = i
        end
    else
        local count = 0
        for tuple in pk:iter(key) do
            if func(tuple) then
                insert(batch, tuple)
            end
            count = count + 1
            if count == batch_size then
                tuple:make_long_living()
                state.key = tuple
                break
            end
        end
    end

    if #batch > 0 then
        delete_batch(space, pk, batch)
    end

    return (#batch + 1) * batch_size / ((batch_size+1) * expires_per_second)
end

local function loop(n, func, state)
    if state == nil then
        state = {ushardn = -1}
    end
    if state.key == nil then
        local next_ushardn = box.next_primary_ushard_n(state.ushardn)
        if next_ushardn == -1 or next_ushardn == state.ushardn then
            state.ushardn = -1
            if _M.testing then
                return nil, 0.03
            else
                return -- sleep for 1 second
            end
        end
        state.ushardn = next_ushardn
    end
    local ok, sleep = xpcall(loop_inner, traceback, n, func, state)
    if not ok then
        say_error('%s', sleep)
        return state -- state.key == nil, so we are moving to next ushard
    end
    return state, sleep
end

function start(n, func)
    local name = 'box_expire('..n..')'
    fiber.loop(name, function (state)
        return loop(n, func, state)
    end)
end


