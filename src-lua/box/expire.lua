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

expires_per_second = 1024
batch_size = 1024

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

    if pk:type() == "HASH" then
        local i, j = key or 0, batch_size
        local batch = {}
        while i < pk:slots() and j > 0 do
            local tuple = pk:get(i)
            if tuple ~= nil and func(tuple) then
                insert(batch, tuple)
            end
            i, j = i + 1, j - 1
        end

        delete_batch(space, pk, batch)

        if i < pk:slots() then
            state.key = i
        end
        return #batch / expires_per_second
    else
        local count = 0
        local batch = {}

        -- must restart iterator after fiber.sleep
        for tuple in pk:iter(key) do
            if func(tuple) then
                insert(batch, tuple)
            end
            if count == batch_size then
                break
            end
            count = count + 1
        end

        delete_batch(space, pk, batch)
        if #batch > 0 then
            state.key = batch[#batch]
            state.key:make_long_living()
        end
        return #batch / expires_per_second
    end
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
                return
            end
        end
        state.ushardn = next_ushardn
    end
    local ok, sleep = xpcall(loop_inner, traceback, n, func, state)
    if not ok then
        say_error('%s', state_or_err)
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


