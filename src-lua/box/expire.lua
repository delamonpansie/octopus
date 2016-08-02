local box = require 'box'
local fiber = require 'fiber'
local xpcall, error, assert, traceback = xpcall, error, assert, debug.traceback
local say_warn, say_error = say_warn, say_error
local ipairs, type = ipairs, type
local insert, unpack = table.insert, unpack
local tostring, format = tostring, string.format

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
local function delete_batch(space, batch, ops_unused)
    local key = {}
    local pk = space:index(0)
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

local function loop_inner(ushard, ops, state)
    local space = ushard:object_space(ops.space)
    local indx = space:index(ops.index)
    local key = state.key
    state.key = nil
    local batch = {}

    local count = 0
    if indx:type() == "HASH" then
        if not ops.whole then
            error(format("iterate non-whole hash index %d of space %d", ops.index, ops.space))
        end
        local nxt = nil
        for tuple in indx:iter_from_pos(key or 0) do
            if tuple ~= nil and ops.filter(tuple) then
                insert(batch, tuple)
            end
            count = count + 1
            if count == batch_size then
                state.key = indx:cur_iter()
                break
            end
        end
    else
        for tuple in indx:iter(key) do
            if ops.filter(tuple) then
                insert(batch, tuple)
            elseif not ops.whole or (ops.whole == 'auto' and ops.index ~= 0) then
                break
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
        delete_batch(space, batch)
    end

    return (#batch + 1) * batch_size / ((batch_size+1) * expires_per_second)
end

local function loop(state, ops)
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

    local ok, sleep = box.with_txn(state.ushardn, loop_inner, ops, state)
    if not ok then
        say_error('%s', sleep)
        return state -- state.key == nil, so we are moving to next ushard
    end
    return state, sleep
end

function start(n, ops)
    if type(n) == 'table' then
        assert(ops == nil)
        ops = n
    elseif type(ops) == 'function' then
        ops = {space = n, filter = ops}
    else
        ops.space = n
    end
    if not ops.index then
        ops.index = 0
    end
    if not ops.action then
        ops.action = delete_batch
    end
    if ops.whole == nil then
        ops.whole = 'auto'
    end
    if not ops.expires_per_second then
        ops.expires_per_second = expires_per_second
    end
    if not ops.batch_size then
        ops.batch_size = batch_size
    end
    local name = 'box_expire('..ops.space..')'
    fiber.loop(name, loop, ops)
end


