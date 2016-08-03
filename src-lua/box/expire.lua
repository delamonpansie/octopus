local box = require 'box'
local loop = require 'box.loop'
local xpcall, error, assert, traceback = xpcall, error, assert, debug.traceback
local say_warn = say_warn
local ipairs, type = ipairs, type
local insert = table.insert
local format = string.format

-- simple usage

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

-- advanced usage

-- local function action_on_batch(space, batch, conf)
--    local now = math.floor(os.ev_time())
--    for _, tuple in ipairs(batch) do
--      space:update(tuple[0], {1, 'set', now}, {2, 'add', 1})
--    end
-- end

-- local function filter_tuples(tuple)
--    return tuple:u32field(1) < math.floor(os.ev_time() - 3600)
-- end

-- exp.start{
--    space = 1,
--    index = 1,                  -- default index is 0 - primary key
--    filter = filter_tuples,
--    action = action_on_batch,   -- default action is to delete tuples
--    whole = false,              -- automatically on non primary TREE indexes
--    batch_size = 10,            -- how many tuples filter per invocation
--    expires_per_second = 100,   -- how many tuples encount per second, default is 1000
--    encount = 'filter',         -- what to encount:
--                                --   'filter' - encount calls to filter
--                                --   'batch'  - encount batch sizes (default)
--    name = 'expire_space_1',    -- name of fiber loop
--                                -- default is format('box_expire(%d)', space)
-- }

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

local function loop_inner(ushard, key, ops)
    local space = ushard:object_space(ops.space)
    local indx = space:index(ops.index)
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
                key = indx:cur_iter()
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
                key = tuple
                break
            end
        end
    end

    if #batch > 0 then
        delete_batch(space, batch)
    end

    if ops.encount == 'batch' then
        return key, (#batch + 1) * batch_size / ((batch_size+1) * expires_per_second)
    elseif ops.encount == 'filter' then
        return key, (count + 1) * batch_size / ((batch_size+1) * expires_per_second)
    else
        error("who the f@ck changed ops.encount?")
    end
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
    if not ops.encount then
        ops.encount = 'batch'
    else
        assert(ops.encount == 'filter' or ops.encount == 'batch')
    end
    if not ops.name then
        ops.name = 'box_expire('..ops.space..')'
    end
    ops.exec = loop_inner
    loop.start(ops)
end


