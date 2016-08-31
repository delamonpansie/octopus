local box = require 'box'
local loop = require 'box.loop'
local xpcall, error, assert, traceback = xpcall, error, assert, debug.traceback
local say_warn = say_warn
local ipairs, type = ipairs, type
local insert = table.insert
local format = string.format
local ev_now = os.ev_now
local setmetatable = setmetatable

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

-- local function action_on_tuple(tuple, ctx)
--    local space, ushard, conf = ctx.space, ctx.ushard, ctx.conf
--    local now = math.floor(os.ev_time())
--    space:update({tuple[0]}, {{1, 'set', now}, {2, 'add', 1}})
-- end

-- -- alternatively, you may work on whole batch of filtered records
-- local function action_on_batch(batch, ctx)
--    local space, ushard, conf = ctx.space, ctx.ushard, ctx.conf
--    local now = math.floor(os.ev_time())
--    for _, tuple in ipairs(batch) do
--      space:update({tuple[0]}, {{1, 'set', now}, {2, 'add', 1}})
--    end
-- end

-- local function filter_tuples(tuple, ctx)
--    return tuple:u32field(1) < math.floor(os.ev_time() - 3600)
-- end
--
-- local function precondition(ctx, first_iteration)
--    if ctx.space:slab_bytes() < 100*1024*1024 then
--      ctx.key = nil -- restart iteration
--      return false
--    end
-- end

-- exp.start{
--    space = 1,
--      -- index to iterate, default is 0 - primary key
--    index = 1,
--
--    filter = filter_tuples,
--      -- alternative way is to specify field (it should be 32bit timestamp) and period
--    -- field = 1,
--      -- period could be nil, number
--      -- or function(tuple, ctx) return period end
--    -- period = 3600,
--
--      -- action to perform on tuple
--    action = action_on_tuple,
--      -- `action_batch` is action on whole batch of filtered records
--      -- `action` and `action_batch` are mutually exclusive
--      -- if neither `action` nor `action_batch` were specified,
--      --    then default `action_batch` deletes all filtered tuples
--    -- action_batch  = action_on_batch,
--      -- direction is a way to iterate index
--      -- `nil`      - 'whole' on primary or hash index, 'head' on secondary tree
--      -- 'whole'    - whole space
--      -- 'head'     - tree index forward, always restarting, pause when filter returns false
--      -- 'tail'     - tree index backward, always restarting, pause when filter returns false
--      -- 'backward' - whole tree index backward
--    direction = 'head',
--      -- how many tuples perform per iteration, default is 25
--    batch_size = 10,
--      -- how many tuples encount per second, default is 1000,
--      -- could be function taking ctx,
--      -- ctx will contain amount of filtered tuples in this iteration
--    expires_per_second = 100,
--      -- what to encount in `batch_size`:
--      --   'filter' - encount calls to filter (default)
--      --   'batch'  - encount batch sizes
--    encount = 'filter',
--      -- name of fiber loop
--      -- default is format('box_expire_%d', space)
--    name = 'expire_space_1',
--      -- `precondition` is called in start of each iteration,
--      -- may analize and add values to ctx
--      -- may analize and change ctx.key (if it sets key to nil, iteration restarts)
--      -- (key is position for hash index,
--      --  tuple for tree index when iterating 'whole' and 'backward',
--      --  and `true` when iteration 'head' or 'tail')
--      -- if returns nil or true, nothing happen
--      -- if returns false, iteration paused for a second and restarted (key becomes nil)
--      -- if returns number, iteration paused for this number of seconds and not restarted
--    precondition = precondition,
-- }

module(...)

expires_per_second = 1000
batch_size = 25

-- every space modification must be done _outside_ of iterator running
local function delete_batch(batch, ctx)
    local key = {}
    local pk = ctx.space:index(0)
    for _, tuple in ipairs(batch) do
        for i,pos in ipairs(pk.__field_indexes) do
            key[i] = tuple:strfield(pos)
        end
        ctx.space:delete_noret(key)
    end
end

local function iter_batch(batch, ctx)
    for _, tuple in ipairs(batch) do
        ctx.conf.action(tuple, ctx)
    end
end

local function loop_inner(ushard, key, conf)
    local space = ushard:object_space(conf.space)
    local indx = space:index(conf.index)
    local ctx = {space = space, ushard = ushard, conf = conf}

    if conf.precondition then
        ctx.key = key
        local sleep = conf.precondition(ctx)
        key, ctx.key = ctx.key, nil
        if sleep ~= nil then
            assert(sleep == nil or type(sleep) == 'boolean' or type(sleep) == 'number')
            if sleep == false then
                return nil, sleep
            else
                return key, sleep
            end
        end
    end

    local newkey = nil
    local batch = {}

    local count = 0
    if indx:type() == "HASH" then
        if conf.direction and conf.direction ~= 'whole' then
            error(format("iterate non-whole hash index %d of space %d (conf.direction = %s)",
                    conf.index, conf.space, conf.direction))
        end
        local nxt = nil
        for tuple in indx:iter_from_pos(key or 0) do
            if tuple ~= nil and conf.filter(tuple, ctx) then
                insert(batch, tuple)
            end
            count = count + 1
            if count == batch_size then
                newkey = indx:cur_iter()
                break
            end
        end
    else
        local direction = conf.direction
        if not direction then
            direction = conf.index == 0 and 'whole' or 'head'
        end
        local nxt, ix
        if direction == 'whole' then
            nxt, ix = indx:iter(key)
        elseif direction == 'backward' then
            nxt, ix = indx:riter(key)
        elseif direction == 'head' then
            nxt, ix = indx:iter()
        elseif direction == 'tail' then
            nxt, ix = indx:riter()
        else
            error(format("unknown direction %s", direction))
        end
        for tuple in nxt, ix do
            if conf.filter(tuple, ctx) then
                insert(batch, tuple)
            elseif direction == 'head' or direction == 'tail' then
                break
            end
            count = count + 1
            if count == batch_size then
                if direction ~= 'head' and direction ~= 'tail' then
                    newkey = nxt(ix)
                    newkey:make_long_living()
                else
                    newkey = true
                end
                break
            end
        end
    end

    if #batch > 0 then
        conf.action_batch(batch, ctx)
    end

    if newkey == nil then
        return nil, nil
    end

    local expires_per_second = conf.expires_per_second
    if type(expires_per_second) == 'function' then
        ctx.batch_size = #batch
        expires_per_second = expires_per_second(ctx)
        if expires_per_second < 1 then
            expires_per_second = 1
        end
    end
    if conf.encount == 'batch' then
        return newkey, (#batch + 1) * batch_size / ((batch_size+1) * expires_per_second)
    elseif conf.encount == 'filter' then
        return newkey, (count + 1) * batch_size / ((batch_size+1) * expires_per_second)
    else
        error("who the f@ck changed conf.encount?")
    end
end

local period_filter_mt = {
    __call = function(filter, tuple, ctx)
        return tuple:u32field(filter.field) < ev_now() - filter.period(tuple, ctx)
    end
}

function start(n, conf)
    if type(n) == 'table' then
        assert(conf == nil)
        conf = n
    elseif type(conf) == 'function' then
        conf = {space = n, filter = conf}
    else
        conf.space = n
    end
    if conf.filter == nil and conf.field then
        local period = conf.period
        if type(period) == 'number' then
            period = function() return conf.period end
        elseif period == nil then
            period = function() return 0 end
        end
        conf.filter = setmetatable({
            field = conf.field,
            period = period,
        }, period_filter_mt)
    end
    assert(conf.filter ~= nil)
    if not conf.index then
        conf.index = 0
    end
    if conf.action then
        if conf.action_batch then
            error("box.expire.start: either action or action_batch should be specified, not both")
        end
        conf.action_batch = iter_batch
    end
    if not conf.action_batch then
        conf.action_batch = delete_batch
    end
    if not conf.expires_per_second then
        conf.expires_per_second = expires_per_second
    end
    if not conf.batch_size then
        conf.batch_size = batch_size
    end
    if not conf.encount then
        conf.encount = 'filter'
    else
        assert(conf.encount == 'filter' or conf.encount == 'batch')
    end
    if not conf.name then
        conf.name = 'box_expire_'..conf.space
    end
    conf.exec = loop_inner
    loop.start(conf)
end


