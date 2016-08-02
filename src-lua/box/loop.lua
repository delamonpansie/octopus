local box = require 'box'
local fiber = require 'fiber'
local assert = assert
local say_error = say_error
local pairs, type = pairs, type

-- exmaple
--
-- local box = require 'box'
-- local loop = require 'box.loop'
-- local function action(ushard, state, conf)
--     if state == nil then
--         state = initial_state_for_ushard() -- we just entered into ushard
--     end
--     local space = ushard:object_space(conf.space)
--     if we_do_some_useful_things_and_succeed(state, space, conf) then
--         modify(state, conf)
--         if done_with_this_ushard(state, conf) then
--             return nil, sleep
--         else
--             return state, sleep
--         end
--     else -- go to next ushard
--         return nil, sleep
--     end
-- end
--
-- loop.start{name="some_loop", exec=action, space=1, other_op=2}}

module(...)

local function loop(state, conf)
    if state == nil then
        state = {ushardn = -1}
    end
    if state.user == nil then
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

    local ok, state_or_err, sleep = box.with_txn(state.ushardn, conf.exec, state.user, conf.user)
    if not ok then
        say_error('box.loop "%s": %s', conf.name, state_or_err)
        state.user = nil -- so we are moving to next ushard
    else
        state.user = state_or_err
    end
    return state, sleep
end

function start(conf)
    local newconf = {exec = conf.exec, name = conf.name, user={}}
    local name = conf.name
    assert(type(name) == "string")
    for k,v in pairs(conf) do
        newconf.user[k] = v
    end
    newconf.user.exec = nil
    fiber.loop(name, loop, newconf)
end
