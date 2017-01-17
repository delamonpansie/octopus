local box = require 'box'
local fiber = require 'fiber'
local assert = assert
local say_error = say_error
local pairs, type = pairs, type
local ev_now = os.ev_now
local tostring = tostring
local loops_stat = stat.new_with_graphite('loops')

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
    local end_sleep = _M.testing and 0.03 or 1
    local first = box.next_primary_ushard_n(-1)
    if first == -1 then
        return nil, end_sleep
    end
    if state == nil then
        state = {states={}, sleeps={}}
    end
    local ushardn = first
    local now = ev_now()
    local min_sleep_till = now + end_sleep
    local states, sleeps = {}, {}
    repeat
        local sleep_till = state.sleeps[ushardn] or 0
        local ustate = state.states[ushardn]
        if sleep_till <= now then
            loops_stat:add1(conf.name .. "_box_cnt")
            local ok, state_or_err, sleep = box.with_txn(ushardn, conf.exec, ustate, conf.user)
            fiber.gc()
            if not ok then
                say_error('box.loop "%s"@%d: %s', conf.name, ushardn, state_or_err)
                loops_stat:add1(conf.name .. "_box_error")
                state_or_err = nil
                sleep = end_sleep
            elseif state_or_err == nil and (sleep or 0) < end_sleep then
                sleep = end_sleep
            end
            now = ev_now()
            sleep_till = now + (sleep or end_sleep)
            ustate = state_or_err
        end
        states[ushardn] = ustate
        sleeps[ushardn] = sleep_till
        if sleep_till < min_sleep_till then
            min_sleep_till = sleep_till
        end
        ushardn = box.next_primary_ushard_n(ushardn)
    until ushardn == -1 or sleeps[ushardn]

    if ushardn == -1 then
        return nil, end_sleep
    end
    state.states = states
    state.sleeps = sleeps

    return state, (min_sleep_till <= now and 0.001 or (min_sleep_till - now))
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
