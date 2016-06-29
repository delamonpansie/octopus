stat = stat or {}
stat.collectors = {}
local ffi = require 'ffi'

function stat._report(base)
    local st = assert(stat.collectors[base], "no collector registered")
    local cur = st.get_current() or {}
    for k,v in pairs(cur) do
        k = tostring(k)
        if type(v) == 'number' then
            ffi.C.stat_report_accum(k, #k, v)
        elseif type(v) == 'table' then
            if v.exact then
                ffi.C.stat_report_exact(k, #k, v.exact)
            elseif v[0] and v[1] and v[2] and v[2] then
                ffi.C.stat_report_double(k, #k, v[0], v[1], v[2], v[3])
            elseif v.sum and v.cnt and v.min and v.max then
                ffi.C.stat_report_double(k, #k, v.sum, v.cnt, v.min, v.max)
            end
        end
    end
end

stat.klass = {__index={}}
local meths = stat.klass.__index
local _ev_now = os.ev_now


local function is_callable(v)
    return v and
      (type(v) == 'function' or
       type(getmetatable(v)) == 'table' and is_callable(getmetatable(v).__call))
end

function stat.klass:new(name, get_current)
    name = tostring(name)
    if stat.collectors[name] then
        return stat.collectors[name]
    end
    if get_current and not is_callable(get_current) then
        error('stat collector get_current should be callable or empty')
    end
    local recs = setmetatable({
        name = name,
        get_current = get_current,
    }, self)
    stat.collectors[name] = recs
    if get_current then
        recs.base = ffi.C.stat_register_callback(name, ffi.C.stat_lua_callback)
    else
        recs.base = ffi.C.stat_register_named(name)
    end
    stat.collectors[recs.base] = recs
    return recs
end

function stat.new(name, get_current)
    return stat.klass:new(name, get_current)
end

stat.new_with_graphite = stat.new

function meths:add1(name)
    name = tostring(name)
    ffi.C.stat_collect_named(self.base, name, #name, 1)
end

function meths:add(name, val)
    name = tostring(name)
    ffi.C.stat_collect_named(self.base, name, #name, val)
end

function meths:avg(name, val)
    name = tostring(name)
    ffi.C.stat_collect_named_double(self.base, name, #name, val)
end

do
    local sum_mt = {
        __index = function(t, k) return 0 end
    }
    local statmt = {
        __index = function(t, k)
            local s = setmetatable({run = 0, ok = 0, [0] = 0}, sum_mt)
            t[k] = s
            return s
        end
    }
    function stat.request_collector(desc)
        local name, gen_name = desc.name, desc.gen_name
        if not name or (gen_name and not is_callable(gen_name)) then
            error("request_collector needs {name='stat', gen_name = function(k) return graphitename(k) end")
        end
        local collect = setmetatable({}, statmt)
        stat.klass:new(name, function()
            local cur = {}
            for f, st in pairs(collect) do
                for k, v in pairs(st) do
                    local n = gen_name and gen_name(f) or f
                    if v > 0 then
                        if k == 'run' then
                            cur[n] = v
                        elseif k == 'ok' then
                            cur[n..':ok'] = v
                        else
                            cur[string.format('%s:%04x', n, k)] = v
                        end
                    end
                end
            end
            collect = setmetatable({}, statmt)
            return cur
        end)
        return {
            add_run = function(name)
                local cur = collect[name]
                cur.run = cur.run + 1
            end,
            add_ok = function(name)
                local cur = collect[name]
                cur.ok = cur.ok + 1
            end,
            add_rcode = function(name, rcode)
                local cur = collect[name]
                cur[rcode] = cur[rcode] + 1
            end
        }
    end
end
