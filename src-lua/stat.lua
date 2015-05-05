stat = stat or {}
stat.collectors = {}

stat.klass = {__index={}}
local meths = stat.klass.__index
local _ev_now = os.ev_now

local function merge_stat(small, big)
    for k, v in pairs(small) do
        if type(v) == 'number' then
            big[k] = (big[k] or 0) + v
        elseif type(v) == 'table' then
            if not big[k] then
                big[k] = {[0] = 0, [1] = 0, [2] = v[2], [3] = v[3]}
            end
            local b = big[k]
            b[0] = b[0] + v[0]
            b[1] = b[1] + v[1]
            if b[2] > v[2] then b[2] = v[2] end
            if b[3] < v[3] then b[3] = v[3] end
        end
    end
end

local function shift_all()
    while true do
        fiber.sleep(1)
        local now = _ev_now()
        for _, st in pairs(stat.collectors) do
            local cur
            if st.get_current then
                local ok, val = xpcall(st.get_current, debug.traceback)
                if not ok then
                    say_error(val)
                end
                cur = ok and val or {}
            else
                cur = st.current
                st.current = {}
            end
            for i = 4, 1, -1 do
                st.records[i] = st.records[i - 1]
            end
            st.records[0] = cur
            merge_stat(st.records[0], st.periodic)
            st.period_stop = now
        end
    end
end
fiber.create(shift_all)

local function is_callable(v)
    return v and
      (type(v) == 'function' or
       type(getmetatable(v)) == 'table' and is_callable(getmetatable(v).__call))
end

function stat.klass:new(name, get_current)
    if stat.collectors[name] then
        return stat.collectors[name]
    end
    if get_current and not is_callable(get_current) then
        error('stat collector get_current should be callable or empty')
    end
    local now = _ev_now()
    local recs = setmetatable({
        name = name,
        get_current = get_current,
        current = {},
        records = {[0] = {}},
        periodic = {},
        period_start = now,
        period_stop = now
    }, self)
    stat.collectors[name] = recs
    return recs
end

function stat.new(name, get_current)
    return stat.klass:new(name, get_current)
end

function stat.new_with_graphite(name, get_current)
    local stats = stat.new(name, get_current)
    if graphite then
        graphite.add_cb(name)
    end
    return stats
end

function meths:admin_out(out)
    local sum = {}
    for i = 0, #self.records do
        if self.records[i] then
            merge_stat(self.records[i], sum)
        end
    end

    if self.name == "stat" then
        table.insert(out, "statistics:\r\n")
    else
        table.insert(out, "statistics@"..self.name..":\r\n")
    end

    local ordered_keys = {}
    for k in pairs(sum) do
        table.insert(ordered_keys, k)
    end
    table.sort(ordered_keys)

    for k, key in pairs(ordered_keys) do
        local val = sum[key]
        if type(val) == 'number' then
            local rps = (val or 0) / (#self.records + 1)
            local line = string.format("  %-25s { rps:  %-8i }\r\n", key .. ':', rps)
            table.insert(out, line)
        elseif type(val) == 'table' then
            local aval = val[0] / val[1]
            local line = string.format("  %-25s { avg: %-08.3f, min: %-8.3f, max %-8.3f }\r\n", key .. ':', aval, val[2], val[3])
            table.insert(out, line)
        end
    end
end

function stat.print()
    local out = {}
    for k, s in pairs(stat.collectors) do
        s:admin_out(out)
    end
    return table.concat(out)
end

function meths:clear()
    self.current = {}
    self.records = {[0] = {}}
    self.periodic = {}
    local now = _ev_now()
    self.periodic_start = now
    self.periodic_stop = now
end

function stat.clear()
    for k, s in pairs(stat.collectors) do
        s:clear()
    end
end

function meths:get_periodic()
    if not self.period_start then
        return {}
    end
    local diff_time = self.period_stop - self.period_start
    if diff_time < 0.8 then
        return {}
    end
    local res = {}
    for k, v in pairs(self.periodic) do
        if type(v) == 'number' then
            res[k] = v / diff_time
        elseif type(v) == 'table' then
            res[k..".avg"] = v[0] / v[1]
            res[k..".cnt"] = v[1]
            res[k..".min"] = v[2]
            res[k..".max"] = v[3]
        end
    end
    self.periodic = {}
    self.period_start = self.period_stop
    return res
end

function meths:add1(name)
    local cur = self.current
    cur[name] = (cur[name] or 0) + 1
end

function meths:add(name, val)
    local cur = self.current
    cur[name] = (cur[name] or 0) + val
end

function meths:avg(name, val)
    local cur = self.current
    local a = cur[name]
    if a then
        a[0] = a[0] + val
        a[1] = a[1] + 1
        if a[2] > val then a[2] = val end
        if a[3] < val then a[3] = val end
    else
        cur[name] = {[0] = val, [1] = 1, [2] = val, [3] = val}
    end
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
