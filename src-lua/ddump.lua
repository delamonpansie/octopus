local tostring, string_format, string_dump = tostring, string.format, string.dump
local type, pairs, pcall = type, pairs, pcall
local setmetatable, getmetatable, getupvalue, getinfo = setmetatable, debug.getmetatable, debug.getupvalue, debug.getinfo
local table_insert, table_concat = table.insert, table.concat
local io_write = io.write

local disp
local ref = {}
local closure_count = 0
local write = io_write

local il = 0
local indent = setmetatable({}, {__index = function(t, k)
                                     local s = {}
                                     for i = 1, k do table_insert(s, "  ") end
                                     t[k] = table_concat(s)
                                     return t[k]
                                end})

local function dumpv(value)
    disp[type(value)](value)
end

local function dumpt(value)
    local numidx = 1
    for k, v in pairs(value) do
        if numidx ~= 1 then write(',\n') end
        if k == numidx then
            numidx = numidx + 1
            write(indent[il])
        else
            numidx = nil
            write(indent[il], '[')
            dumpv(k)
            write('] = ')
        end
        dumpv(v)
    end
end

local function dumpf(value)
    local i = getinfo(value, "S")
    write("<function: ", i.short_src, '@', i.linedefined, ">")
end

disp = setmetatable({
    string = function(value) write(string_format('%q', value)) end,
    number = function(value) write(value) end,
    boolean = function(value) write(tostring(value)) end,
    ['nil'] = function(value) write('nil') end,
    ['function'] = function(value)
        local ok, body = pcall(string_dump, value)
        if not ok then write('<', tostring(value), '>'); return end

        if ref[value] then write('closure(', ref[value], ')'); return end

        if getupvalue(value, 1) == nil then
            -- write(string_format('loadstring(%q)', body))
            dumpf(value)
        else
            closure_count = closure_count + 1
            ref[value] = closure_count
            write('closure(', ref[value], ", ")
            dumpf(value)
            write(', {\n')
            il = il + 1
            local i = 1
            while true do
                local name, upvalue = getupvalue(value, i)
                if name == nil then break
                elseif i ~= 1 then write(',\n') end

                write(indent[il], '[')
                dumpv(name)
                write('] = ')
                dumpv(upvalue)
                i = i + 1
            end
            write("})")
            il = il - 1
        end
    end,
    ['table'] = function(value)
        if(ref[value]) then
            write('table(', ref[value], ')')
            return
        end

        ref[value] = tostring(value):gsub('^%w+: ', '') -- "foo: 0xdeadbead" -> "0xdeadbead"
        write('table(', ref[value], ', {\n')

        il = il + 1
        dumpt(value)
        write('}')

        local meta = getmetatable(value)
        if meta then
            write(', {\n')
            dumpt(meta)
            write('}')
        end

        write(')')
        il = il - 1
    end
    },
    {__index = function(t, k) return function (value) write('<', tostring(value), '>') end end}
)


local function dump(value, w)
    ref = {}
    closure_count = 0
    write = w or io_write
    dumpv(value)
end

return dump