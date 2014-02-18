local tostring, string_format, string_dump, string_byte = tostring, string.format, string.dump, string.byte
local type, pairs, pcall = type, pairs, pcall
local setmetatable, getmetatable, getupvalue, getinfo = setmetatable, debug.getmetatable, debug.getupvalue, debug.getinfo
local table_insert, table_concat = table.insert, table.concat
local io_write = io.write

local fdsip, disp
local ref = {}
local closure_count = 0
local write = io_write

local function noop(value) end
local function _fill_ref(value)
    local fill = fdisp[type(value)]
    if fill then
        fill(value)
    end
end
local function _add_ref(value)
    local cnt = (ref[value] or 0) + 1
    ref[value] = cnt
    return cnt
end
fdisp = {
    ['function']  = function(value)
        if _add_ref(value) ~= 1 then
            return
        end
        if getupvalue(value, 1) ~= nil then
            local i = 1
            while true do
                local name, upvalue = getupvalue(value, i)
                if name == nil then break end
                _fill_ref(upvalue)
                i = i + 1
            end
        end
    end,
    ['table'] = function(value)
        if _add_ref(value) ~= 1 then
            return
        end
        for k, v in pairs(value) do
            _fill_ref(k)
            _fill_ref(v)
        end
        local meta = getmetatable(value)
        if meta then _fill_ref(meta) end
    end
}
local function fill_ref(value)
    _fill_ref(value)
    local _ref = {}
    for k, v in pairs(ref) do
        if v > 1 then
            _ref[k] = false
        end
    end
    ref = _ref
end

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

local ident_chars = {}
for i=string_byte('A'),string_byte('Z') do
    ident_chars[i] = true
    ident_chars[i+32] = true
end
for i=string_byte('0'),string_byte('9') do
    ident_chars[i] = true
end
ident_chars[string_byte('_')] = true
local reserved = {}
local lua_reserved_keywords = {
  'and', 'break', 'do', 'else', 'elseif', 'end', 'false', 'for',
  'function', 'if', 'in', 'local', 'nil', 'not', 'or', 'repeat',
  'return', 'then', 'true', 'until', 'while' }
for _, v in ipairs(lua_reserved_keywords) do
    reserved[v] = true
end

local function dumpk(value)
    if type(value) == 'string' and not reserved[value] then
        local is_ident = true
        for i=1,#value do
            if not ident_chars[string_byte(value, i)] then
                is_ident = false
                break
            end
        end
        if is_ident then
            write(indent[il], value, ' = ')
            return
        end
    end
    write(indent[il], '[')
    dumpv(value)
    write('] = ')
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
            dumpk(k)
        end
        dumpv(v)
    end
end

local function dumpf(value)
    local i = getinfo(value, "S")
    write("<function: ", i.short_src, '@', i.linedefined, ">")
end

local function dump_other(value)
    write('<', tostring(value), '>')
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

                dumpk(name)
                dumpv(upvalue)
                i = i + 1
            end
            write("})")
            il = il - 1
        end
    end,
    ['table'] = function(value)
        if ref[value] then
            write('tbl(', ref[value], ')')
            return
        end

        if ref[value] == nil then
            write('{\n')
            il = il + 1
            assert(il < 10)
            dumpt(value)
            write('}')
            il = il - 1
            return
        end

        ref[value] = tostring(value):gsub('^%w+: ', '') -- "foo: 0xdeadbead" -> "0xdeadbead"
        write('tbl{ [id]=',ref[value],',\n')

        il = il + 1
        dumpt(value)

        local meta = getmetatable(value)
        if meta then
            write(',\n',indent[il],'[meta]=')
            dumpv(meta)
        end

        write('}')
        il = il - 1
    end
    },
    {__index = function(t, k) return dump_other end}
)


local function dump(value, w)
    ref = {}
    closure_count = 0
    write = w or io_write
    fill_ref(value)
    dumpv(value)
end

return dump
