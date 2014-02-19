local tostring, string_format, string_dump, string_byte = tostring, string.format, string.dump, string.byte
local type, pairs, ipairs, pcall = type, pairs, ipairs, pcall
local setmetatable, getmetatable, getupvalue, getinfo = setmetatable, debug.getmetatable, debug.getupvalue, debug.getinfo
local table_insert, table_concat = table.insert, table.concat
local io_write = io.write

local dump, fdisp, disp
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
    local notempty = false
    local numidx = 1
    for k, v in pairs(value) do
        notempty = true
        if numidx ~= 1 then
            write(',\n')
        else
            write('\n')
        end
        if k == numidx then
            numidx = numidx + 1
            write(indent[il])
        else
            numidx = nil
            dumpk(k)
        end
        dumpv(v)
    end
    return notempty
end

local c_functions = {}
for _,lib in pairs{'_G', 'string', 'table', 'math',
    'io', 'os', 'coroutine', 'package', 'debug', 'ffi', 'bit', 'jit'} do
  local t = _G[lib] or {}
  lib = lib .. "."
  if lib == "_G." then lib = "" end
  for k,v in pairs(t) do
    if type(v) == 'function' and not pcall(string_dump, v) then
      c_functions[v] = lib..k
    end
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
        if value == ddump then
            write('ddump.dump')
            return
        end
        local ok, body = pcall(string_dump, value)
        if not ok then
            if c_functions[value] then
                write(c_functions[value])
            else
                write('<', tostring(value), '>')
            end
            return
        end

        if ref[value] then write('closure(', ref[value], ')'); return end

        if getupvalue(value, 1) == nil then
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
            write('table(', ref[value], ')')
            return
        end

        local meta = getmetatable(value)
        if ref[value] == nil then
            if meta then
                write('setmetatable({')
            else
                write('{')
            end
        else
            ref[value] = tostring(value):gsub('^%w+: ', '') -- "foo: 0xdeadbead" -> "0xdeadbead"
            write('table(',ref[value],', {')
        end


        il = il + 1
        local notempty = dumpt(value)
        write("}")
        if meta then
            if notempty then
                write(',\n',indent[il])
            else
                write(',')
            end
            dumpv(meta)
            write(')')
        elseif ref[value] ~= nil then
            write(')')
        end

        il = il - 1
    end
    },
    {__index = function(t, k) return dump_other end}
)


dump = function (value, w)
    ref = {}
    closure_count = 0
    write = w or io_write
    fill_ref(value)
    dumpv(value)
end

return dump
