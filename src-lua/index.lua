local ffi = require('ffi')
local objc = require('objc')
local object, varint32 = object, varint32

local assert = assert
local tonumber = tonumber
local error = error
local type = type
local tostring = tostring
local format = string.format
local select = select
local pcall = pcall
local table = table
local assertarg = assertarg
local setmetatable = setmetatable

local ipairs, pairs = ipairs, pairs
local string = string
local loadstring = loadstring
local print, printf = print, printf

module(...)

ffi.cdef[[
struct BasicIndex {
	struct { void *isa; };
	const struct index_conf conf;
};
struct Tree {
	struct { void *isa; };
	const struct index_conf conf;
};
]]

local find_node = objc.msg_lookup('find_node:')
local iterator_init = objc.msg_lookup("iterator_init")
local iterator_init_with_node = objc.msg_lookup("iterator_init_with_node:")
local iterator_init_with_object = objc.msg_lookup("iterator_init_with_object:")
local iterator_init_with_direction = objc.msg_lookup("iterator_init_with_direction:")
local iterator_init_with_node_direction = objc.msg_lookup("iterator_init_with_node:direction:")
local iterator_init_with_object_direction = objc.msg_lookup("iterator_init_with_object:direction:")
local iterator_next = objc.msg_lookup("iterator_next")
local get = objc.msg_lookup('get:')

local maxnodesize = ffi.sizeof('struct index_node') + 8 * ffi.sizeof('union index_field')
local node = ffi.cast('struct index_node *', ffi.C.malloc(maxnodesize))
local strbuf = ffi.new('char[?]', 5 + 0xffff)
gen = {node = node, strbuf = strbuf}

local function packfield(ftype, field, key)
    if ftype == ffi.C.NUM16 then
	field.u16 = key
    elseif ftype == ffi.C.NUM32 then
	field.u32 = key
    elseif ftype == ffi.C.NUM64 then
	field.u64 = key
    elseif ftype == ffi.C.STRING then
	if #key > 0xffff then
	    error("key too big", 4)
	end
	field.str.len = #key
	if #key <= 8 then
	    ffi.copy(field.str.data.bytes, key, #key)
	else
	    field.str.data.ptr = key
	end
    else
	error("unknown index field_type:" .. tostring(ftype), 4)
    end
end

local cgen_mt = {
    __index = {
        emit = function (self, fmt, lbindings)
            for k, v in pairs(lbindings or {}) do
                fmt = string.gsub(fmt, '$' .. k, v)
            end
            for k, v in pairs(self.bindings) do
                fmt = string.gsub(fmt, '$' .. k, v)
            end
            table.insert(self.code, fmt)
        end,
        bind = function (self, sym, value)
            self.bindings[sym] = value
            table.insert(self.sym_stack, sym)
        end,
        bind_pop = function (self, n)
            for i = 1, n or 1 do
                local sym = table.remove(self.sym_stack)
                self.bindings[sym] = nil
            end
        end,
        gen = function (self)
            local code = table.concat(self.code, "\n")
            return code
        end
    }
}

local function cgen(bindings)
    return setmetatable({code = {},
                         bindings = bindings or {},
                         sym_stack = {}},
                        cgen_mt)
end

local function gen_packfield(e, index, i, key)
    e:bind('offset', index.conf.offset[i])
    e:bind('key', key)

    e:emit('do')
    e:emit("    local field = ffi.cast(index_field, node.key.chr + $offset)")
    local ftype = index.conf.field_type[i]
    if ftype == ffi.C.NUM16 then
	e:emit("    field.u16 = $key", {key = key})
    elseif ftype == ffi.C.NUM32 then
        e:emit("    field.u32 = $key", {key = key})
    elseif ftype == ffi.C.NUM64 then
        e:emit("    field.u64 = $key", {key = key})
    elseif ftype == ffi.C.STRING then
        e:emit('    if #$key > 0xffff then error("key too big", 4) end')
        e:emit('    field.str.len = #$key')
	e:emit('    if #$key <= 8 then')
	e:emit('        ffi.copy(field.str.data.bytes, $key, #$key)')
	e:emit('    else')
	e:emit('        field.str.data.ptr = $key')
	e:emit('    end')
    else
	error("unknown index field_type:" .. tostring(ftype), 4)
    end
    e:emit('end')
    e:bind_pop(2)
end

local function gen_packnode(index)
    local e = cgen()

    e:emit('local ffi = require("ffi")')
    e:emit('local node = index.gen.node')
    e:emit('local strbuf = index.gen.strbuf')
    e:emit('local void = ffi.typeof("void *")')
    e:emit('local index_field = ffi.typeof("union index_field *")')

    if index.conf.cardinality == 1 and index.conf.field_type[0] == ffi.C.STRING then
        e:emit('return function(self, key)')
        e:emit('    if #key > 0xffff then error("key too big") end')
        e:emit('    local n = varint32.write(strbuf, #key)')
        e:emit('    ffi.copy(strbuf + n, key, #key)')
        e:emit('    node.key.ptr = strbuf')
        e:emit('    node.obj = ffi.cast(void, 1)')
        e:emit('    return node')
        e:emit('end')
        return loadstring(e:gen())()
    end

    local keys = {}
    for i = 1, index.conf.cardinality do
        table.insert(keys, 'k' .. i)
    end
    e:bind('args', 'self, ' .. table.concat(keys, ', '))

    e:emit('return function ($args)')
    for i, key in ipairs(keys) do
        e:bind('key',  key)
        if i == 1 then
            e:emit('    if $key == nil then error("empty key") end')
        else
            e:emit('    if $key == nil then return node end')
        end
        gen_packfield(e, index, i - 1, key)
        e:emit('    node.obj = ffi.cast(void, $i)', {i = i})
        e:bind_pop()
    end
    e:emit('     return node')
    e:emit('end')

    return loadstring(e:gen())()
end

local function packerr(err, fname, index, ...)
    local itype = {}
    local atype = {}
    for i = 0, index.conf.cardinality - 1 do
	local t = "UNKNOWN"
	if index.conf.field_type[i] == ffi.C.NUM16 then
	    t = "NUM16"
	elseif index.conf.field_type[i] == ffi.C.NUM32 then
	    t = "NUM32"
	elseif index.conf.field_type[i] == ffi.C.NUM64 then
	    t = "NUM64"
	elseif index.conf.field_type[i] == ffi.C.STRING then
	    t = "STRING"
	else
	    t = tostring(index.conf.field_type[i])
	end
	table.insert(itype, t)
    end
    for i = 1, select('#', ...) do
	table.insert(atype, tostring(type(select(i, ...))))
	-- TODO: check string size
    end
    local msg = format("bad argument #? to method '%s' ({%s} expected, got {%s}) <%s> %s %s",
		       fname, table.concat(itype, ', '), table.concat(atype, ', '),
		       err, tostring(index), tostring(index.conf))
    error(msg, 3)
end

local magic_key = {}
local legacy_mt
-- legacy iter interface interface
function iter(index, key)
    assertarg(index, legacy_mt, 1, 2)
    index = index[magic_key]

    if index.__ptr.conf.field_type[0] ~= ffi.C.STRING and type(key) == 'string' then
        key = tonumber(key)
    end
    return index:iter(key)
end

legacy_mt = {
    __index = function(index, key)
        assertarg(index, legacy_mt, 1, 2)
        index = index[magic_key]
        if index.__ptr.conf.field_type[0] ~= ffi.C.STRING and type(key) == 'string' then
            key = tonumber(key)
        end
        return index:find(key)
    end,
}
local function legacy_proxy(index)
    return setmetatable({[magic_key] = index}, legacy_mt)
end

local int32_t = ffi.typeof('int32_t')
local uint32_t = ffi.typeof('uint32_t')
local int = ffi.typeof('int')
local dir_decode = setmetatable({ forward = ffi.C.iterator_forward,
                                  backward = ffi.C.iterator_backward, },
                                { __index = function() return ffi.C.iterator_forward end })

local function iter_next(index)
    return object(iterator_next(index.__ptr))
end

local basic_mt = {
    __index = {
        packnode = function(self, ...)
            local ok, node = pcall(self.__packnode, self.__ptr, ...)
            if not ok then
                packerr(node, "find", self.__ptr, ...)
            end
            return node
        end,
        find = function(self, ...)
            local node = self:packnode(...)
            return object(find_node(self.__ptr, node))
        end,
        size = function(self)
            return tonumber(ffi.cast(uint32_t, objc.msg_send(self.__ptr, "size")))
        end,
        slots = function(self)
            return tonumber(ffi.cast(uint32_t, objc.msg_send(self.__ptr, "slots")))
        end,
        get = function(self, i)
            assert(self.__ptr.conf.type == ffi.C.HASH)
            return object(get(self.__ptr, ffi.cast(int32_t, i)))
        end,
        type = function(self)
            if self.__ptr.conf.type == ffi.C.HASH then
                return "HASH"
            elseif self.__ptr.conf.type == ffi.C.TREE then
                return "TREE"
            else
                error("bad index type", 2)
            end
        end,
        iter = function (self, ...)
            if select('#', ...) == 0 or select(1, ...) == nil then
                iterator_init(self.__ptr)
            elseif type(select(1, ...)) == 'table' then
                local init = select(1, ...)
                assert(init.__obj)
                iterator_init_with_object(self.__ptr, init.__obj)
            else
                local node = self:packnode(...)
                iterator_init_with_node(self.__ptr, node)
            end
            return iter_next, self
        end
    }
}

local tree_mt = {
    __index = {
        diter = function (self, direction, ...)
            if type(direction) == 'string' then
                direction = dir_decode[direction]
            end
            if select('#', ...) == 0 or select(1, ...) == nil then
                iterator_init_with_direction(self.__ptr, int(direction))
            elseif type(select(1, ...)) == 'table' then
                local init = select(1, ...)
                assert(init.__obj)
                iterator_init_with_object_direction(self.__ptr, init.__obj, int(direction))
            else
                local node = self:packnode(...)
                iterator_init_with_node_direction(self.__ptr, node, int(direction))
            end
            return iter_next, self
        end,
        iter = function(self, ...) return self:diter("forward", ...) end,
	riter = function(self, ...) return self:diter("backward", ...) end
   }
}
setmetatable(tree_mt.__index, basic_mt)

local index_mt = { [tonumber(ffi.C.TREE)] = tree_mt,
                   [tonumber(ffi.C.HASH)] = basic_mt }
setmetatable(index_mt, { __index = function() assert(false) end })

local function proxy(index)
    local p = { __ptr = index,
                __packnode = gen_packnode(index) }
    return setmetatable(p, index_mt[tonumber(index.conf.type)])
end
function cast(cdata)
    local index = proxy(cdata)
    return legacy_proxy(index), index
end
