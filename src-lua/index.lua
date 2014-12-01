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

local printf = printf

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

local index_field = ffi.typeof('union index_field *')
local void = ffi.typeof('void *')
local function packnode(index, ...)
    if index.conf.cardinality == 1 and index.conf.field_type[0] == ffi.C.STRING then
	local key = ...
	if #key > 0xffff then
	    error("key too big")
	end
	local n = varint32.write(strbuf, #key)
	ffi.copy(strbuf + n, key, #key)
	node.key.ptr = strbuf
	node.obj = ffi.cast(void, 1)
    else
	for i = 0, select('#', ...) - 1 do
	    local field = ffi.cast(index_field, node.key.chr + index.conf.offset[i])
	    packfield(index.conf.field_type[i], field, select(i + 1, ...))
	end
	node.obj = ffi.cast(void, select('#', ...))
    end
    return node
end
local function packnode1(index, k)
    if index.conf.field_type[0] == ffi.C.STRING then
	if #k > 0xffff then
	    error("key too big")
	end
	local n = varint32.write(strbuf, #k)
	ffi.copy(strbuf + n, k, #k)
	node.key.ptr = strbuf
    else
        local field = ffi.cast(index_field, node.key.chr + index.conf.offset[0])
        packfield(index.conf.field_type[0], field, k)
    end
    node.obj = ffi.cast(void, 1)
    return node
end
local function packnode2(index, k1, k2)
    local field = ffi.cast(index_field, node.key.chr + index.conf.offset[0])
    packfield(index.conf.field_type[0], field, k1)
    node.obj = ffi.cast(void, 1)
    if k2 ~= nil then
        field = ffi.cast(index_field, node.key.chr + index.conf.offset[1])
        packfield(index.conf.field_type[1], field, k2)
        node.obj = ffi.cast(void, 2)
    end
    return node
end
local function packnode3(index, k1, k2, k3)
    local field = ffi.cast(index_field, node.key.chr + index.conf.offset[0])
    packfield(index.conf.field_type[0], field, k1)
    node.obj = ffi.cast(void, 1)
    if k2 ~= nil then
        field = ffi.cast(index_field, node.key.chr + index.conf.offset[1])
        packfield(index.conf.field_type[1], field, k2)
        node.obj = ffi.cast(void, 2)
        if k3 ~= nil then
            field = ffi.cast(index_field, node.key.chr + index.conf.offset[2])
            packfield(index.conf.field_type[2], field, k3)
            node.obj = ffi.cast(void, 3)
        end
    end
    return node
end
local packnode_reg = setmetatable( { [1] = packnode1,
                                     [2] = packnode2,
                                     [3] = packnode3 },
                                   { __index = function () return packnode end } )

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
            local ok, node = pcall(self.__packnode or packnode, self.__ptr, ...)
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
                __packnode = packnode_reg[index.conf.cardinality] }
    return setmetatable(p, index_mt[tonumber(index.conf.type)])
end
function cast(cdata)
    local index = proxy(cdata)
    return legacy_proxy(index), index
end
