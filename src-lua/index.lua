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
struct OpaqueIndex;
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

local opaquet = ffi.typeof('struct OpaqueIndex *')
local indext = ffi.typeof('struct BasicIndex *')
local treet = ffi.typeof('struct Tree *')

-- legacy iter interface interface
function iter(index, key)
   if ffi.typeof(index) == opaquet then
      index = ffi.cast(indext, index)
   end
   assertarg(index, indext, 1, 2)
   if index.conf.field_type[0] ~= ffi.C.STRING and type(key) == 'string' then
       key = tonumber(key)
   end
   return index:iter(key)
end

local legacy_mt = {
   __index = function(index, key)
      if ffi.typeof(index) == opaquet then
         index = ffi.cast(indext, index)
      end
      return index:find(key)
   end,
   __metatable = {}
}
ffi.metatype('struct OpaqueIndex', legacy_mt)

local int32_t = ffi.typeof('int32_t')
local uint32_t = ffi.typeof('uint32_t')
local int = ffi.typeof('int')
local dir_decode = setmetatable({ forward = ffi.C.iterator_forward,
                                  backward = ffi.C.iterator_backward, },
                                { __index = function() return ffi.C.iterator_forward end })

local function iter_next(index)
   return object(iterator_next(index))
end

local function iter(index, ...)
    if select('#', ...) == 0 or select(1, ...) == nil then
        iterator_init(index)
    elseif type(select(1, ...)) == 'table' then
        local init = select(1, ...)
        assert(init.__obj)
        iterator_init_with_object(index, init.__obj)
    else
        local ok, node = pcall(packnode, index, ...)
        if not ok then
            packerr(node, "iter", index, ...)
        end
        iterator_init_with_node(index, node)
    end
    return iter_next, index
end

local function diter(index, direction, ...)
    if type(direction) == 'string' then
        direction = dir_decode[direction]
    end
    if select('#', ...) == 0 or select(1, ...) == nil then
        iterator_init_with_direction(index, int(direction))
    elseif type(select(1, ...)) == 'table' then
        local init = select(1, ...)
        assert(init.__obj)
        iterator_init_with_object_direction(index, init.__obj, int(direction))
    else
        local ok, node = pcall(packnode, index, ...)
        if not ok then
            packerr(node, "iter", index, ...)
        end
        iterator_init_with_node_direction(index, node, int(direction))
    end
    return iter_next, index
end

local basicindex_mt = {
    __index = {
	find = function(index, ...)
	    local ok, node = pcall(packnode, index, ...)
	    if not ok then
		packerr(node, "find", index, ...)
	    end
	    return object(find_node(index, node))
	end,
        size = function(index)
            return tonumber(ffi.cast(uint32_t, objc.msg_send(index, "size")))
        end,
	slots = function(index)
	    return tonumber(ffi.cast(uint32_t, objc.msg_send(index, "slots")))
	end,
	get = function(index, i)
	    assert(index.conf.type == ffi.C.HASH)
	    return object(get(index, ffi.cast(int32_t, i)))
	end,
        type = function(index)
            if index.conf.type == ffi.C.HASH then
                return "HASH"
            elseif index.conf.type == ffi.C.TREE then
                return "TREE"
            else
                error("bad index type", 2)
            end
        end,

        iter = iter
   }
}
ffi.metatype('struct BasicIndex', basicindex_mt)

local tree_mt = {
    __index = {
	find = basicindex_mt.__index.find,
        size = basicindex_mt.__index.size,
        slots = basicindex_mt.__index.slots,
        get = basicindex_mt.__index.get,
        type = basicindex_mt.__index.type,

        diter = diter,
        iter = function(index, ...) return diter(index, "forward", ...) end,
	riter = function(index, ...) return diter(index, "backward", ...) end
   }
}
ffi.metatype('struct Tree', tree_mt)

function cast(index)
    local legacy = ffi.cast(opaquet, index)
    if index:type() == 'TREE' then
        index = ffi.cast(treet, index)
    end
    return legacy, index
end
