local ffi = require('ffi')
local objc = require('objc')
local object, varint32 = object, varint32

local setmetatable = setmetatable
local assert = assert
local tonumber = tonumber
local error = error
local type = type
local tostring = tostring
local select = select
local assertarg = assertarg

local printf = printf
module(...)

ffi.cdef[[
struct OpaqueIndex;
struct Index {
	struct { void *isa; };
	const struct index_conf conf;
};
]]

local find_by_node = objc.msg_lookup('find_by_node:')
local iterator_init = objc.msg_lookup("iterator_init")
local iterator_init_with_node = objc.msg_lookup("iterator_init_with_node:")
local iterator_init_with_object = objc.msg_lookup("iterator_init_with_object:")
local iterator_next = objc.msg_lookup("iterator_next")
local get = objc.msg_lookup('get:')

local maxnodesize = ffi.sizeof('struct index_node') + 8 * ffi.sizeof('union index_field')
local nodebuf = ffi.new('char[?]', maxnodesize) -- keep this pointer live
local node = ffi.cast('struct index_node *', nodebuf)
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
	    ffi.copy(field.str.data, key, #key)
	else
	    field.str.ptr = key
	end
    else
	error("unknown index field_type:" .. tostring(ftype), 4)
    end
end

local index_field = ffi.typeof('union index_field *')
local void = ffi.typeof('void *')
-- local uintptr = ffi.typeof('intptr_t')
local function packnode(index, ...)
    if index.conf.cardinality == 1 and index.conf.field_type[0] == ffi.C.STRING then
	local key = ...
	if #key > 0xffff then
	    error("key too big", 3)
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

local opaquet = ffi.typeof('struct OpaqueIndex *')
local indext = ffi.typeof('struct Index *')

-- legacy iter interface interface
function iter(index, key)
   if ffi.typeof(index) == opaquet then
      index = ffi.cast(indext, index)
   end
   assertarg(index, indext, 1, 2)
   if index.conf.field_type[0] ~= ffi.C.STRING then
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

local function iter_next(index)
   return object(iterator_next(index))
end
local int32_t = ffi.typeof('int32_t')
local index_mt = {
    __index = {
	find = function(index, ...)
	    local node = packnode(index, ...)
	    return object(find_by_node(index, node))
	end,
	iter = function(index, ...)
	    if select('#', ...) == 0 or select(1, ...) == nil then
		iterator_init(index)
	    elseif type(select(1, ...)) == 'table' then
		local init = select(1, ...)
		assert(init.__obj)
		iterator_init_with_object(index, init.__obj)
	    else
		local node = packnode(index, ...)
		iterator_init_with_node(index, node)
	    end
	 return iter_next, index
	end,
	slots = function(index)
	    return tonumber(obj.msg_send(index, "slots"))
	end,
	get = function(index, i)
	    assert(index.conf.type == ffi.C.HASH)
	    return object(get(index, ffi.cast(int32_t, i)))
	end
   }
}
ffi.metatype('struct Index', index_mt)

function cast(index)
   local legacy = ffi.cast('struct OpaqueIndex *', index)
   return legacy, index
end
