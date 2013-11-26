local G = _G
local bit = bit
local ffi = require("ffi")

local print = print
local assert = assert
local error = error
local tonumber = tonumber
local require = require
local pcall = pcall

module(...)

ffi.cdef 'struct conn_wrap { struct conn *ptr; };'

local C = ffi.C

local netmsg_t = ffi.typeof('struct netmsg *')
local iproto_ptr_t = ffi.typeof('struct iproto *')
local iproto_t = ffi.typeof('struct iproto')
local iproto_retcode_t = ffi.typeof('struct iproto_retcode')

local ref -- forward decl

local netmsg_op = {}
function netmsg_op:add_iov_ref(obj, len, v) C.net_add_ref_iov(self, v or ref(obj), obj, len) end
function netmsg_op:add_iov_string(str) C.net_add_ref_iov(self, ref(str), str, #str) end
function netmsg_op:add_iov_iproto_header(request)
   assert(request ~= nil)
   local request = ffi.new(iproto_ptr_t, request)
   local header = ffi.new(iproto_retcode_t, 0, request.msg_code, 4, request.sync)
   self:add_iov_ref(header, ffi.sizeof(iproto_retcode_t))
   return header
end

local netmsg_head_t = ffi.metatype(ffi.typeof('struct netmsg_head'), { __index = netmsg_op,
								       __gc = C.netmsg_head_release })
function netmsg()
   local h = netmsg_head_t()
   ffi.C.netmsg_head_init(h, nil)
   return h
end


local conn_op = {}
function conn_op:bytes() return self.ptr.out_messages.bytes end
function conn_op:add_iov_dup(obj, len) C.net_add_iov_dup(self.ptr.out_messages, obj, len) end
function conn_op:add_iov_ref(obj, len, v) C.net_add_ref_iov(self.ptr.out_messages, v or ref(obj), obj, len) end
function conn_op:add_iov_string(str)
   if #str < 512 then
      C.net_add_iov_dup(self.ptr.out_messages, str, #str)
   else
      C.net_add_ref_iov(self.ptr.out_messages, ref(str), str, #str)
   end
end
function conn_op:add_iov_iproto_header(request)
   assert(request ~= nil)
   local request = ffi.new(iproto_ptr_t, request)
   local header = ffi.new(iproto_retcode_t, 0, request.msg_code, 4, request.sync)
   self:add_iov_ref(header, ffi.sizeof(header))
   return header
end
local netmsg_mark_t = ffi.typeof('struct netmsg_mark')
function conn_op:pcall(f, ...)
   local mark = ffi.new(netmsg_mark_t)
   ffi.C.netmsg_getmark(self.ptr.out_messages, mark)
   local ok, errmsg = pcall(f, self, ...)
   if not ok then
      ffi.C.netmsg_rewind(self.ptr.out_messages, mark)
      error(errmsg)
   end
end

local conn_t = ffi.metatype(ffi.typeof('struct conn_wrap'), { __index = conn_op,
							      __gc = function(c) C.conn_unref(c.ptr) end
							    })
function conn(ptr)
   local c = conn_t(ptr)
   c.ptr.ref = c.ptr.ref + 1
   return c
end

function iproto(ptr)
   return iproto_ptr_t(ptr)
end

function iproto_copy(req)
   local c = ffi.new(iproto_t, req.data_len)
   ffi.copy(c, req, ffi.sizeof(c))
   return c
end

-- gc ref's
local ref_registry = {[0]=0}

ref = function(obj)
   local ref = ref_registry[0]
   if ref > 0 then
      ref_registry[0] = ref_registry[ref]
   else
      ref = #ref_registry + 1
   end
   ref_registry[ref] = obj
   return ref * 2 + 1 -- ref to lua objects have lower bit set
end

G.ref = ref
function G.__netmsg_unref(m, from)
   local m = netmsg_t(m)
   for i = from, m.count do
      local ref = tonumber(m.ref[i])
      if bit.band(ref, 1) == 1 then
	 ref = bit.rshift(ref, 1)
	 ref_registry[ref] = ref_registry[0]
	 ref_registry[0] = ref
      end
   end
end
