local G = _G
local bit = bit
local ffi = require("ffi")

local print = print
local type = type
local assert = assert
local format = string.format
local debug = debug
local error = error
local tonumber = tonumber

module(...)

ffi.cdef[[
typedef double ev_tstamp;

typedef struct ev_io {
 int active;
 int pending;
 int priority;
 void *data;
 char coro;
 const char *cb_src;
 void (*cb)(struct ev_io *w, int revents);
 struct ev_watcher_list *next;
 int fd;
 int events;
} ev_io;

typedef struct ev_timer {
 int active;
 int pending;
 int priority;
 void *data;
 char coro;
 const char *cb_src;
 void (*cb)(struct ev_timer *w, int revents);
 ev_tstamp at;
 ev_tstamp repeat;
} ev_timer;
]]

ffi.cdef[[
struct iproto_retcode {
	uint32_t msg_code;
	uint32_t data_len;
	uint32_t sync;
	uint32_t ret_code;
};

struct iproto {
	uint32_t msg_code;
	uint32_t data_len;
	uint32_t sync;
};
]]

if ffi.abi('32bit') then
   ffi.cdef('typedef unsigned int uintptr_t;')
elseif ffi.abi('64bit') then
   ffi.cdef('typedef unsigned long int uintptr_t;')
else
   error('ABI not supported by LuaJIT.ffi')
end

ffi.cdef[[
struct iovec {
 void *iov_base;
 size_t iov_len;
};

struct netmsg_head {
 struct netmsg_tailq { struct netmsg *tqh_first; struct netmsg **tqh_last; } q;
 struct palloc_pool *pool;
 size_t bytes;
};

struct netmsg {
 struct netmsg_head *head;
 unsigned offset, count;
 struct { struct netmsg *tqe_next; struct netmsg **tqe_prev; } link;
 struct iovec dummy;
 struct iovec iov[1024];
 uintptr_t ref[1024];
};

enum conn_memory_ownership {
 MO_MALLOC = 0x01,
 MO_STATIC = 0x02,
 MO_SLAB = 0x03,
 MO_MY_OWN_POOL = 0x10
};

struct conn {
 struct palloc_pool *pool;
 struct tbuf *rbuf;
 int fd, ref;
 struct netmsg_head out_messages;
 enum conn_memory_ownership memory_ownership;
 enum { CLOSED, IN_CONNECT, CONNECTED } state;
 struct link { struct conn *le_next; struct conn **le_prev; } link;
 struct processing_link { struct conn *tqe_next; struct conn **tqe_prev; } processing_link;
 ev_io in, out;
 struct service *service;
 char peer_name[22];
 ev_timer timer;
};

struct conn_wrap {
 struct conn *ptr;
};

void netmsg_release(struct netmsg *m);
void netmsg_rewind(struct netmsg_head *h, struct netmsg_mark *mark);
void netmsg_getmark(struct netmsg_head *h, struct netmsg_mark *mark);

void net_add_iov(struct netmsg_head *o, const void *buf, size_t len);
struct iovec *net_reserve_iov(struct netmsg_head *o);
void net_add_iov_dup(struct netmsg_head *o, const void *buf, size_t len);
void net_add_ref_iov(struct netmsg_head *o, uintptr_t ref, const void *buf, size_t len);
void net_add_obj_iov(struct netmsg_head *o, struct tnt_object *obj, const void *buf, size_t len);

int conn_unref(struct conn *c);

]]

local C = ffi.C

netmsg_t = ffi.typeof('struct netmsg *')
iproto_t = ffi.typeof('struct iproto *')
iproto_retcode_t = ffi.typeof('struct iproto_retcode')

local cm = {}
function cm:bytes() return self.ptr.out_messages.bytes end

function cm:add_iov_dup(obj, len) C.net_add_iov_dup(self.ptr.out_messages, obj, len) end
function cm:add_iov_ref(obj, len, v) C.net_add_ref_iov(self.ptr.out_messages, v or ref(obj), obj, len) end
function cm:add_iov_string(str)
   if #str < 512 then
      C.net_add_iov_dup(self.ptr.out_messages, str, #str)
   else
      C.net_add_ref_iov(self.ptr.out_messages, ref(str), str, #len)
   end
end
function cm:add_iov_iproto_header(request)
   local request = ffi.new(iproto_t, request)
   local header = ffi.new(iproto_retcode_t, request.msg_code, 4, request.sync)
   self:add_iov_ref(header, ffi.sizeof(iproto_retcode_t))
   return header
end

local conn_mt = {
   __index = cm,
   __gc = function(c) C.conn_unref(c.ptr) end
}
local conn_t = ffi.metatype(ffi.typeof('struct conn_wrap'), conn_mt)

function conn(ptr)
   local c = conn_t(ptr)
   c.ptr.ref = c.ptr.ref + 1
   return c
end

function iproto(ptr)
   return iproto_t(ptr)
end


-- gc ref's
local ref_registry = {[0]=nil}

function ref(obj)
   local ref = ref_registry[0]
   if ref then
      ref_registry[0] = ref_registry[ref]
   else
      ref = #ref_registry + 1
   end
   ref_registry[ref] = obj
   return ref * 2 + 1 -- ref to lua objects have lower bit set
end

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
