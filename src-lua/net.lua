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
	uint8_t data[];
} __attribute__((packed));

struct iproto {
	uint32_t msg_code;
	uint32_t data_len;
	uint32_t sync;
	uint8_t data[];
} __attribute__((packed));
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

struct netmsg *netmsg_tail(struct netmsg_head *h);
struct netmsg *netmsg_concat(struct netmsg_head *dst, struct netmsg_head *src);
void netmsg_release(struct netmsg *m);
void netmsg_rewind(struct netmsg **m, struct netmsg_mark *mark);
void netmsg_getmark(struct netmsg *m, struct netmsg_mark *mark);

void net_add_iov(struct netmsg **m, const void *buf, size_t len);
struct iovec *net_reserve_iov(struct netmsg **m);
void net_add_iov_dup(struct netmsg **m, const void *buf, size_t len);
void net_add_ref_iov(struct netmsg **m, uintptr_t ref, const void *buf, size_t len);
void net_add_obj_iov(struct netmsg **m, struct tnt_object *obj, const void *buf, size_t len);

int conn_unref(struct conn *c);
]]

local C = ffi.C

local conn_t = ffi.typeof('struct conn *')
local netmsg_t = ffi.typeof('struct netmsg *')
local netmsg_ref_t = ffi.typeof('struct netmsg *[1]') -- mutable ptr to netmsg
local netmsg_head_t = ffi.typeof('struct netmsg_head &')
local iproto_t = ffi.typeof('struct iproto *')
local iproto_retcode_t = ffi.typeof('struct iproto_retcode')

function conn(ptr)
   c = conn_t(ptr)
   ffi.gc(c, C.conn_unref)
   return c
end

function iproto(ptr)
   return iproto_t(ptr)
end

local function ffi_type_assert(argn, want_type, arg)
   if not ffi.istype(want_type, arg) then
      error(format("bad argument #%i to '%s'(%s type expected, got %s)",
		   argn, debug.getinfo(2, "n").name, want_type, ffi.typeof(arg))
	    .. debug.traceback())
   end
   return true
end

function out_bytes(dst)
   if ffi.istype(conn_t, dst) then
      dst = dst[0].out_messages
   end
   ffi_type_assert(1, netmsg_head_t, dst)
   return dst.bytes

end

function netmsg_tail(dst)
   if ffi.istype(conn_t, dst) then
      dst = dst[0].out_messages
   else
      error("not implemented")
   end
   ffi_type_assert(1, netmsg_head_t, dst)
   return netmsg_ref_t(C.netmsg_tail(dst))
end

function add_iov_iproto_header(out, request)
   assert(out ~= nil)
   assert(request ~= nil)
   local request = iproto_t(request)
   local header = iproto_retcode_t(request.msg_code, 4, request.sync)
   add_iov_cdata(out, net_ref(header), header, ffi.sizeof(iproto_retcode_t))
   return header
end

function add_iov_dup_string(out, v)
   if (ffi.istype(conn_t, out)) then
      out = netmsg_tail(out)
   end
   ffi_type_assert(1, netmsg_ref_t, out)
   C.net_add_iov_dup(out, v, #v)
end

function add_iov_ref_string(out, v)
   if (ffi.istype(conn_t, out)) then
      out = netmsg_tail(out)
   end
   ffi_type_assert(1, netmsg_ref_t, out)
   C.net_add_ref_iov(out, net_ref(v), v, #v)
end

function add_iov_string(out, v)
   if #v < 512 then
      add_iov_dup_string(out, v)
   else
      add_iov_ref_string(out, v)
   end
end

function add_iov_cdata(out, v, ptr, len)
   if (ffi.istype(conn_t, out)) then
      out = netmsg_tail(out)
   end
   ffi_type_assert(1, netmsg_ref_t, out)
   -- assert(type(v) == number or ffi_type_assert(2, oct.object_t, v))
   C.net_add_ref_iov(out, v, ptr, len)
end


-- gc ref's
local net_ref_registry = {[0]=nil}

function net_ref(obj)
   local ref = net_ref_registry[0]
   if ref then
      net_ref_registry[0] = net_ref_registry[ref]
   else
      ref = #net_ref_registry + 1
   end
   net_ref_registry[ref] = obj
   return ref * 2 + 1 -- ref to lua objects have lower bit set
end

function G.__netmsg_unref(m, from)
   local m = netmsg_t(m)
   for i = from, m.count do
      local ref = tonumber(m.ref[i])
      if bit.band(ref, 1) == 1 then
	 ref = bit.rshift(ref, 1)
	 net_ref_registry[ref] = net_ref_registry[0]
	 net_ref_registry[0] = ref
      end
   end
end
