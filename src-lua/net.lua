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

if ffi.abi('32bit') then
   ffi.cdef [[typedef unsigned int uintptr_t;
	      typedef int ssize_t;]]
elseif ffi.abi('64bit') then
   ffi.cdef[[typedef unsigned long int uintptr_t;
	     typedef long int ssize_t;]]
else
   error('ABI not supported by LuaJIT.ffi')
end

ffi.cdef[[
typedef unsigned int socklen_t;
typedef unsigned short int sa_family_t;
struct sockaddr {
	sa_family_t sa_family;
	char sa_data[14];
};

enum {
	PF_UNSPEC = 0,
	PF_LOCAL = 1,
	PF_INET	= 2
};
enum {
	SOCK_STREAM = 1,
	SOCK_DGRAM = 2,
	SOCK_RAW = 3,
	SOCK_RDM = 4,
	SOCK_SEQPACKET = 5,
	SOCK_DCCP = 6,
	SOCK_PACKET = 10,
	SOCK_CLOEXEC = 02000000,
	SOCK_NONBLOCK = 04000
};
enum {
	MSG_OOB = 0x01,
	MSG_PEEK = 0x02,
	MSG_DONTROUTE = 0x04,
	MSG_CTRUNC = 0x08,
	MSG_PROXY = 0x10,
	MSG_TRUNC = 0x20,
	MSG_DONTWAIT = 0x40,
	MSG_EOR = 0x80,
	MSG_WAITALL = 0x100,
	MSG_FIN = 0x200,
	MSG_SYN = 0x400,
	MSG_CONFIRM = 0x800,
	MSG_RST = 0x1000,
	MSG_ERRQUEUE = 0x2000,
	MSG_NOSIGNAL = 0x4000,
	MSG_MORE = 0x8000,
	MSG_WAITFORONE = 0x10000,
	MSG_CMSG_CLOEXEC = 0x40000000
};
enum {
	IPPROTO_IP = 0,
	IPPROTO_HOPOPTS = 0,
	IPPROTO_ICMP = 1,
	IPPROTO_IGMP = 2,
	IPPROTO_IPIP = 4,
	IPPROTO_TCP = 6,
	IPPROTO_EGP = 8,
	IPPROTO_PUP = 12,
	IPPROTO_UDP = 17,
	IPPROTO_IDP = 22,
	IPPROTO_TP = 29,
	IPPROTO_DCCP = 33,
	IPPROTO_IPV6 = 41,
	IPPROTO_ROUTING = 43,
	IPPROTO_FRAGMENT = 44,
	IPPROTO_RSVP = 46,
	IPPROTO_GRE = 47,
	IPPROTO_ESP = 50,
	IPPROTO_AH = 51,
	IPPROTO_ICMPV6 = 58,
	IPPROTO_NONE = 59,
	IPPROTO_DSTOPTS = 60,
	IPPROTO_MTP = 92,
	IPPROTO_ENCAP = 98,
	IPPROTO_PIM = 103,
	IPPROTO_COMP = 108,
	IPPROTO_SCTP = 132,
	IPPROTO_UDPLITE = 136,
	IPPROTO_RAW = 255,
	IPPROTO_MAX
};
typedef uint16_t in_port_t;
typedef uint32_t in_addr_t;
struct in_addr {
	in_addr_t s_addr;
};
struct sockaddr_in {
	sa_family_t sin_family;
	in_port_t sin_port;
	struct in_addr sin_addr;
	unsigned char sin_zero[sizeof(struct sockaddr) -
			       (sizeof(unsigned short int)) -
			       sizeof(in_port_t) - sizeof(struct in_addr)];
};

struct iovec {
	void *iov_base;
	size_t iov_len;
};
struct msghdr {
	void *msg_name;
	socklen_t msg_namelen;
	struct iovec *msg_iov;
	size_t msg_iovlen;
	void *msg_control;
	size_t msg_controllen;
	int msg_flags;
};

extern int socket(int domain, int type, int protocol);
extern ssize_t sendto(int fd, const void *buf, size_t n,
		      int flags, const struct sockaddr *addr, socklen_t addr_len);

extern ssize_t readv(int fd, const struct iovec *iovec, int count);
extern ssize_t writev(int fd, const struct iovec *iovec, int count);
extern int socket(int domain, int type, int protocol);
extern int socketpair(int domain, int type, int protocol, int fds[2]);
extern int bind(int fd, const struct sockaddr *addr, socklen_t len);
extern int getsockname(int fd, struct sockaddr *addr, socklen_t *len);
extern int connect(int fd, const struct sockaddr *addr, socklen_t len);
extern int getpeername(int fd, struct sockaddr *addr, socklen_t *len);
extern ssize_t send(int fd, const void *buf, size_t n, int flags);
extern ssize_t recv(int fd, void *buf, size_t n, int flags);
extern ssize_t sendto(int fd, const void *buf, size_t n,
		      int flags, const struct sockaddr *addr,
		      socklen_t addr_len);
extern ssize_t recvfrom(int fd, void *buf, size_t n,
			int flags, struct sockaddr *addr,
			socklen_t *addr_len);
extern ssize_t sendmsg(int fd, const struct msghdr *message, int flags);
extern ssize_t recvmsg(int fd, struct msghdr *message, int flags);
extern int getsockopt(int fd, int level, int optname,
		      void *optval, socklen_t *optlen);
extern int setsockopt(int fd, int level, int optname,
		      const void *optval, socklen_t optlen);
extern int listen(int fd, int n);
extern int accept(int fd, struct sockaddr *addr, socklen_t *addr_len);
extern int shutdown(int fd, int how);
extern uint32_t ntohl(uint32_t netlong);
extern uint16_t ntohs(uint16_t netshort);
extern uint32_t htonl(uint32_t hostlong);
extern uint16_t htons(uint16_t hostshort);
]]

require 'net_cdef'
ffi.cdef 'struct conn_wrap { struct conn *ptr; };'

local C = ffi.C

netmsg_t = ffi.typeof('struct netmsg *')
iproto_ptr_t = ffi.typeof('struct iproto *')
iproto_t = ffi.typeof('struct iproto')
iproto_retcode_t = ffi.typeof('struct iproto_retcode')


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

function ref(obj)
   local ref = ref_registry[0]
   if ref > 0 then
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
