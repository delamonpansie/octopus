local graphite_addr = '127.0.0.1:3333'

--
--
-- this script will periodically send stats to graphite
-- to enable: put it to cfg.workdir, add reloadfile('graphite.lua')
-- to init.lua and edit graphite_addr abowe
--
--

local ffi = require 'ffi'
local fiber = require 'fiber'
require 'net' -- for ffi.cdef

if not graphite_loaded then
   graphite_loaded = true
   local loop = function ()
      while true do
	 if type(graphite_sender) == 'function' then
	    graphite_sender()
	 end
	 fiber.sleep(1)
      end
   end
   fiber.create(loop)
   ffi.cdef[[
extern int gethostname(char *name, size_t len);
extern char *custom_proc_title;
extern char *primary_addr;

typedef long int time_t;
time_t time(time_t *t);
]]
end

local function gethostname() 
   local buf = ffi.new('char[?]', 64)
   local result = "unknown"
   if ffi.C.gethostname(buf, ffi.sizeof(buf)) ~= -1 then
      result = ffi.string(buf)
   end
   if ffi.C.primary_addr ~= nil then
      result = result .. ":" .. ffi.string(ffi.C.primary_addr)
   end
   if ffi.C.custom_proc_title ~= nil then
      local proctitle = ffi.string(ffi.C.custom_proc_title)
      if #proctitle > 0 then
	 result = result .. "@" .. proctitle
      end
   end
   return result
end

local function graphite()
   local hostname = gethostname()
   local time = tostring(tonumber(ffi.C.time(nil)))
   local msg = {}
   for k, v in pairs(stat.records[0]) do
      table.insert(msg, "my.octopus.")
      table.insert(msg, hostname)
      table.insert(msg, ".")
      table.insert(msg, k)
      table.insert(msg, " ")
      table.insert(msg, v)
      table.insert(msg, " ")
      table.insert(msg, time)
      table.insert(msg, "\n")
   end
   return table.concat(msg)
end

local addr = ffi.new('struct sockaddr')
local sock = ffi.C.socket(ffi.C.PF_INET, ffi.C.SOCK_DGRAM, 0)
ffi.C.atosin(graphite_addr, addr)

function graphite_sender ()
    local msg = graphite()
    ffi.C.sendto(sock, msg, #msg, 0, addr, ffi.sizeof(addr))      
end

