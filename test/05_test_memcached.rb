#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    <<EOD
pid_file = "tarantool.pid"

slab_alloc_arena = 1
logger = "exec cat - >> tarantool.log"
primary_port = 33013
wal_fsync_delay = 10

memcached = 1
object_space = []

EOD
  end
end

Env.clean.with_server do
  ENV["T_MEMD_USE_DAEMON"]="127.0.0.1:33013"
  result = `prove ../../third_party/memcached/t`
  puts result.gsub(/, \d+ wallclock.*/, "")
end
