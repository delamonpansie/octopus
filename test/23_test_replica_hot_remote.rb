#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class StandAloneEnvX < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

class MasterEnv < StandAloneEnvX
  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = ":33034"
EOD
  end
end

class SlaveEnv < StandAloneEnvX
  def initialize
    super
    @primary_port = 33023
  end

  def test_root
    super + "_slave"
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33034"
local_hot_standby = 1
EOD
  end
end

def wait_for(n=100)
  n.times do
    return if yield
    sleep 0.05
  end
  raise "wait_for failed"
end

MasterEnv.clean do
  puts "start"
  start
  puts "connect"
  master = connect
  master.ping

  SlaveEnv.clean do
    start
    slave = connect

    master.insert ['0', 'a', 'b', 'c', 'd']
    wait_for { slave.select_nolog(['0']).length > 0 }


    master.select ['0']
    slave.select ['0']
  end
end

