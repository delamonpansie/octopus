#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def test_root
    super << "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = ":33034"
EOD
  end
end

class SlaveEnv < RunEnv
  def initialize
    @primary_port = 33023
    @test_root_suffix = "_slave"
    super
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33034"
local_hot_standby = 1
EOD
  end
end

master = MasterEnv.new.connect_eval do
  ping
  self
end

SlaveEnv.connect_eval do
  master.insert ['0', 'a', 'b', 'c', 'd']
  wait_for { select_nolog(['0']).length > 0 }


  master.select ['0']
  select ['0']
end

