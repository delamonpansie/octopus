#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def initialize
    @test_root_suffix = "_master"
    super
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = ":33034"
#{$io_compat}

object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "STR"
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
admin_port = 33025
wal_feeder_addr = "127.0.0.1:33034"
#{$io_compat}

object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "STR"
EOD
  end
end

master = MasterEnv.new
master.start
master.connect_eval do
  self.connect_name = "master"
  ping
end

SlaveEnv.connect_eval do |env|
  self.connect_name = "slave"
  wait_for "readable 00000000000000000001.snap" do
    FileTest.readable?("00000000000000000001.snap")
  end
  env.stop
  env.start
  wait_for "reconnect" do reconnect end

  master.connect_eval do
    self.connect_name = 'master'
    insert [1, 2, "abc", "def"]
  end
  wait_for "non empty select [1]" do  select_nolog([1]).length > 0 end
  select [1]

  env.stop
  env.start
  wait_for "reconnect" do reconnect end
  wait_for "non empty select [1]" do  select_nolog([1]).length > 0 end
  select [1]
end



