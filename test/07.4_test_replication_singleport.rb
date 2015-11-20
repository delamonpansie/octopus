#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def test_root
    super << "_master"
  end

  def config
    super + <<EOD
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
wal_feeder_addr = "127.0.0.1:33013"

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
  ping

  100.times do |i|
    insert [i, i + 1, "abc", "def"]
    insert [i, i + 1, "abc", "def"]
    insert [i, i + 1, "abc", "def"], :object_space => 1
    if i == 50 then
      master.snapshot
      wait_for "readable 00000000000000000154.snap" do
        FileTest.readable?("00000000000000000154.snap")
      end
    end
  end
end

SlaveEnv.connect_eval do |env|
  wait_for "readable 00000000000000000301.snap" do
    FileTest.readable?("00000000000000000301.snap")
  end

  wait_for { select_nolog([99]).length > 0 }

  select [99]
  select [99], :object_space => 1

  Process.kill("STOP", env.pid)
  master.connect_eval do
    1000.times do |i|
      insert [i, i + 1, "ABC", "DEF"]
      insert [i, i + 1, "ABC", "DEF"]
      insert [i, i + 1, "ABC", "DEF"], :object_space => 1
    end
  end
  Process.kill("CONT", env.pid)

  wait_for { select_nolog([999]).length > 0 }
  select [998]
  select [999]
  select [998], :object_space => 1
  select [999], :object_space => 1

  # verify that replica is able to read it's own xlog's
  env.stop
  env.start

  wait_for "reconnect" do reconnect end

  wait_for { select_nolog([999]) }
  select [998]
  select [999]
  select [998], :object_space => 1
  select [999], :object_space => 1
end

