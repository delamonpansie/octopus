#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'
require 'yaml'

class MasterEnv < StandAloneEnv
  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
nop_hb_delay=0.01
run_crc_delay=0.01
wal_feeder_bind_addr = "ANY:33034"
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

class SlaveEnv < StandAloneEnv
  def initialize
    super
    @primary_port = 33023
    @secondary_port = 0
    @admin_port = 22025
  end

  def test_root
    super + "_slave"
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33034"
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
end

MasterEnv.clean do
  start
  sleep(0.1)
  master = connect
  master.ping

  SlaveEnv.clean do
    start
    sleep(0.1)
    slave = connect

    master.insert [1,2,3]
    sleep(0.4)

    TCPSocket.open(0, 22025) do |s|
      s.puts "sh in"
      s.puts "quit"
      r = YAML.load(s.read)
      info = r["info"]

      %w/recovery_lag recovery_run_crc_status/.each do |p|
        puts "#{p}: #{info[p]}"
      end
    end
  end
end
