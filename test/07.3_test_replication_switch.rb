#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

OBJ_SPACE_CFG = <<EOD
nop_hb_delay = 3600
run_crc_delay = 3600


object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD

class MasterEnv1 < StandAloneEnv
  def initialize
    super
    @primary_port = 33013
    @feeder_port = 33016
    @admin_port = 33015
    @suffix = "master1"
  end

  def test_root
    super + "_#@suffix"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "0.0.0.0:#@feeder_port"
#{OBJ_SPACE_CFG}
EOD
  end
end

class MasterEnv2 < MasterEnv1
  def initialize
    super
    @primary_port = 33023
    @feeder_port = 33026
    @admin_port = 33025
    @suffix = "master2"
  end
end

class SlaveEnv < StandAloneEnv
  def initialize
    super
    @primary_port = 33033
    @feeder_port = 33036
    @admin_port = 33035
    @replication_source = 33016
  end

  def test_root
    super + "_slave"
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:#@replication_source"
#{OBJ_SPACE_CFG}
EOD
  end
end


MasterEnv1.clean do
  start
  master1 = connect
  master1.ping


  MasterEnv2.clean do
    start
    master2 = connect
    master2.ping

    [1,2,3].each do |i|
      master1.insert [i,i]
      master2.insert [i,i]
    end
    master2.insert [4, 42]

    SlaveEnv.clean do
      start
      sleep(1.5)
      slave = connect
      slave.select 1,2,3,4

      @replication_source = 33026
      File.open(SlaveEnv::ConfigFile, "w+") { |io| io.write(config) }
      puts `/bin/echo -e "relo conf\\nexit" | nc 0 33035`

      sleep(0.5)
      slave.select 1,2,3,4
    end
  end
end
