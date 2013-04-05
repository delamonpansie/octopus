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


class MasterEnv < StandAloneEnv
  def initialize
    super
    @primary_port = 33033
    @feeder_port = 33036
    @admin_port = 33035
  end

  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "ANY:33037"
#{OBJ_SPACE_CFG}
EOD
  end
end


class SlaveEnv1 < StandAloneEnv
  def initialize
    super
    @primary_port = 33013
    @secondary_port = 33016
    @admin_port = 33015
    @suffix = "slave1"
  end

  def test_root
    super + "_#@suffix"
  end

  def config
    super + <<EOD
local_hot_standby = 1
wal_feeder_addr = "127.0.0.1:33037"
#{OBJ_SPACE_CFG}
EOD
  end
end

class SlaveEnv2 < SlaveEnv1
  def initialize
    super
    @primary_port = 33013
    @secondary_port = 33026
    @admin_port = 33025
    @suffix = "slave2"
  end

  def config
    super + <<EOD
wal_dir = "../var_slave1"
EOD
  end
end


MasterEnv.clean do
  start
  master = connect


  SlaveEnv1.clean do |slave_env1|
    start

    master.insert [1]

    SlaveEnv2.clean do
      ln_s "../var_slave1/00000000000000000001.snap", "00000000000000000001.snap"
      
      start

      master.insert [2]

      sleep 0.5

      slave = connect #connect to primary port

      master.select [1,2]
      slave.select [1,2]

      slave_env1.stop
      master.insert [3]
      sleep 0.5

      slave = connect # reconnect, because slave1 is stopped
      master.select [1,2,3]
      slave.select [1,2,3]
    end
  end
end
