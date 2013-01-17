#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class MasterEnv < StandAloneEnv
  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "ANY:33034"
#{$io_compat}
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end

  task :setup => ["feeder_init.lua"]
  file "feeder_init.lua" do
    f = open("feeder_init.lua", "w")
    f.write <<-EOD
      function replication_filter.id_xlog(obj)
        local row = feeder.crow(obj)
        print("row lsn:" .. tostring(row.lsn) ..
              " scn:" .. tostring(row.scn) ..
              " tag:" .. row.tag ..
              " cookie:" .. tostring(row.cookie) ..
              " tm:" .. row.tm)

        local box_nop = "\01\00\00\00\00\00"

        if row.scn == 2198 or row.scn == 2199 then
                return box_nop
        end
        return true
      end
    EOD
    f.close
  end
end

class SlaveEnv < StandAloneEnv
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
wal_feeder_filter = "id_xlog"
#{$io_compat}
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
  master = connect
  master.ping

  100.times do |i|
    master.insert [i, i + 1, "abc", "def"]
    master.insert [i, i + 1, "abc", "def"]
  end

  SlaveEnv.clean do
    start
    sleep(0.5)
    slave = connect
    slave.select [99]

    Process.kill("STOP", pid)
    1000.times do |i|
      master.insert [i, i + 1, "ABC", "DEF"]
      master.insert [i, i + 1, "ABC", "DEF"]
    end
    Process.kill("CONT", pid)
    sleep(0.5)
    slave.select [998]
    slave.select [999]
  end
end

