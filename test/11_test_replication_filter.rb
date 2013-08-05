#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class MasterEnv < StandAloneEnv
  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "0:33034"
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
      function replication_filter.test_filter(obj)
        local row = feeder.crow(obj)
        print("row lsn:" .. tostring(row.lsn) ..
              " scn:" .. tostring(row.scn) ..
              " tag:" .. row.tag ..
              " cookie:" .. tostring(row.cookie) ..
              " tm:" .. row.tm)

        if feeder.pass_tag[row.tag] then
        	return true
	end
	if row.scn % 2 == 0 then
         	return false
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
wal_feeder_filter = "test_filter"
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"
sync_scn_with_lsn = 0
panic_on_scn_gap = 0
EOD
  end
end


MasterEnv.clean do
  start
  master = connect

  SlaveEnv.clean do
    start
    # File.open("octopus.log").lines.each {|l| puts l }
    slave = connect


    4.times do |i|
      master.insert [i]
    end
    sleep 0.1

    master.select 0,1,2,3
    slave.select 0,1,2,3

    stop
    start
    puts "Slave\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
  end
  puts "Master\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
end

