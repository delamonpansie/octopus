#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class MasterEnv < StandAloneEnv
  def test_root
    super + "_master"
  end

  def config
    super + <<EOD
# normaly not needed. but because there is no WAL then test started (and thus no inotify watcher running),
# feeder will sleep until WAL dir rescan.
wal_dir_rescan_delay = 0.05

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
local wal = require 'wal'
local box = require 'box.op'

function replication_filter.test_filter(row)
  print(row)

  if row:tag_name() ~= 'wal_tag' and not row:tag_name():find('usr') then
  	return true
  end

  if row.scn < 5 and row.scn % 2 == 0 then
   	return false
  end

  local cmd = box.wal_parse(row.tag, row.data, row.len)
  if not cmd then
      print("can't parse cmd")
      return true
  end

  if cmd.op == box.op.UPDATE_FIELDS then
      for _, v in ipairs(cmd.update_mops) do
          if v[2] == "add" then
              v[3] = v[3] + 1  -- preserve type
          end
      end

      local _, data, len = box.pack.update(cmd.n, cmd.key:strfield(0), cmd.update_mops)
      return row:update_data(data, len)
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


    6.times do |i|
      master.insert [i, i]
    end
    master.update_fields 4, [1, :add, 1]
    master.update_fields 5, [1, :add, 1]
    sleep 0.1

    master.select 0,1,2,3,4,5
    slave.select 0,1,2,3,5

    stop
    start
    puts "Slave\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
  end
  puts "Master\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
end

