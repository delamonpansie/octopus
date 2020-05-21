#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def test_root
    super << "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "0:33034"
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end

  task :setup => ["feeder_init.lua"]
  file "feeder_init.lua" do
    f = open("feeder_init.lua", "w")
    f.write <<-EOD
local wal = require 'wal'
local box_op = require 'box.op'
require 'box.string_ext'

local tou32 = string.tou32

local shard

replication_filter.test_filter = box_op.wal_filter(function (row, arg)
  if row == nil then
     shard = tonumber(arg)
     return nil
  end
  print(row)

  local cmd = row.cmd
  if cmd == nil then
      return true
  end

  if cmd.op_name == 'insert' then
      if cmd.tuple:u32field(0) % 2 ~= shard then
          return row:nop()
      end
      return row:replace(cmd.n, {cmd.tuple[0], tou32(cmd.tuple:u32field(1) + 1)})
  elseif cmd.op_name == 'update' then
      if cmd.key:u32field(0) % 2 ~= shard then
          return row:nop()
      end
      for _, v in ipairs(cmd.update_mops) do
          if v[2] == "add" then
              v[3] = v[3] + 1  -- preserve type
          end
      end

      return row:update(cmd.n, {cmd.key[0]}, cmd.update_mops)
  elseif cmd.op_name == 'delete' then
      if cmd.key:u32field(0) % 2 ~= shard then
          return row:nop()
      end
      return true
  end
  return true
end)
    EOD
    f.close
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
wal_feeder_filter = "test_filter"
wal_feeder_filter_arg = "1"
sync_scn_with_lsn = 0
panic_on_scn_gap = 0

object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
end

master_env = MasterEnv.new
master = master_env.env_eval do
  start
  connect
end

SlaveEnv.new.env_eval do
  start
  slave = connect

  6.times do |i|
    master.insert [i, i]
  end
  master.update_fields 4, [1, :add, 1]
  master.update_fields 5, [1, :add, 1]

  wait_for "non empty xlog" do
    File.size("00000000000000000002.xlog") > 400
  end

  master.select 0,1,2,3,4,5
  slave.select 0,1,2,3,4,5

  master.delete 1
  master.delete 2

  sleep 0.01
  master.select 1, 2
  slave.select 1, 2


  restart
  master_env.stop

  puts "Slave\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
  master_env.cd do
    puts "Master\n" + `./octopus --cat 00000000000000000002.xlog 2>/dev/null| sed 's/tm:[^ ]* //'` + "\n"
  end
end

