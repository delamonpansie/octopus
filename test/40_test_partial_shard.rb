#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def test_root
    super << "_master"
  end

  def initialize
    super
    @secondary_port = nil
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "0:33034"
object_space[0].index[0].key_field[0].type = "STR"
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

replication_filter.partial = function (row, arg)
  if row == nil then
     shard = tonumber(arg)
     say_info("partial filter for shard:" .. tostring(shard))
     return nil
  end
  if row.scn == 0 or row.scn == -1 then
     return true
  end
  local cmd = box_op.wal_parse(row.tag, row.data, row.len)
  if cmd and cmd.op_name == 'insert' then
      local key = cmd.tuple:strfield(0)
      local mod = tonumber(key) % 4
      if mod ~= shard then
          return false
      end
      row.shard_id = shard
      return row
  else
      return false
  end
end
    EOD
    f.close
  end
end

class SlaveEnv < RunEnv
  def initialize
    @port_offset = 10
    @test_root_suffix = "_slave"
    super
    @secondary_port = nil
  end

  def config
    @hostname = "two"
    super(:hostname => @hostname, :object_space => false)
  end

  def meta(arg)
    puts "# #@hostname.meta(#{arg})"
    puts `perl ../../client/shard/shardbox.pl -s=localhost:#{33013 + (@port_offset || 0)} #{arg}`
    puts
  end
end

master_env = MasterEnv.new
master = master_env.env_eval do
  start
  connect
end

slave_env = SlaveEnv.new
slave_env.env_eval do
  start
  slave = connect

  master.connect_name = "legacy_master"
  10.times do |i|
    master.insert [i.to_s, 'val']
  end

  master.select 0,1,2,3,4,5

  (0..3).each do |shard_id|
    slave_env.meta("shard #{shard_id} create por")
    slave_env.meta("shard #{shard_id} obj_space 0 create hash unique string 0")
  end

  (0..3).each do |shard_id|
    slave_env.meta("shard #{shard_id} add_replica one")
    slave_env.meta("shard #{shard_id} type part")
    slave_env.meta("shard #{shard_id} master one")
  end

  sleep 0.2

  keys = (0..16).map{|a| a.to_s}.to_a
  slave.select *keys, :shard => 0
  slave.select *keys, :shard => 1
  slave.select *keys, :shard => 2
  slave.select *keys, :shard => 3

  slave_env.restart
  slave.reconnect
  slave.ping

  sleep 0.2

  slave.select *keys, :shard => 0
  slave.select *keys, :shard => 1
  slave.select *keys, :shard => 2
  slave.select *keys, :shard => 3

  slave_env.snapshot

  slave_env.stop
  4.times do |i|
    master.insert [(i + 10).to_s, "!"]
  end

  slave_env.start
  slave.reconnect
  slave.ping

  slave.select *keys, :shard => 0
  slave.select *keys, :shard => 1
  slave.select *keys, :shard => 2
  slave.select *keys, :shard => 3

end

