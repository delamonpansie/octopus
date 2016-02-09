#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class MasterEnv < RunEnv
  def test_root
    super << "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = ":33034"

object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "STR"
EOD
  end

  task :setup => ["feeder_init.lua"]
  file "feeder_init.lua" do
    f = open("feeder_init.lua", "w")
    f.write <<-EOD
local ffi = require('ffi')
local box_old_nop = "\1\0\0\0\0\0"
local box_nop = "\0\0\0\0"
ffi.cdef 'void *malloc(int)'
local buf = ffi.C.malloc(1024)
function replication_filter.id_xlog(row)
    print(row)

    if row.scn == 3296 or row.scn == 3297 or row.scn == 3298 then
	local new = ffi.new('struct row_v12 *', buf)
	ffi.copy(new, row, ffi.sizeof('struct row_v12'))
	if row.tag == 0x8003 then
            new.len = #box_old_nop
            ffi.copy(new.data, box_old_nop, #box_old_nop)
        else
            new.tag = 0x8033 -- NOP|TAG_WAL
	    new.len = #box_nop
	    ffi.copy(new.data, box_nop, #box_nop)
        end
	return new
    end
    return true
end
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
admin_port = 33025
wal_feeder_addr = "127.0.0.1:33034"
wal_feeder_filter = "id_xlog"

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

# $options = {valgrind: true}

SlaveEnv.connect_eval do |env|
  wait_for "readable 00000000000000000301.snap" do
    FileTest.readable?("00000000000000000301.snap")
  end

  wait_for "non empty select [99]" do  select_nolog([99]).length > 0 end

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

  wait_for "non empty select [999]" do select_nolog([999]).length > 0 end
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

