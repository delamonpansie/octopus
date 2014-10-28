#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

OBJ_SPACE_CFG = <<EOD
nop_hb_delay = 3600
run_crc_delay = 3600
EOD

class MasterEnv1 < RunEnv
  def initialize
    @primary_port = 33013
    @admin_port = 33015
    @test_root_suffix = "_master"
    super
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "0.0.0.0:33016"
#{OBJ_SPACE_CFG}
EOD
  end
end

class MasterEnv2 < RunEnv
  def initialize
    @primary_port = 33023
    @admin_port = 33025
    @test_root_suffix = "_master2"
    super
  end
  def config
    super + <<-EOD
wal_feeder_bind_addr = "0.0.0.0:33026"
#{OBJ_SPACE_CFG}
EOD
  end
end

class SlaveEnv < RunEnv
  def initialize
    @primary_port = 33033
    @admin_port = 33035
    @test_root_suffix = "_slave"
    super
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33016"
#{OBJ_SPACE_CFG}
EOD
  end
end


master1 = MasterEnv1.new.connect_eval do
  ping
  self
end


master2 = MasterEnv2.new.connect_eval do
  ping
  self
end

[1,2,3].each do |i|
  master1.insert [i,i]
  master2.insert [i,i]
end
master2.insert [4, 42]


SlaveEnv.connect_eval do |env|
  wait_for ("select from master1 replica") { select_nolog(3).length > 0 }
  select 1,2,3,4

  env.env_eval do
    File.open(SlaveEnv::ConfigFile, "a") do |io|
      io.puts 'wal_feeder_addr = "127.0.0.1:33026"'
    end
  end

  TCPSocket.open(0, 33035) do |s|
    s.puts "reload conf"
    s.puts "exit"
    puts s.read
  end

  wait_for ("select from master2 replica") { select_nolog(4).length > 0 }
  select 1,2,3,4
end
