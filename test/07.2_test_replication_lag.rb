#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

require 'yaml'

class MasterEnv < RunEnv
  def initialize
    @test_root_suffix = "_master"
    super
  end

  def config
    super + <<-EOD
    nop_hb_delay=0.01
    run_crc_delay=0.01
    wal_feeder_bind_addr = "0:33034"
    EOD
  end
end

class SlaveEnv < RunEnv
  def initialize
    @primary_port = 33023
    @test_root_suffix = "_slave"
    super
  end

  def config
    super + <<-EOD
    secondary_port = 0
    admin_port = 22025
    wal_feeder_addr = "127.0.0.1:33034"
    EOD
  end
end


master = MasterEnv.new.connect_eval do
  ping
  self
end

SlaveEnv.new.env_eval do start end

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
