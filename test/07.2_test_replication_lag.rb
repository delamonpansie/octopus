#!/usr/bin/ruby
# coding: utf-8

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
  # force creation of first xlog
  # otherwise [reader follow] will be polling wal_dir
  # for new files once per second
  self.insert [1,2,3]
  self
end

SlaveEnv.new.env_eval do
  start
  master.insert [1,2,3]
  wait_for { open('|./octopus --cat 00000000000000000003.xlog 2>/dev/null').each_line.grep(/scn:3/).length > 0 }

end


TCPSocket.open(0, 22025) do |s|
  s.puts "sh in"
  s.puts "quit"
  r = YAML.load(s.read)
  info = r["info"]

  %w/recovery_lag recovery_run_crc_status/.each do |p|
    puts "#{p}: #{info[p]}"
  end
end
