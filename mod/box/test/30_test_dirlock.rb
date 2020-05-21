#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.env_eval do
  start
  connect.ping

  File.open("octopus.cfg", "a+") do |fd|
    fd.puts('pid_file = "pid2"')
    fd.puts("primary_port = 33023")
    fd.puts('logger = "exec cat - >> octopus.log2"')
  end

  octopus [], :out => "/dev/null", :err => "/dev/null"

  ret = wait_for("error message") do
    File.read("octopus.log2").match(/Can't lock wal_dir/)
  end
  puts ret
end
