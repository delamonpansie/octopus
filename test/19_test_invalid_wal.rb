#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.env_eval do
  touch '00000000000000000002.xlog.inprogress'
  start
  connect.ping
  stop
end


RunEnv.env_eval do
  invoke :start
  touch '00000000000000000002.xlog'
  octopus [], :out => "/dev/null", :err => "/dev/null"

  log_line = wait_for "expected failure" do
    File.open('octopus.log').each_line.grep(/not all WALs have been successfully read/)[0].sub(/.*> /, '')
  end
  puts log_line
end

RunEnv.env_eval do
  start
  connect.insert [1,2,3]
  stop

  x = File.open('00000000000000000002.xlog.tmp', 'w')
  File.open('00000000000000000002.xlog').each_line.each do |l|
    x.write(l)
    break if l == "\n"
  end
  x.close
  mv '00000000000000000002.xlog.tmp', '00000000000000000002.xlog'

  octopus [], :out => "/dev/null", :err => "/dev/null"
  log_line = wait_for "expected failure" do
    File.open('octopus.log').each_line.grep(/no valid rows were read/)[0].sub(/.*> /, '')
  end
  puts log_line
end
