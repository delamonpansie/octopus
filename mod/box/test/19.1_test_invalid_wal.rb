#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
rows_per_wal=5
EOD
  end
end

Env.env_eval do |env|
  start
  8.times do |i|
    snapshot if i == 4
    connect.insert [1,2,3]
  end

  wait_for "snapshot" do
    File.open "00000000000000000005.snap"
  end
  rm "00000000000000000005.xlog"
  stop

  octopus [], :out => "/dev/null", :err => "/dev/null"
  log_line = wait_for "expected failure" do
    File.open('octopus.log').each_line.grep(/exception.*missing/)[0].sub(/.* F> /, '')
  end
  puts log_line
end
