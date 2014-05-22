#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
rows_per_wal=100
io_compat=1
EOD
  end
end

Env.connect_eval do |env|
  ping
  insert [1,2,3]

  Process.kill('USR1', env.pid)

  wait_for "readable 00000000000000000002.snap" do
    FileTest.readable?("00000000000000000002.snap")
  end
end


env = Env.new
env.connect_eval do
  ping
  1000.times {|i| insert [i, i + 1, i + 2]}
env.stop
end

env.connect_eval do
  select 1, 500, 505, 999, 1001

  Process.kill('USR1', env.pid)
  insert [1]
  insert [2]
  env.stop

  wait_for "readable 00000000000000001001.snap" do
    FileTest.readable? '00000000000000001001.snap'
  end

  puts Dir.glob("*.snap").sort + ["\n"]
  puts Dir.glob("*.xlog").sort + ["\n"]
  puts File.open("00000000000000001001.snap").lines.take_while {|l| l != "\n" } + ["\n"]
  puts File.open("00000000000000001002.xlog").lines.take_while {|l| l != "\n" } + ["\n"]
  puts `./octopus --cat 00000000000000000500.xlog | sed 's/tm:[^ ]* //'` + "\n"
  puts `./octopus --cat 00000000000000001002.xlog | sed 's/tm:[^ ]* //'` + "\n"
  puts `./octopus --cat 00000000000000001001.snap | sed 's/tm:[^ ]* //' | egrep 't:snap_(initial|final)_tag'`
end

env.connect_eval do
  select 1, 2
end
