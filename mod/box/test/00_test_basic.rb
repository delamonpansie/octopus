#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
rows_per_wal=100
EOD
  end
end

Env.connect_eval do |env|
  ping
  insert [1,2,3]

  env.snapshot

  wait_for "readable 00000000000000000002.snap" do
    FileTest.readable?("00000000000000000002.snap")
  end
end

def header(file_name)
  lines = File.open(file_name).each_line.take_while {|l| l != "\n" }
  lines.map {|l| l.sub /^Octopus-version: .*/, "Octopus-version: REDACTED" }
end

env = Env.new
env.connect_eval do
  ping
  1000.times {|i| insert [i, i + 1, i + 2]}
  env.stop
end

env.connect_eval do
  select 1, 500, 505, 999, 1001

  env.snapshot
  insert [1]
  insert [2]
  env.stop

  wait_for "readable 00000000000000001001.snap" do
    FileTest.readable? '00000000000000001001.snap'
  end

  puts Dir.glob("*.snap").sort + ["\n"]
  puts Dir.glob("*.xlog").sort + ["\n"]
  puts header("00000000000000001001.snap") + ["\n"]
  puts header("00000000000000001002.xlog") + ["\n"]
  puts `./octopus --cat 00000000000000000500.xlog | sed 's/tm:[^ ]* //'` + "\n"
  puts `./octopus --cat 00000000000000001002.xlog | sed 's/tm:[^ ]* //'` + "\n"
  puts `./octopus --cat 00000000000000001001.snap | sed 's/tm:[^ ]* //' | egrep 't:snap_(initial|final)_tag'`
  puts `grep 'E>' octopus.log`
end

env.connect_eval do
  select 1, 2
end
