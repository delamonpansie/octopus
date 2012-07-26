#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
rows_per_wal=100

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

env = Env.clean
env.with_server do
  ping
  insert [1,2,3]
  Process.kill('USR1', env.pid)
  20.times do
    break if FileTest.readable?("00000000000000000002.snap")
    env.delay
  end
  raise "no snapshot" unless FileTest.readable?("00000000000000000002.snap")
end


evn = Env.clean
env.with_server do
  ping
  1000.times {|i| insert [i, i + 1, i + 2]}
end

env.with_server do
  select [1, 500, 505, 999, 1001]

  Process.kill('USR1', env.pid)
  insert [1]
  insert [2]
  sleep 0.5

  puts Dir.glob("*.snap").sort + ["\n"]

  puts Dir.glob("*.xlog").sort + ["\n"]

  puts File.open("00000000000000001001.snap").lines.take(4) + ["\n"]

  puts File.open("00000000000000001002.xlog").lines.take(4) + ["\n"]

  puts `./tarantool --cat 00000000000000000500.xlog | sed 's/tm:[^ ]* //'` + "\n"

  puts `./tarantool --cat 00000000000000001002.xlog | sed 's/tm:[^ ]* //'` + "\n"

  puts `./tarantool --cat 00000000000000001001.snap | sed 's/tm:[^ ]* //' | egrep 't:snap_(initial|final)_tag'`
end
