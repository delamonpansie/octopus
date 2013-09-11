#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
rows_per_wal=10

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
  30.times do insert [1] end

  puts File.open("00000000000000000010.xlog").lines.take_while {|l| l != "\n" } + ["\n"]
  puts `octopus --cat 00000000000000000010.xlog | sed 's/tm:[^ ]* //'`
  puts
  puts File.open("00000000000000000020.xlog").lines.take_while {|l| l != "\n" } + ["\n"]
  puts `octopus --cat 00000000000000000020.xlog | sed 's/tm:[^ ]* //'`
end
