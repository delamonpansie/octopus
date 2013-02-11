#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

wal_writer_inbox_size=0
EOD
  end
end

env = Env.clean

env.with_server do
  12.times {|i| insert [i,i]}

  puts Dir.glob("*.xlog").sort.join("\n")
end
