#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
rows_per_wal=100

object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
object_space[0].index[0].key_field[1].fieldno = 1
object_space[0].index[0].key_field[1].type = "STR"
EOD
  end
end

env = Env.clean
env.with_server do
  ping
  insert ["x16", "y16", 1]
  insert ["x22", "y22", 2]
  select ["x22"], ["x16"]
end
