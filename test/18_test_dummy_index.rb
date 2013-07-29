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
object_space[0].index[0].key_field[0].type = "NUM"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 0
object_space[0].index[1].key_field[0].fieldno = 0
object_space[0].index[1].key_field[0].type = "NUM"

EOD
  end
end

env = Env.clean
env.with_server do
  ping
  insert [1,2]
  insert [2,3]

  Process.kill('USR1', env.pid)
end

env.with_server do
  ping
  insert [1,2]
  insert [2,3]
end

env.with_server do
  ping
  insert [1,2]
  insert [2,3]
end


