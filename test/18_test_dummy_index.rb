#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
rows_per_wal=100

object_space[0].index[0].key_field[0].type = "NUM"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 0
object_space[0].index[1].key_field[0].fieldno = 0
object_space[0].index[1].key_field[0].type = "NUM"

EOD
  end
end

env = Env.new

env.start
env.connect_eval do
  ping
  insert [1,2]
  insert [2,3]

  Process.kill('USR1', env.pid)
end

env.restart
env.connect_eval do
  ping
  insert [1,2]
  insert [2,3]
end

env.restart
env.connect_eval do
  ping
  insert [1,2]
  insert [2,3]
end


