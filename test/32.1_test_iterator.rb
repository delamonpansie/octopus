#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"
object_space[0].index[0].key_field[1].fieldno = 1
object_space[0].index[0].key_field[1].type = "NUM"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 1
object_space[0].index[1].key_field[0].fieldno = 0
object_space[0].index[1].key_field[0].type = "NUM"
object_space[0].index[1].key_field[1].fieldno = 1
object_space[0].index[1].key_field[1].type = "NUM"
object_space[0].index[1].key_field[1].sort_order = "DESC"


EOD
  end
end

Env.connect_eval do
  insert [1, 1]
  insert [1, 2]
  insert [0, 0]
  insert [2, 2]

  lua 'user_proc.iterator2', '0', '1'
  lua 'user_proc.iterator2r', '0', '1'

  lua 'user_proc.iterator2', '1', '1'
  lua 'user_proc.iterator2r', '1', '1'
end
