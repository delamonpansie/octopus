#!/usr/bin/ruby
# encoding: ASCII

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 0
object_space[0].index[1].key_field[0].fieldno = 0
object_space[0].index[1].key_field[0].type = "STR"
object_space[0].index[1].key_field[1].fieldno = 1
object_space[0].index[1].key_field[1].type = "NUM"
object_space[0].index[1].key_field[2].fieldno = 2
object_space[0].index[1].key_field[2].type = "STR"


object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "NUM32"
object_space[1].index[1].type = "HASH"
object_space[1].index[1].unique = 1
object_space[1].index[1].key_field[0].fieldno = 1
object_space[1].index[1].key_field[0].type = "NUM32"
object_space[1].index[2].type = "HASH"
object_space[1].index[2].unique = 1
object_space[1].index[2].key_field[0].fieldno = 2
object_space[1].index[2].key_field[0].type = "NUM64"
object_space[1].index[3].type = "TREE"
object_space[1].index[3].unique = 0
object_space[1].index[3].key_field[0].fieldno = 0
object_space[1].index[3].key_field[0].type = "NUM32"


object_space[2].enabled = 1
object_space[2].index[0].type = "TREE"
object_space[2].index[0].unique = 1
object_space[2].index[0].key_field[0].fieldno = 0
object_space[2].index[0].key_field[0].type = "NUM32"
object_space[2].index[0].key_field[1].fieldno = 1
object_space[2].index[0].key_field[1].type = "NUM32"


EOD
  end
end

Env.connect_eval do
  100.times {|i| insert [i.to_s, i, i.to_s] }
  insert ["\0\0\0\0", "\0\0\0\0", "\0\0\0\0\0\0\0\0"], :object_space => 1
  insert ["\0\0\0\0", "\0\0\0\0", "----"], :object_space => 2

  (1..9).each do |i|
    lua "user_proc.test#{i}"
  end
end
