#!/usr/bin/ruby
# encoding: ASCII

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

object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "NUM"

object_space[2].enabled = 1
object_space[2].index[0].type = "HASH"
object_space[2].index[0].unique = 1
object_space[2].index[0].key_field[0].fieldno = 0
object_space[2].index[0].key_field[0].type = "NUM"
object_space[2].index[1].type = "TREE"
object_space[2].index[1].unique = 0
object_space[2].index[1].key_field[0].fieldno = 1
object_space[2].index[1].key_field[0].type = "STR"
object_space[2].index[1].key_field[0].sort_order = "DESC"
EOD
  end
end

Env.connect_eval do
  ping
  [0,1,2].each do |o|
    self.object_space = o
    1.upto(100) do |i|
      insert [i, (i%2).to_s]
    end
    if o == 0
      lua "user_proc.start_expire", o.to_s
    else
      lua "user_proc.start_expire", o.to_s, (['','0','1'][o])
    end
    sleep 0.22
    1.upto(100) do |i|
      select i
    end
  end
end
