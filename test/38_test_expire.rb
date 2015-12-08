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
EOD
  end
end

Env.connect_eval do
  ping
  [0,1].each do |o|
    self.object_space = o
    1.upto(100) do |i|
      insert [i, (i%2).to_s]
    end
    lua "user_proc.start_expire", o.to_s
    sleep 0.1
    1.upto(100) do |i|
      select i
    end
  end
end
