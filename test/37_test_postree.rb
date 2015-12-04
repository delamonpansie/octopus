#!/usr/bin/ruby
# encoding: ASCII

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "POSTREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"

object_space[0].index[1].type = "POSTREE"
object_space[0].index[1].unique = 1
object_space[0].index[1].key_field[0].fieldno = 0
object_space[0].index[1].key_field[0].type = "NUM"
object_space[0].index[1].key_field[0].sort_order = "DESC"
EOD
  end
end

Env.connect_eval do
  ping
  1.upto(1000) do |i|
    insert [i]
  end

  1.step(1000, 17) do |i|
    select i, :index => 0
  end
  1.step(1000, 17) do |i|
    select i, :index => 1
  end

  1.step(1000, 13) do |i|
    lua "user_proc.position", '0', i.to_s
  end

  1.step(1000, 13) do |i|
    lua "user_proc.position", '1', i.to_s
  end
end
