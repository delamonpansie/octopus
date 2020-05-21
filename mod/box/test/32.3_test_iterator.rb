#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

EOD
  end
end

Env.connect_eval do
  keys = %w{00 aa abc abcd abce def}
  keys.each do |i| insert_nolog [i] end

  log_try { lua 'user_proc.iterator3' }
end
