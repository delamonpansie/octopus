#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
end

Env.connect_eval do
  200.times {|i| insert [i, "x" * i] }

  lua_nolog('user_proc.get_all_tuples', '0', '0').map{|t| t[1]}.sort.
    each{|f| puts(f)}
end
