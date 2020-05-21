#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  100.times {|i| insert [i.to_s] }

  all = lua_nolog 'user_proc.iterator', '0'
  p all.sort
  p all.size

  f68 = lua_nolog 'user_proc.iterator', '0', '68', '10'
  p f68[0]
  f26 = lua_nolog 'user_proc.iterator', '0', '26', '10'
  p f26[0]
end
