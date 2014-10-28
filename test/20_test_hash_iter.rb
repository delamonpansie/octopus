#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  100.times {|i| insert [i.to_s] }

  lua 'user_proc.iterator', '0'

  lua 'user_proc.iterator', '0', '68', '10'
  lua 'user_proc.iterator', '0', '26', '10'
end
