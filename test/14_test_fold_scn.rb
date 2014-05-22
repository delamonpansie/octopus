#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  12.times {|i| insert [i,i]}

  puts `./octopus --fold 2 -vALL=-2 2>/dev/null`
end
