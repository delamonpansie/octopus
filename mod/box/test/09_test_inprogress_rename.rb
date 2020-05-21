#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do |env|
  insert [1]
  env.restart
  wait_for { reconnect }
  insert [2]
  env.stop

  puts Dir.glob("*.xlog*").sort
end
