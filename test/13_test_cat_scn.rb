#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  16.times {|i| insert [i,i]}

  open('|./octopus --cat 10 2>/dev/null').lines do |l|
    puts l.gsub(/tm:\d+\.\d+ /, '')
  end
end
