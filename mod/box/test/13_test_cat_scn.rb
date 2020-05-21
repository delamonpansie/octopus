#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  16.times {|i| insert [i,i]}

  open('|./octopus --cat 10 2>/dev/null').each_line do |l|
    puts l.gsub(/tm:\d+\.\d+ /, '')
  end
end
