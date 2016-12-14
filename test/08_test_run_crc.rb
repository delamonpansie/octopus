#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do |env|
  ping
  33.times do |i|
    insert [i, i + 1, i + 2]
  end

  env.stop

  puts `OCTOPUS_CAT_RUN_CRC=1 ./octopus --cat 00000000000000000002.xlog 2>/dev/null | sed 's/tm:[^ ]* //'` + "\n"
end
