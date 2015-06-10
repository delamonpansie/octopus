#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
run_crc_delay = 0.4
EOD
  end
end


Env.connect_eval do |env|
  ping
  sleep 0.5 # run_crc_delay + 0.1 tolerance
  33.times do |i|
    insert [i, i + 1, i + 2]
  end

  wait_for { open('|./octopus --cat 00000000000000000002.xlog 2>/dev/null').each_line.grep(/run_crc/).length > 0 }
  env.stop

  puts `./octopus --cat 00000000000000000002.xlog 2>/dev/null | sed 's/tm:[^ ]* //' | grep run_crc` + "\n"
end
