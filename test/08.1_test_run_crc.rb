#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
run_crc_delay = 0.1
rows_per_wal = 1000000
EOD
  end
end

Env.env_eval do
  start

  t = []
  256.times do |x|
    t << Thread.new do
      s = connect
      129.times do |i|
        s.insert_nolog [x * 256 + i]
      end
    end
  end
  t.pop.join while t.length > 0

  restart
  puts File.open("octopus.log").each_line.grep(/mismatch/)
end
