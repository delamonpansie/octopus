#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
run_crc_delay = 0.1
rows_per_wal = 1000000
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.clean do
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

  stop
  start
  puts File.open("octopus.log").lines.grep(/mismatch/)
end
