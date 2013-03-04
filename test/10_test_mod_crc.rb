#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
rows_per_wal=32
run_crc_delay=0.1
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

env = Env.clean

def wait_for(n=100)
  n.times do
    return if yield
    sleep 0.05
  end
  raise "wait_for failed"
end

env.with_server do
  16.times {|i| insert [i,i]}
  Process.kill('USR1', env.pid)
  32.times {|i| insert [i, i + 1]}
  wait_for { lx = open('|./octopus --cat 00000000000000000032.xlog').lines.grep(/run_crc/).length > 0 }
end

env.with_server do
  puts `./octopus --cat 00000000000000000032.xlog | grep run_crc | sed 's/ tm:[^ ]*//'`
  puts File.open('octopus.log').grep(/mismatch/)

  16.times {|i| insert [i,i]}
  Process.kill('USR1', env.pid)
  wait_for { File.exist? '00000000000000000066.snap' }
end

env.with_server do
  32.times {|i| insert [i, i + 1]}
  wait_for { open('|./octopus --cat 00000000000000000096.xlog').lines.grep(/run_crc/).length > 0 }
end

env.with_server do
  puts `./octopus --cat 00000000000000000096.xlog | grep run_crc | sed 's/ tm:[^ ]*//'`
  puts File.open('octopus.log').grep(/mismatch/)
  puts `grep -P '>E|mismatch' octopus.log*`
end
