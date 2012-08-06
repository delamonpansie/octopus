#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
run_crc_delay = 0.4

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.clean.with_server do
  ping
  129.times do |i|
    insert [i, i + 1, i + 2]
  end
  sleep 1.1

  puts `./octopus --cat 00000000000000000002.xlog | sed 's/tm:[^ ]* //' | grep run_crc` + "\n"
end
