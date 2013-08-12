#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.clean.with_server do
  100.times {|i| insert [i.to_s] }

  lua 'user_proc.iterator', '0'

  lua 'user_proc.iterator', '0', '68', '10'
  lua 'user_proc.iterator', '0', '26', '10'
end
