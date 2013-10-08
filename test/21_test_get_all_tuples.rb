#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0] = { type = "HASH"
			     unique = 1
			     key_field[0] = { fieldno = 0
                                              type = "NUM" } }
EOD
  end
end

Env.clean.with_server do
  200.times {|i| insert [i, "x" * i] }

  lua('user_proc.get_all_tuples', '0', '0').each do |t| puts t[1] end
end
