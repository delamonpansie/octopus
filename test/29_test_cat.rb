#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
object_space[0].index[0].key_field[1].fieldno = 1
object_space[0].index[0].key_field[1].type = "STR"
EOD
  end
end

Env.clean do
  start
  s = connect

  s.insert %w[1 2 3]
  s.delete %w[1 2]
  s.select %w[1]

  s.insert %w[1 2 3]
  s.insert %w[1 2a 3]
  log_try { s.update_fields %w[1], [1, :set, "aa"] }
  s.update_fields %w[1 2], [1, :set, "aa"]
  s.select %w[1]

  stop
  puts `./octopus --cat 00000000000000000002.xlog`.gsub(/ tm:\d+\.\d+ /, ' ')
end
