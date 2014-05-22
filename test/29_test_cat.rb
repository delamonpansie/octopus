#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
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

Env.connect_eval do |env|
  insert %w[1 2 3]
  delete %w[1 2]
  select %w[1]

  insert %w[1 2 3]
  insert %w[1 2a 3]
  log_try { update_fields %w[1], [1, :set, "aa"] }
  update_fields %w[1 2], [1, :set, "aa"]
  select %w[1]

  env.stop
  puts `./octopus --cat 00000000000000000002.xlog`.gsub(/ tm:\d+\.\d+ /, ' ')
end
