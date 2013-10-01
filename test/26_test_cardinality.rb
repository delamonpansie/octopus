#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].cardinality = 2
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.clean.with_server do
  insert ['a', 'b']
  log_try { insert ['a'] }
  log_try { insert ['a', 'b', 'c'] }

  select 'a'
  log_try { update_fields 'a', [1, :delete] }
  update_fields 'a', [1, :delete], [1, :insert, 'c']
  select 'a'
end
