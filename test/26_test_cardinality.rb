#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].cardinality = 2
EOD
  end
end

Env.connect_eval do
  insert ['a', 'b']
  log_try { insert ['a'] }
  log_try { insert ['a', 'b', 'c'] }

  select 'a'
  log_try { update_fields 'a', [1, :delete] }
  update_fields 'a', [1, :delete], [1, :insert, 'c']
  select 'a'
end
