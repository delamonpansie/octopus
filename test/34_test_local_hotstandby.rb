#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
local_hot_standby = 1
EOD
  end
end

Env.connect_eval do
  insert ['0', 'a', 'b', 'c', 'd']
  select ['0']
end

