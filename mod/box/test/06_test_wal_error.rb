#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
wal_dir = "/"
EOD
  end
end

Env.connect_eval do
  ping
  log_try { insert [1, 2, "abc", "def"] }
  log_try { insert [1, 2, "abc", "def"] }
end
