#!/usr/bin/ruby1.9.1

$:.push 'test'
require 'replication'

class MasterEnvCompat < MasterEnv
  def config
    super + <<EOD
io_compat = 1
EOD
  end
end

class SlaveEnvCompat < SlaveEnv
  def config
    super + <<EOD
io_compat = 1
EOD
  end
end

test MasterEnvCompat, SlaveEnvCompat

