#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super.gsub(/.*_port.*/, '') + "\n" + $x + <<EOD

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end


$x = "primary_port = 33013"
Env.clean.with_server do ping end

$x = "primary_addr = 33013"
Env.clean do
  invoke :setup
  puts `./octopus 2>&1`.match(/Option 'primary_port' is not set/)[0]
  puts
end

$x = 'primary_addr = "127.0.0.1"'
Env.clean do
  invoke :setup
  puts `./octopus 2>&1`.match(/Option 'primary_port' is not set/)
  puts
end

$x = "primary_addr = \"127.0.0.1:33013\"\nprimary_port = 33013"
Env.clean.with_server do ping end

$x = "primary_addr = \"127.0.0.1\"\nprimary_port = 33013"
Env.clean.with_server do ping end

$x = 'primary_addr = "127.0.0.1:33013"'
Env.clean.with_server do ping end

$x = "primary_addr = \"127.0.0.1:13\"\nprimary_port = 33013"
Env.clean.with_server do
  ping
  puts File.new('octopus.log').read.match(/Option 'primary_addr' is overridden by 'primary_port'/)
end
