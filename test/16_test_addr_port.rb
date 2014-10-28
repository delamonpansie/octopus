#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super.gsub(/.*_port.*/, '') + "\n" + $x
  end
end


$x = "primary_port = 33013"
Env.connect_eval do ping end

$x = "primary_addr = 33013"
Env.new.env_eval do
  invoke :setup
  puts `./octopus 2>&1`.match(/Option 'primary_port' is not set/)[0]
  puts
end

$x = 'primary_addr = "127.0.0.1"'
Env.new.env_eval do
  invoke :setup
  puts `./octopus 2>&1`.match(/Option 'primary_port' is not set/)
  puts
end

$x = "primary_addr = \"127.0.0.1:33013\"\nprimary_port = 33013"
Env.connect_eval do ping end

$x = "primary_addr = \"127.0.0.1\"\nprimary_port = 33013"
Env.connect_eval do ping end

$x = 'primary_addr = "127.0.0.1:33013"'
Env.connect_eval do ping end

$x = "primary_addr = \"127.0.0.1:13\"\nprimary_port = 33013"
Env.connect_eval do
  ping
  puts File.new('octopus.log').read.match(/Option 'primary_addr' is overridden by 'primary_port'/)
end
