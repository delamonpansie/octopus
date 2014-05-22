#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def octopus(args, param = {})
    param[:err] = "/dev/null"
    super args, param
  end
end

class EnvIOCompat < Env
  def config
    super + <<EOD
io_compat = 1
EOD
  end
end


SNAP = {
  11 => "SNAP\n0.11\n\n\x1E\xAB\xAD\x10",
  12 => "SNAP\n0.12\n\n\x1E\xAB\xAD\x10"
}

def test(env, ver)
  env.env_eval do
    invoke :setup

    File.open('00000000000000000001.snap', 'w') do |io|
      io.write(SNAP[ver])
    end

    begin
      start
      puts "#{env.name} success loading SNAP#{ver}"
      stop
    rescue
      puts "#{env.name} failure loading SNAP#{ver}"
      stop
    end
  end
end

test Env, 11
test Env, 12
test EnvIOCompat, 11
test EnvIOCompat, 12
