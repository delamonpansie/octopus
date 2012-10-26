#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end

  def delay
    sleep 0.01
  end

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
  env.clean do
    rm '00000000000000000001.snap'
    f = File.new('00000000000000000001.snap', 'w')
    f.write(SNAP[ver])
    f.close

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
