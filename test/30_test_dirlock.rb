#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class MasterEnv < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end


MasterEnv.clean do
  start
  master = connect
  master.ping

  File.open("octopus.cfg", "a+") do |fd|
    fd.puts('pid_file = "pid2"')
    fd.puts("primary_port = 33023")
    fd.puts('logger = "exec cat - >> octopus.log2"')
  end

  octopus [], :out => "/dev/null", :err => "/dev/null"
  sleep 0.1
  puts File.read("octopus.log2").match(/Can't lock wal_dir/)
end
