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

object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "STR"
EOD

  end
end

Env.clean do
  start
  server = connect

  server.insert ["foo"]
  server.insert ["foo"], :object_space => 1

  Process.kill('USR1', pid)

  server.insert ["bar"]
  server.insert ["bar"], :object_space => 1

  server.select "foo", "bar"
  server.select "foo", "bar", :object_space => 1

  stop
  File.open(Env::ConfigFile, "a") { |io| io.puts("object_space[1].ignored = 1") }
  start

  server = connect
  server.insert ["baz"]
  log_try { server.insert ["baz"], :object_space => 1 }
  puts

  server.select "foo", "bar", "bar"
  server.select "foo", "bar", "baz", :object_space => 1

  stop
end
