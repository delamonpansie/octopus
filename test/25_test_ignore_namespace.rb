#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[1].enabled = 1
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.connect_eval do |server|

  insert ["foo"]
  insert ["foo"], :object_space => 1

  Process.kill('USR1', server.pid)

  insert ["bar"]
  insert ["bar"], :object_space => 1

  select "foo", "bar"
  select "foo", "bar", :object_space => 1

  server.stop
  File.open(Env::ConfigFile, "a") { |io| io.puts("object_space[1].ignored = 1") }
  server.start

  wait_for "reconnect" do reconnect end

  insert ["baz"]
  log_try { insert ["baz"], :object_space => 1 }
  puts

  select "foo", "bar", "bar"
  select "foo", "bar", "baz", :object_space => 1
end
