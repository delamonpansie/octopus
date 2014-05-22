#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
end

Env.env_eval do
  start

  c1 = connect
  c2 = connect
  c4 = connect

  wal_writer_pid = pid + 4 # hack!
  c1.insert [3, "baz"]
  puts "# wal_writer stop"
  Process.kill "STOP", wal_writer_pid
  t1 = Thread.new { log_try { c1.insert [1, "foo"] } }
  t2 = Thread.new { sleep 0.01; log_try { c2.insert [1, "foobar"] } }

  sleep 0.05
  c4.select 1

  puts "# wal_writer cont"
  Process.kill "CONT", wal_writer_pid
  t1.join
  t2.join
  c4.select 1
  c4.delete 1
  c4.select 1


  c1.insert [3, "bar"]
  puts "# wal_writer stop"
  Process.kill "STOP", wal_writer_pid
  t2 = Thread.new { log_try { c2.update_fields 3, [0, :set, 1] } }
  t1 = Thread.new { sleep 0.01; log_try { c1.insert [1, "foo"] } }
  sleep 0.05
  c4.select 1, 3
  Process.kill "CONT", wal_writer_pid
  puts "# wal_writer cont"
  t1.join
  t2.join
  c4.select 1
end
