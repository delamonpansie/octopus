class UpdateEnv < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
end

env = UpdateEnv.new
env.with_server do |c1|
  c2 = env.connect_to_box
  c4 = env.connect_to_box

  wal_writer_pid = env.pid + 2 # hack!
  c1.insert [3, "baz"]
  puts "# wal_writer stop"
  Process.kill "STOP", wal_writer_pid
  t1 = Thread.new { LogPpProxy.try { c1.insert [1, "foo"] } }
  t2 = Thread.new { sleep 0.01; LogPpProxy.try { c2.insert [1, "foobar"] } }
  sleep 0.05
  c4.select 1
  Process.kill "CONT", wal_writer_pid
  puts "# wal_writer cont"
  t1.join
  t2.join
  c4.select 1
  c4.delete 1
  c4.select 1


  c1.insert [3, "bar"]
  puts "# wal_writer stop"
  Process.kill "STOP", wal_writer_pid
  t2 = Thread.new { LogPpProxy.try { c2.update_fields 3, [0, :set, 1] } }
  t1 = Thread.new { sleep 0.01; LogPpProxy.try { c1.insert [1, "foo"] } }
  sleep 0.05
  c4.select 1, 3
  Process.kill "CONT", wal_writer_pid
  puts "# wal_writer cont"
  t1.join
  t2.join
  c4.select 1

end
