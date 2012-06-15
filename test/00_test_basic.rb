class BasicEnv < RunEnv
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


basic_env = BasicEnv.new

basic_env.with_server do |box|
  box.ping
  box.insert [1,2,3]
  Process.kill('USR1', basic_env.pid)
  20.times do
    break if FileTest.readable?("00000000000000000002.snap")
    sleep(basic_env.delay)
  end
  raise "no snapshot" unless FileTest.readable?("00000000000000000002.snap")
end


basic_env.with_env do |env|
  env.start_server
  box = env.connect_to_box

  box.ping
  1000.times {|i| box.insert [i, i + 1, i + 2]}
  env.stop_server

  env.start_server
  box = env.connect_to_box
  box.select [1, 500, 505, 999, 1001]

  Process.kill('USR1', env.pid)
  box.insert [1]
  box.insert [2]
  sleep 0.5
  raise "no snapshot" unless FileTest.readable?("00000000000000001001.snap")
  raise "no xlog" unless FileTest.readable?("00000000000000001002.xlog")
  puts File.open("00000000000000001001.snap").lines.take(4)
  puts

  puts File.open("00000000000000001002.xlog").lines.take(4)
  puts

  puts `./tarantool --cat 00000000000000001002.xlog | sed 's/tm:[^ ]* //'`
end
