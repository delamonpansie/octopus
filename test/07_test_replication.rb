class MasterEnv < RunEnv
  def server_root
    super + "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = "ANY:33034"

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

class SlaveEnv < RunEnv
  def server_root
    super + "_slave"
  end

  def config
    @primary_port = 33023
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33034"
wal_feeder_filter = "id_log"

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"
EOD
  end
  def connect_string
    "0:33023"
  end
end

MasterEnv.new.with_server do |master|
  master.ping

  100.times do |i|
    master.insert [i, i + 1, "abc", "def"]
    master.insert [i, i + 1, "abc", "def"]
  end

  sleep(0.1)
  slave_env = SlaveEnv.new
  slave_env.with_server do |slave|
    slave.select [99]

    Process.kill("STOP", slave_env.pid)
    1000.times do |i|
      master.insert [i, i + 1, "ABC", "DEF"]
      master.insert [i, i + 1, "ABC", "DEF"]
    end
    Process.kill("CONT", slave_env.pid)
    sleep(0.3)
    slave.select [999]
  end
end
