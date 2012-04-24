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
    super + <<EOD
primary_port = 33023
wal_feeder_addr = "127.0.0.1:33034"

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
  master.insert [1, 2, "abc", "def"]
  master.insert [1, 2, "abc", "def"]
  SlaveEnv.new.with_server do |slave|
    slave.select [1]
  end
end
