class UpdateEnv < RunEnv
  def config
    super + <<EOD
wal_dir = "/"

object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

UpdateEnv.new.with_server do |box|
  box.ping
  LogPpProxy.try { box.insert [1, 2, "abc", "def"] }
  LogPpProxy.try { box.insert [1, 2, "abc", "def"] }
end
