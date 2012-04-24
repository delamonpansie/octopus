class IndexEnv < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 1
object_space[0].index[1].key_field[0].fieldno = 1
object_space[0].index[1].key_field[0].type = "STR"

object_space[0].index[2].type = "TREE"
object_space[0].index[2].unique = 0
object_space[0].index[2].key_field[0].fieldno = 1
object_space[0].index[2].key_field[0].type = "STR"
object_space[0].index[2].key_field[0].fieldno = 2
object_space[0].index[2].key_field[0].type = "NUM"

object_space[1].enabled = 1
object_space[1].index[0].type = "TREE"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "NUM64"

object_space[1].index[1].type = "TREE"
object_space[1].index[1].unique = 0
object_space[1].index[1].key_field[0].fieldno = 1
object_space[1].index[1].key_field[0].type = "NUM64"
object_space[1].index[1].key_field[1].fieldno = 2
object_space[1].index[1].key_field[1].type = "STR"

EOD
  end
end

index_env = IndexEnv.new

index_env.with_server do |box|
  box.ping
  box.insert ['1', '2', 3]
  box.select '1', :index => 0
  box.select '2', :index => 1
  box.select 2, :index => 1

  box.delete '1'
  box.select '1'

  box.object_space = 1

  LogPpProxy.try { box.insert [0] }
  box.insert ["00000000", "00000000", "1"]
  box.insert ["00000001", "00000000", "2"]
  box.select "00000000", :index => 1
end

index_env.with_server do |box|
  box.ping

  LogPpProxy.try { box.insert [] }
  LogPpProxy.try { box.insert [1] }
  LogPpProxy.try { box.insert [1,2] }
end


index_env.with_server do |box|
  100.times {|i| box.insert [i.to_s, i.to_s, i] }

  box.pks
  box.select 1
end
