class Test3Env < RunEnv
  def config
    connect_string, config = super
    config += <<EOD
object_space[0].enabled = 1

object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 0
object_space[0].index[1].key_field[0].fieldno = 1
object_space[0].index[1].key_field[0].type = "STR"

EOD
    return connect_string, config
  end
end

Test3Env.new.with_server do |box|
  3.times {|i| box.insert [i.to_s, 'x'] }

  box.select %w{0 1 2}
  box.select 'x', :index => 1
end
