#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1

object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 0
object_space[0].index[1].key_field[0].fieldno = 1
object_space[0].index[1].key_field[0].type = "STR"

object_space[0].index[2].type = "TREE"
object_space[0].index[2].unique = 0
object_space[0].index[2].key_field[0].fieldno = 2
object_space[0].index[2].key_field[0].type = "STR"
object_space[0].index[2].key_field[1].fieldno = 1
object_space[0].index[2].key_field[1].type = "STR"

object_space[0].index[3].type = "TREE"
object_space[0].index[3].unique = 0
object_space[0].index[3].key_field[0].fieldno = 1
object_space[0].index[3].key_field[0].type = "STR"
object_space[0].index[3].key_field[1].fieldno = 2
object_space[0].index[3].key_field[1].type = "STR"

EOD
  end
end

Env.clean.with_server do
  3.times {|i| insert [i.to_s, 'x', 'y'] }

  select %w{0 1 2}
  select 'x', :index => 1
  select ['x'], :index => 2
  select ['y'], :index => 2
  select ['x'], :index => 3
  select ['y'], :index => 3
end
