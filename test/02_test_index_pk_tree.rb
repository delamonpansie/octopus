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

Env.clean.with_server do
  ping
  insert ['1', '2', 3]
  select '1', :index => 0
  select '2', :index => 1
  select 2, :index => 1

  delete '1'
  select '1'

  self.object_space = 1

  log_try { insert [0] }
  insert ["00000000", "00000000", "1"]
  insert ["00000001", "00000000", "2"]
  select "00000000", :index => 1
end

Env.clean.with_server do
  ping

  log_try { insert [] }
  log_try { insert [1] }
  log_try { insert [1,2] }
end


Env.clean.with_server do |box|
  100.times {|i| insert [i.to_s, i.to_s, i] }

  pks
  select 1
end
