#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
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
object_space[1].index[0].type = "HASH"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "NUM64"

object_space[1].index[1].type = "TREE"
object_space[1].index[1].unique = 0
object_space[1].index[1].key_field[0].fieldno = 1
object_space[1].index[1].key_field[0].type = "NUM64"
object_space[1].index[1].key_field[1].fieldno = 2
object_space[1].index[1].key_field[1].type = "STR"

object_space[1].index[2].type = "TREE"
object_space[1].index[2].unique = 1
object_space[1].index[2].key_field[0].fieldno = 1
object_space[1].index[2].key_field[0].type = "NUM64"
object_space[1].index[2].key_field[1].fieldno = 2
object_space[1].index[2].key_field[1].type = "STR"

object_space[2].enabled = 1
object_space[2].index[0].type = "TREE"
object_space[2].index[0].unique = 1
object_space[2].index[0].key_field[0].fieldno = 0
object_space[2].index[0].key_field[0].type = "STR"

object_space[2].index[1].type = "TREE"
object_space[2].index[1].unique = 1
object_space[2].index[1].key_field[0].fieldno = 3
object_space[2].index[1].key_field[0].type = "NUM"
object_space[2].index[1].key_field[1].fieldno = 1
object_space[2].index[1].key_field[1].type = "NUM64"
object_space[2].index[1].key_field[2].fieldno = 0
object_space[2].index[1].key_field[2].type = "STR"

EOD
  end
end

Env.connect_eval do
  ping
  insert ['1', '2', 3]
  select '1', :index => 0
  select '2', :index => 1
  select 2, :index => 1

  delete '1'
  select '1'

  self.object_space = 1
  log_try { insert [0] }
  log_try { select [0] }
  insert ["00000000", "00000000", "1"]
  insert ["00000001", "00000000", "2"]
  select "00000000", :index => 1
  select "00000000", :index => 2

  self.object_space = 2
  insert ["000", "00000000", 0, 0]
  select "000"
  select 0, :index => 1
end

Env.connect_eval do
  ping

  log_try { insert [] }
  log_try { insert [1] }
  log_try { insert [1,2] }
end

Env.connect_eval do
  100.times {|i| insert [i.to_s, i.to_s, i] }

  pks
  select 1
end
