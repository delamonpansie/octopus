#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "TREE"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

object_space[1].enabled = 1
object_space[1].index[0].type = "TREE"
object_space[1].index[0].unique = 1
object_space[1].index[0].key_field[0].fieldno = 0
object_space[1].index[0].key_field[0].type = "NUM"

object_space[2].enabled = 1
object_space[2].index[0].type = "TREE"
object_space[2].index[0].unique = 1
object_space[2].index[0].key_field[0].fieldno = 0
object_space[2].index[0].key_field[0].type = "NUM64"

object_space[3].enabled = 1
object_space[3].index[0].type = "TREE"
object_space[3].index[0].unique = 1
object_space[3].index[0].key_field[0].fieldno = 0
object_space[3].index[0].key_field[0].type = "STR"
object_space[3].index[0].key_field[1].fieldno = 1
object_space[3].index[0].key_field[1].type = "NUM"

object_space[4].enabled = 1
object_space[4].index[0].type = "TREE"
object_space[4].index[0].unique = 1
object_space[4].index[0].key_field[0].fieldno = 0
object_space[4].index[0].key_field[0].type = "STR"
object_space[4].index[0].key_field[0].sort_order = "DESC"

object_space[5].enabled = 1
object_space[5].index[0].type = "TREE"
object_space[5].index[0].unique = 1
object_space[5].index[0].key_field[0].fieldno = 0
object_space[5].index[0].key_field[0].type = "NUM"
object_space[5].index[0].key_field[0].sort_order = "DESC"

object_space[6].enabled = 1
object_space[6].index[0].type = "TREE"
object_space[6].index[0].unique = 1
object_space[6].index[0].key_field[0].fieldno = 0
object_space[6].index[0].key_field[0].type = "NUM64"
object_space[6].index[0].key_field[0].sort_order = "DESC"

object_space[7].enabled = 1
object_space[7].index[0].type = "TREE"
object_space[7].index[0].unique = 1
object_space[7].index[0].key_field[0].fieldno = 0
object_space[7].index[0].key_field[0].type = "STR"
object_space[7].index[0].key_field[0].sort_order = "DESC"
object_space[7].index[0].key_field[1].fieldno = 1
object_space[7].index[0].key_field[1].type = "NUM"
object_space[7].index[0].key_field[1].sort_order = "DESC"

object_space[8].enabled = 1
object_space[8].index[0].type = "TREE"
object_space[8].index[0].unique = 1
object_space[8].index[0].key_field[0].fieldno = 0
object_space[8].index[0].key_field[0].type = "STR"
object_space[8].index[0].key_field[0].sort_order = "DESC"
object_space[8].index[0].key_field[1].fieldno = 1
object_space[8].index[0].key_field[1].type = "NUM"

object_space[9] = { enabled = 1
                    index[0] = { type = "HASH"
                                 unique = 1
                                 key_field[0] = { fieldno = 0
                                                  type = "STR" }
                               }
                    index[1] = { type = "TREE"
                                 unique = 0
                                 key_field[0] = { fieldno = 1
                                                  type = "STR" }
				 key_field[1] = { fieldno = 2
                                                  type = "STR" }
                               }
                    index[2] = { type = "TREE"
                                 unique = 0
                                 key_field[0] = { fieldno = 1
                                                  type = "STR" }
				 key_field[1] = { fieldno = 2
                                                  type = "STR"
						  sort_order = "DESC" }
                               }
                  }

EOD
  end
end

Env.connect_eval do
  [0, 4].each do |o|
    self.object_space = o
    [["a"], ["b"], ["c"], ["d"]].each &method(:insert)
  end

  [1, 5].each do |o|
    self.object_space = o
    [[1], [2], [3], [4]].each &method(:insert)
  end

  [2, 6].each do |o|
    self.object_space = o
    [["\0\0\0\0\0\0\0\1"], ["\0\0\0\0\0\0\0\2"],
     ["\0\0\0\0\0\0\0\3"], ["\0\0\0\0\0\0\0\4"]].each &method(:insert)
  end

  [3, 7].each do |o|
    self.object_space = o
    [["a", 1], ["b", 2], ["c", 3], ["d", 4]].each &method(:insert)
  end

  [0,4, 1,5, 2,6, 3,7].each do |o|
    lua 'user_proc.get_all_tuples', "#{o}"
  end

  self.object_space = 9
  i = 'a'
  %w/a b/.each do |j|
    %w/a b c d/.each do |k|
      insert_nolog [i, j, k]
      i = i.succ
    end
  end
  select "a", "b", :index => 1
  select "a", "b", :index => 2
end
