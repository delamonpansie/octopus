#!/usr/bin/ruby
# encoding: ASCII

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM"

EOD
  end
end

env = Env.new
env.cd do
  ln_s '../mop.ml', '.'
  ln_s '../../src-ml/box1.cmi', 'box1.cmi'
  `ocamlopt.opt -O3 -g -annot -I . -I +../batteries -shared -ccopt "-Wl,-Bsymbolic -Wl,-z,now -Wl,-z,combreloc" mop.ml -o mop_1.cmxs`
end

env.connect_eval do
  5.times {|i| insert [i, i.to_s] }
  lua "user_proc.mop"
  env.stop
  puts `./octopus --cat 00000000000000000002.xlog | sed 's/tm:[^ ]* //'` + "\n"
end


env.connect_eval do
  select 1,2
end
