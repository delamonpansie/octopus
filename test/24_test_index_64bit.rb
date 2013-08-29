#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "NUM64"
EOD

  end

  task :setup => [:foo]

  task :foo do
    cd_test_root do
      f = open("box_init.lua", "w")
      f.write <<-EOD
      local box = require'box'
      user_proc.select = box.wrap(function (n)
              return 0, {box.object_space[n].index[0][0ULL],
			 box.object_space[n].index[0][0xffffffffffffffffULL]}
      end)
    EOD
      f.close
    end
  end
end

Env.clean.with_server do
  insert ["\x00\x00\x00\x00\x00\x00\x00\x00", "foo"]
  insert ["\xff\xff\xff\xff\xff\xff\xff\xff", "bar"]

  lua 'user_proc.select', '0'
end
