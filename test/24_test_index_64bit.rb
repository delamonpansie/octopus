#!/usr/bin/ruby
# encoding: ASCII

$: << File.dirname($0) + '/lib'
require 'run_env'
require 'facets'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].index[0].key_field[0].type = "NUM64"
EOD
  end

  task :setup => "box_init.lua"
  file "box_init.lua" do
    File.create "box_init.lua", <<-EOD
        local box = require'box'
        user_proc.select = box.wrap(function (n)
                return 0, {box.object_space[n].index[0][0ULL],
			   box.object_space[n].index[0][0xffffffffffffffffULL]}
        end)
      EOD
  end
end

Env.connect_eval do
  insert ["\x00\x00\x00\x00\x00\x00\x00\x00", "foo"]
  insert ["\xff\xff\xff\xff\xff\xff\xff\xff", "bar"]
  lua 'user_proc.select', '0'
end
