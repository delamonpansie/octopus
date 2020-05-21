#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class RunEnvX < RunEnv
  task :setup => "box_init.lua"
  file "box_init.lua" do
    File.open("box_init.lua", "w") do |io|
      io.write <<-EOD
        local box = require 'box'
        user_proc.select = box.wrap(function (shard, n)
	  local object_space = shard:object_space(n)
	  local pk = object_space:index(0)
	  return 0, {pk:find('0')}
        end)

        user_proc.update = box.wrap(function (shard, n, i)
          i = tonumber(i)
	  local object_space = shard:object_space(n)
	  local pk = object_space:index(0)
	  object_space:update('0', { i, 'delete' } )
	  return 0, {pk:find('0')}
        end)

	user_proc.error = box.wrap(function (shard, n)
          error('fooo')
        end)

        print('box_init.lua loadded')
      EOD
    end
  end

end

class MasterEnv < RunEnvX
  def test_root
    super << "_master"
  end

  def config
    super + <<EOD
wal_feeder_bind_addr = ":33034"
EOD
  end
end

class SlaveEnv < RunEnvX
  def initialize
    @primary_port = 33023
    @test_root_suffix = "_slave"
    super
  end

  def config
    super + <<EOD
wal_feeder_addr = "127.0.0.1:33034"
EOD
  end
end

master = MasterEnv.new.connect_eval do
  self.connect_name = "master"
  ping
  self
end

SlaveEnv.connect_eval do
  self.connect_name = "slave"
  master.insert ['0', 'a', 'b', 'c', 'd']
  wait_for { select_nolog(['0']).length > 0 }

  master.select ['0']
  select ['0']

  master.lua 'user_proc.select', '0'
  lua 'user_proc.select', '0'

  master.lua 'user_proc.update', '0', '2'
  log_try { lua 'user_proc.update', '0', '3' }

  log_try { lua 'user_proc.error', '0' }
end

