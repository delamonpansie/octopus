#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def initialize
    @hostname = "one"
    super
  end

  def meta(arg)
    puts "# #@hostname.meta(#{arg})"
    puts `perl ../../client/shard/shardbox.pl -s=localhost:#{33013 + (@port_offset || 0)} #{arg}`
    puts
  end
end

env = Env.new

env.connect_eval do
  insert ["foo"]
  env.snapshot
  insert ["bar"]

  env.stop
end

env.connect_eval do
  select "foo", "bar"
  env.stop
end

env.cd do
  File.open(Env::ConfigFile, "w") { |io| io.puts(env.config :object_space => false) }
end

env.env_eval do
  cd do
    invoke :start
    env.octopus [], :out => "/dev/null", :err => "/dev/null"
    err = ""
    wait_for "server failure" do
      err = File.read(Env::LogFile).match(/exception: (shard|object_space) 0 is not configured/)
    end
    puts err
    puts
    rm Env::PidFile
  end
end

env.cd do
  File.open(Env::ConfigFile, "w") { |io| io.puts(env.config) }
end

env.connect_eval do
  select "foo", "bar"
  insert ["baz"]
  log_try { create_index 1, :type => :FASTTREE, :unique => 1, :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC } }
  env.stop
end

env.cd do
  File.open(Env::ConfigFile, "w") { |io| io.puts(env.config :hostname => "one") }
end

env.connect_eval do
  env.meta 'shard 0 undummy'
  sleep 0.2
  # create_object_space 0, :shard => 0, :index => {:type => :FASTTREE, :unique => 1, :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC }}
  env.meta 'shard 0 obj_space 0 index 1 create fasttree unique  string 0 desc'

  env.meta 'shard 1 create por'
  env.meta 'shard 1 obj_space 1 create fasttree unique string 0 desc'

  env.snapshot
  insert ["baf"]
  env.stop
end

env.connect_eval do
  select "foo", "bar", "baz", "baf"
  env.stop
end

env.cd do
  puts `./octopus --cat 00000000000000000007.snap | sed 's/tm:[0-9.]\\+ //g;'`
end
