#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
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
    wait_for "server failure" do
      File.read(Env::LogFile).match(/exception: object_space 0 is not configured/)
    end
    puts File.read(Env::LogFile).match(/F> exception: object_space 0 is not configured/)
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
  puts "reloading config"
  File.open(Env::ConfigFile, "w") { |io| io.puts(env.config :object_space => false) }
  s = TCPSocket.new 'localhost', 33015
  s.puts "reload conf"
  s.gets
  puts s.gets
  puts
  s.close

  create_index 1, :type => :FASTTREE, :unique => 1, :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC }

  env.snapshot
  insert ["baf"]
  env.stop
end

env.connect_eval do
  select "foo", "bar", "baz", "baf"
  env.stop
end

env.cd do
  puts `./octopus --cat 00000000000000000005.snap | sed 's/tm:[0-9.]\\+ //'`
end
