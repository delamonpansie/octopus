#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super :hostname => "one", :object_space => false
  end

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

Env.connect_eval do
  create_shard 0, :POR
  create_object_space 0, :shard => 0, :index => {:type => :FASTTREE, :unique => 1, :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC }}
  keys = []
  5.times  do |i|
    foo = "foo#{i}"
    insert [foo]
    keys << foo
  end
  select *keys
  3.times do truncate end
  select *keys
end
