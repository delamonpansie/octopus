#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super :object_space => false
  end
end

Env.connect_eval do
  create_object_space 0, :index => {:type => :FASTTREE, :unique => 1, :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC }}
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
