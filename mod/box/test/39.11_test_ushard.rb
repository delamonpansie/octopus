#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one_env.meta 'shard 1 create por'
$one_env.meta 'shard 1 obj_space 0 create tree unique string 0'


$one.insert [0,"one"], :shard => 1
$one.select [0], :shard => 1

t = Thread.new {
  connect = $one
  connect.connect_name = "thread"
  connect.lua 'user_proc.test10', :shard => 1
  sleep 1
  log_try { connect.select [0], :shard => 1 }
}

$one_env.meta 'shard 1 delete'
$one_env.meta 'shard 1 create por'
$one_env.meta 'shard 1 obj_space 0 create tree unique string 0'

$one = $one_env.connect
$one.select [0], :shard => 1
$one.insert [0,"one2"], :shard => 1
$one.select [0], :shard => 1

t.join
