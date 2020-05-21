#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'


$one_env.meta 'shard 1 create por'

# change shard type
log_try { $one_env.meta 'shard 1 type raft' }


$one_env.meta 'shard 1 add_replica two'
$one_env.meta 'shard 1 add_replica three'

$one_env.meta 'shard 1 type raft'

sleep 5

$one_env.meta 'shard 1 obj_space 0 create tree unique string 0'
$one.insert ["aaa"], :shard => 1
