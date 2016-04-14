#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one_env.meta 'shard 1 create por'
$one_env.meta 'shard 1 add_replica three'

$one_env.meta 'shard 2 create por'
$one_env.meta 'shard 2 add_replica two'

$two_env.meta 'shard 3 create por'
$two_env.meta 'shard 3 add_replica three'

$one.create_object_space 0, :shard => 1, :index => DEFIDX
$one.insert [1,"one"], :shard => 1

$one.create_object_space 0, :shard => 2, :index => DEFIDX
$one.insert [1,"one"], :shard => 2

$one.create_object_space 0, :shard => 3, :index => DEFIDX
$one.insert [1,"one"], :shard => 3

$three = ThreeEnv.new.connect_eval do
  self.connect_name = "three"
  ping
  self
end

sleep 0.1

$three.select 1, :shard => 1
$three.select 1, :shard => 3
