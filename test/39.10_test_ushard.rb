#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one.create_shard 1, :POR, "one", "three"
$one.create_shard 2, :POR, "one", "two"
$two.create_shard 3, :POR, "two", "three"

$one.create_object_space 0, :shard => 1, :index => DEFIDX
$one.insert [1,"one"], :shard => 1

$one.create_object_space 0, :shard => 2, :index => DEFIDX
$one.insert [1,"one"], :shard => 2

$one.create_object_space 0, :shard => 3, :index => DEFIDX
$one.insert [1,"one"], :shard => 3

$three = ThreeEnv.new.connect_eval do
  ping
  self
end

sleep 0.1

$three.select 1, :shard => 1
$three.select 1, :shard => 3
