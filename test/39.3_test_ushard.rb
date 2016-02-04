#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'


$one.create_shard 1, :POR, "one"
$one.create_object_space 0, :shard => 1, :index => DEFIDX
$one.insert [1,"one"], :shard => 1
$one.select 1, :shard => 1

$two.insert [1,"two"], :shard => 1
$two.select 1, :shard => 1
$one.select 1, :shard => 1

