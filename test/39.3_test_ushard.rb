#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one_env.meta 'shard 1 alter por one'
$one_env.meta 'shard 1 obj_space 0 create tree unique string 0'

$one.insert [1,"one"], :shard => 1
$one.select 1, :shard => 1

$two.insert [1,"two"], :shard => 1
$two.select 1, :shard => 1
$one.select 1, :shard => 1

