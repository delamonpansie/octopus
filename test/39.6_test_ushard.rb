#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one_env.meta 'shard 1 alter por one'
$one_env.meta 'shard 1 obj_space 0 create tree unique string 0'
$one.insert [1,"one"], :shard => 1

# join two into shard 1

$two_env.meta 'shard 1 alter por one two'
sleep 0.3

$one.insert [1, "One"], :shard => 1
$two.insert [1, "Two"], :shard => 1
$two.insert [2, "Two"], :shard => 1
wait_for { $two.select_nolog(2, :shard => 1).length > 0 }

$one.select 1, :shard => 1
$two.select 1, :shard => 1

$two_env.env_eval do
  stop
  puts `./octopus --cat 00000000000000000001.snap`.gsub(/ tm:\d+(\.\d+)? /, ' ')
  puts `./octopus --cat 00000000000000000002.xlog`.gsub(/ tm:\d+(\.\d+)? /, ' ')
end
