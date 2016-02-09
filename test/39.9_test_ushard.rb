#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one.create_shard 1, :POR, "one"
$one.create_object_space 0, :shard => 1, :index => DEFIDX
$one.insert [1,"one"], :shard => 1
$two.create_shard 1, :POR, "one", "two"
sleep 0.1

# switch master
$two.create_shard 1, :POR, "two", "one"
sleep 0.1

$two.create_shard 1, :POR, "two", "three", "one"
sleep 0.1

$two.create_shard 1, :POR, "two", "three"
sleep 0.1

$two_env.env_eval do
  stop
  puts `./octopus --cat 00000000000000000001.snap`.gsub(/ tm:\d+(\.\d+)? /, ' ')
  puts `./octopus --cat 00000000000000000002.xlog`.gsub(/ tm:\d+(\.\d+)? /, ' ')
  puts `grep META octopus.log`.gsub(/^\d+\.\d+ \d+ /, '')
end

