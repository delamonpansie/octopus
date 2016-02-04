#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one.create_shard 1, :POR, "one"
$one.create_object_space 0, :shard => 1, :index => DEFIDX

10.times {|i|
  $one.insert_nolog [i, "one#{i}"], :shard => 1
}

$one_env.env_eval do
  snapshot
  wait_for "readable 00000000000000000013.snap" do
    FileTest.readable? '00000000000000000013.snap'
  end
  rm '00000000000000000001.snap'
  restart
  puts "restart\n"
  $one.reconnect
  $one.ping
end

# join two into shard 1
$two.create_shard 1, :POR, "one", "two"
sleep 0.1
$one.insert [42, "One"], :shard => 1

$two_env.env_eval do
  stop
  puts `./octopus --cat 00000000000000000001.snap`.gsub(/ tm:\d+(\.\d+)? /, ' ')
  puts `./octopus --cat 00000000000000000002.xlog`.gsub(/ tm:\d+(\.\d+)? /, ' ')
end
