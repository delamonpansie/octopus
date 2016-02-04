#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'


$one.create_shard 1, :POR, "one"

# change shard type
log_try { $one.create_shard 1, :PAXOS, "one" }

