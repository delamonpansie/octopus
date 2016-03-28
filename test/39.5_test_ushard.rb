#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'


$one_env.meta 'shard 1 alter por one'

# change shard type
log_try { $one_env.meta 'shard 1 alter paxos one' }

