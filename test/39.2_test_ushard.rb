#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

# non existent shard
log_try { $one.select 1, :shard => 33 }

