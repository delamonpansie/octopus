#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0)
require '39_test_ushard'

$one_env.meta 'shard 1 create por'

# create existing shard
log_try { $two_env.meta 'shard 1 create por' }


