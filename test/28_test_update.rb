#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.connect_eval do
  insert [1, "\0\1", "\0\0\0\1", "\0\0\0\0\0\0\0\1"]
  update_fields 1, [1, :add, "aa"] , [2, :add, "aaaa"], [3, :add, "aaaaaaaa"]
  select 1
end
