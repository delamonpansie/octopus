#!/usr/bin/ruby
# encoding: ASCII

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

EOD
  end
end

Env.env_eval do |env|
  env.start
  t = Thread.new do
    env.connect.insert ["2", "sleep"]
  end

  sleep 0.2
  conn = env.connect
  log_try { conn.lua "user_proc.test11" }
  t.join
  conn.select "\0\0\0\0", "2"
end
