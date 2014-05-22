#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
wal_writer_inbox_size=0
EOD
  end
end

Env.connect_eval do
  12.times {|i| insert [i,i]}

  puts Dir.glob("*.xlog").sort.join("\n")
end
