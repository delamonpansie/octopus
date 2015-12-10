#!/usr/bin/ruby

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
  12.times {|i| select i}
  puts Dir.glob("*.xlog").sort.join("\n")
end
