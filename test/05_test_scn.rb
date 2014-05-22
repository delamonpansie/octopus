#!/usr/bin/ruby1.9.1

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < StrHashEnv
  def config
    super + <<EOD
rows_per_wal=10
EOD
  end
end

Env.connect_eval do
  ping
  30.times do insert [1] end

  puts File.open("00000000000000000010.xlog").lines.take_while {|l| l != "\n" } + ["\n"]
  puts `./octopus --cat 00000000000000000010.xlog | sed 's/tm:[^ ]* //'`
  puts
  puts File.open("00000000000000000020.xlog").lines.take_while {|l| l != "\n" } + ["\n"]
  puts `./octopus --cat 00000000000000000020.xlog | sed 's/tm:[^ ]* //'`
end
