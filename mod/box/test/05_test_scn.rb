#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < StrHashEnv
  def config
    super + <<EOD
rows_per_wal=10
EOD
  end
end

def header(file_name)
  lines = File.open(file_name).each_line.take_while {|l| l != "\n" }
  lines.map {|l| l.sub /^Octopus-version: .*/, "Octopus-version: REDACTED" }
end

Env.connect_eval do
  ping
  30.times do insert [1] end

  puts header("00000000000000000010.xlog") + ["\n"]
  puts `./octopus --cat 00000000000000000010.xlog | sed 's/tm:[^ ]* //'`
  puts
  puts header("00000000000000000020.xlog") + ["\n"]
  puts `./octopus --cat 00000000000000000020.xlog | sed 's/tm:[^ ]* //'`
end
