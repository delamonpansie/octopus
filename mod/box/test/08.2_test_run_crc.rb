#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class Env < RunEnv
  def config
    super + <<EOD
rows_per_wal = 10
EOD
  end
end

Env.env_eval do |env|
  start
  c = connect
  13.times do
    c.insert [1]
  end

  puts `OCTOPUS_CAT_RUN_CRC=1 ./octopus --cat 00000000000000000010.xlog 2>/dev/null | sed 's/tm:[^ ]* //'` + "\n"
  FileUtils.rm "00000000000000000010.xlog"

  restart
  c = connect
  5.times do
    c.insert [1]
  end
  puts `OCTOPUS_CAT_RUN_CRC=1 ./octopus --cat 00000000000000000010.xlog 2>/dev/null | sed 's/tm:[^ ]* //'` + "\n"
end
