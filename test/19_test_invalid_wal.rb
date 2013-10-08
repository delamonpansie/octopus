#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0] = { type = "HASH"
    			     unique = 1
			     key_field[0] = { fieldno = 0,
					      type = "NUM" }
                           }
EOD
  end
end

env = Env.clean do
  touch '00000000000000000002.xlog.inprogress'
  start
  connect.ping
  stop
end


env = Env.clean do
  touch '00000000000000000002.xlog'
  octopus [], :out => "/dev/null", :err => "/dev/null"
  delay
  puts File.open('octopus.log').lines.grep(/fail/)[0].sub(/.*> /, '')
end


env = Env.clean do
  start
  connect.insert [1,2,3]
  stop

  x = File.open('00000000000000000002.xlog.tmp', 'w')
  File.open('00000000000000000002.xlog').lines.each do |l|
    x.write(l)
    break if l == "\n"
  end
  x.close
  mv '00000000000000000002.xlog.tmp', '00000000000000000002.xlog'

  octopus [], :out => "/dev/null", :err => "/dev/null"
  delay
  puts File.open('octopus.log').lines.grep(/no valid rows were read/)[0].sub(/.*> /, '')
end
