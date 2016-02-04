#!/usr/bin/ruby
# coding: utf-8

$: << File.dirname($0) + '/lib'
require 'run_env'

class ShardEnv < RunEnv
  def initialize
    @test_root_suffix = "_#@hostname"
    @secondary_port = nil
    super
  end

  def config
    super(:hostname => @hostname, :object_space => false) + <<EOD
run_crc_delay=-1
nop_hb_delay=-1
EOD
  end

end

class OneEnv < ShardEnv
  def initialize
    @hostname = "one"
    super
  end
end

class TwoEnv < ShardEnv
  def initialize
    @port_offset = 10
    @hostname = "two"
    super
  end
end

class ThreeEnv < ShardEnv
  def initialize
    @port_offset = 20
    @hostname = "three"
    super
  end
end

$one_env = OneEnv.new
$one = $one_env.connect_eval do
  self.connect_name = "one"
  ping
  self
end

$two_env = TwoEnv.new
$two = $two_env.connect_eval do
  self.connect_name = "two"
  ping
  self
end

# $three = ThreeEnv.new.connect_eval do
#   ping
#   self
# end


DEFIDX = {:type => :FASTTREE, :unique => 1,
          :field_0 => { :type => :STRING, :index => 0 , :sort_order => :DESC }}


# HOF нужен для тестирования переключения
