require 'tinyrake'
require 'silverbox-log'

$options ||= {}

def log(*args)
  print *args
end

def log_try(&block)
  yield
rescue => e
  err = "Failed with: #{e}\n" #FIXME: e.inspect
  err += e.backtrace.reject{|l| l =~ /runtest\.rb/}.join("\n") if $options[:backtrace]
  log err
end

class RunEnv < TinyRakeEmbed
  include Process
  include FileTest
  include FileUtils

  Root = FileUtils.getwd

  attr_reader :pid

  def initialize
    super
    @test_dir = "test"
  end

  task :clean
  task :start => [:setup]
  task :stop

  def test_root
    Root
  end

  def delay
    sleep($options[:valgrind] ? 4 : 0.1)
  end

  def connect
    SilverBox.new connect_string
  end

  def cd_test_root
    cd Root do
      cd test_root do
        yield
      end
    end
  end

  def self.clean(&block)
    obj = new
    obj.instance_eval do
      invoke :clean, :setup
      if block_given? then
        cd_test_root do
          begin
            instance_exec obj, &block
          ensure
            stop
          end
        end
      else
        obj
      end
    end
  end

  def with_server(&block)
    raise unless block_given?

    invoke :setup
    cd_test_root do
      begin
        start
        connect.instance_eval &block
      ensure
        stop
      end
    end
  end
end
