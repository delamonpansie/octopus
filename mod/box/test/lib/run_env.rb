require 'erb'
require 'pathname'

require 'tinyrake'
require 'silverbox-log'

class Pathname
  def <<(arg)
    Pathname.new(self.to_s + arg)
  end
end

Root = (Pathname.new($0).dirname + '..').realpath

$options ||= {}

def log(*args)
  print *args
end

def log_try(&block)
  yield
rescue => e
  err = "Failed with: #{e}\n" #FIXME: e.inspect
  err.gsub!('../','')
  err += e.backtrace.reject{|l| l =~ /runtest\.rb/}.join("\n") if $options[:backtrace]
  log err # + "\n"
end

$timefactor = 5
def wait_for(reason = "deadline", deadline=3)
  deadline *= $timefactor
  deadline = Time.now + deadline
  while deadline > Time.now do
    begin
      result = yield
      return result if result
    rescue
    end
    sleep 0.05
  end
  raise reason if reason
end

class RunEnv < TinyRakeEmbed
  include Process
  include FileTest
  include FileUtils

  attr_reader :pid

  ConfigFile = "octopus.cfg"
  PidFile = "octopus.pid"
  LogFile = "octopus.log"
  Binary = Root + "../../octopus"

  def initialize
    @primary_port ||= 33013
    @secondary_port ||= 33014
    @admin_port ||= 33015
    if @port_offset then
      @primary_port += @port_offset
      @secondary_port += @port_offset
      @admin_port += @port_offset
    end
    @test_root_suffix ||= ''
    rm_rf test_root
    mkdir_p test_root
    @test_root ||= test_root.realpath

    at_exit do self.stop end
    super
  end

  def connect_string
    "0:#{@primary_port}"
  end

  def config(object_space: true, hostname: nil)
    cfg = ""
    cfg << <<EOD
pid_file = "octopus.pid"
slab_alloc_arena = 0.1
log_level = 7
primary_port = #@primary_port
admin_port = #@admin_port
wal_fsync_delay = 0.1
rows_per_wal = 5000
coredump = 0.00017
seed = "0b"
EOD

    if @secondary_port then
      cfg << "secondary_port = #@secondary_port\n"
    end

    if not $options[:valgrind] then
      cfg << %Q{logger = "exec cat - >> octopus.log"\n}
    end

    cfg << <<EOD  if object_space
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"

#######

EOD
    cfg << <<EOD if hostname
sync_scn_with_lsn = 0
hostname = "#{hostname}"
peer = [ { name = "one"
           addr = "127.0.0.1:33013" },
         { name  = "two"
           addr = "127.0.0.1:33023" },
         { name = "three"
           addr = "127.0.0.1:33033" } ]
EOD
    cfg
  end

  def test_root
    Pathname.new("test/var#@test_root_suffix")
  end

  def cd(&block)
    FileUtils.cd @test_root, &block
  end

  file ConfigFile do
    File.open(ConfigFile, "w+") do |io|
      io.write config
    end
  end

  task :setup => ConfigFile do
    ln_s Binary, "octopus"
    ln_s Root + "../../.gdbinit", ".gdbinit"
    ln_s Root + "../../.gdb_history", ".gdb_history"

    if not @skip_init and not readable? "00000000000000000001.snap"
      waitpid(octopus ['--init-storage'], :out => "/dev/null", :err => "/dev/null")
    end

    if readable? LogFile
      mv LogFile, LogFile + ".init"
    end
  end

  def gdb_if_core
    return unless $stdin.tty? && $stdout.tty?

    if FileTest.readable? "core"
      STDERR.puts "\nLast 20 lines from log\n-------\n"
      system *%w[tail -n20 octopus.log]
      STDERR.puts "\n-------\n"
      STDERR.puts "\n\nCore found. starting gdb."
      # spawn gdb in separate process: cleanup callbacks will be called at script exit
      # e.g. kill master after slave coredump
      system *%w[gdb --quiet octopus core]
    end
  end

  def octopus(args, param = {})
    pid = fork
    if pid
      return pid
    else
      Process.setpgid($$, 0)
      ENV["OCTOPUS_TEST"] = "true"
      argv = ['./octopus', '-c', ConfigFile, *args]
      if $options[:valgrind]
        $timefactor = 40
        argv.unshift('valgrind', '-q',
                     "--trace-children=yes",
                     "--suppressions=#{Root + '../../scripts/valgrind.supp'}",
                     "--suppressions=#{Root + '../../third_party/luajit/src/lj.supp'}")
      end
      exec *argv, param
    end
  end

  def rotate_logs
    i = 0
    while true do
      if readable? "#{LogFile}.#{i}" then
        i += 1
        next
      end
      mv LogFile, "#{LogFile}.#{i}" if readable? LogFile
      break
    end
  end

  def start
    cd do
      invoke :start
      rotate_logs

      pid = octopus []

      begin
        wait_for "server start" do
          File.read(LogFile).match(/entering event loop/)
        end
        @pid = pid
      rescue
        gdb_if_core
        raise
      end
    end
  end
  alias :start_server :start

  def stop
    return unless @pid

    if not waitpid @pid, WNOHANG then
      kill("INT", @pid)

      cd do
        wait_for nil do
          not readable? PidFile
        end

        if readable? PidFile
          STDERR.puts "killing hang server pid #@pid"
          kill("KILL", @pid)
        end
      end

      waitpid @pid
    else
      STDERR.puts "server prematurely exit"
    end

    @pid = nil
    cd do
      gdb_if_core
    end
    if $?.signaled? and $?.termsig != Signal.list['INT'] then
      raise "#{File.basename Binary} exited on uncaught signal #{$?.termsig}"
    end

  end
  alias :stop_server :stop

  def restart
    stop
    start
  end

  def snapshot
    raise "no server running" unless @pid
    sleep 0.1
    Process.kill('USR1', @pid)
  end

  task :start => [:setup]
  task :stop

  def connect(*args)
    wait_for "connect to server" do
      SilverBox.new connect_string, *args
    end
  end

  def env_eval(&block)
    cd do
      return self.instance_eval &block
    end
  end

  def self.env_eval(&block)
    raise unless block_given?
    env = new
    env.instance_eval do
      cd do
        begin
          return instance_exec env, &block
        ensure
          stop
        end
    end
    end
  end

  def connect_eval(&block)
    raise unless block_given?
    start unless @pid
    cd do
      return connect.instance_eval &block
    end
  end

  def self.connect_eval(&block)
    raise unless block_given?
    env = new
    env.instance_eval do
      begin
        start
        cd do
          return connect.instance_exec env, &block
        end
      ensure
        stop
      end
    end
  end
end


class StrHashEnv < RunEnv
  def config
    super + <<EOD
EOD
  end
end
