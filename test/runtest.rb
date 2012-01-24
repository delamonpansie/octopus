%w{fileutils erb stringio optparse timeout}.each {|f| require f}
$:.push('mod/box/client/ruby')
require 'silverbox'

$options = {}
OptionParser.new do |opt|
  opt.banner = "Usage: #{$0} ..."

  opt.on('-v', '--[no-]verbose', 'Verbose output') do |v|
    $options[:verbose] = v
  end

  opt.on('-m', '--match REGEXP', 'Select tests matching REGEXP') do |pattern|
    $options[:match] = Regexp.new(pattern)
  end

  $options[:diff] = true
  opt.on('--no-diff', 'Do not show diff of failed tests') do |v|
    $options[:diff] = v
  end

  opt.on('--force-ok', 'Install ...') do |v|
    $options[:force_ok] = v
  end

  opt.on('-t', '--backtrace', 'Show backtraces') do |v|
    $options[:backtrace] = v
  end

  opt.on('--valgrind', 'Use valgrind') do |v|
    $options[:valgrind] = v
  end
end.parse!


class File
  def self.save(filename, contents)
    f = File.new(filename, 'w+')
    f.write(contents)
  ensure
    f and f.close
  end
end

class LogPpProxy
  def initialize(proxy)
    @proxy = proxy
  end

  def respond_to?(sym)
    @proxy.respond_to?(sym) or super(sym)
  end

  def method_missing(meth, *args, &block)
    puts "# box.#{meth}(#{args.map{|arg| arg.inspect}.join(', ')})"
    value = @proxy.__send__(meth, *args, &block)
    p value
    puts
    value
  end

  def self.try
    yield
  rescue => e
    err = "Failed with: #{e}\n"
    err += e.backtrace.reject{|l| l =~ /runtest\.rb/}.join("\n") if $options[:backtrace]
    puts err
  end

  def self.capture_stdout
    saved_stdout = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = saved_stdout
  end

end

class RunEnv
  include Process
  include FileTest
  include FileUtils

  ConfigFile = "tarantool.cfg"
  PidFile = "tarantool.pid"
  LogFile = "tarantool.log"
  Binary = FileUtils.getwd + "/tarantool"
  Suppressions = FileUtils.getwd + "/scripts/valgrind.supp"

  attr_reader :pid

  def initialize
    @test_dir = "test/var"
    @config_template = File.read("test/basic.cfg")
  end

  def config
    return "0:33013", ERB.new(@config_template).result(binding)
  end

  def tarantool(args, param = {})
    pid = fork
    if pid
      return pid
    else
      argv = ['./tarantool', '-c', ConfigFile, *args]
      if $options[:valgrind]
        argv.unshift 'valgrind', '-q', "--suppressions=#{Suppressions}"
      end
      exec *argv
    end
  end

  def gdb_if_core
    if FileTest.readable?("core")
      STDERR.puts "\n\nCore found. starting gdb."
      system *%w[tail -n20 tarantool.log]
      STDERR.puts "\n-------\n"
      exec *%w[gdb --quiet tarantool core]
    end
  end

  def delay
    return 4 if $options[:valgrind]
    return 0.1
  end
  def start_server
    @pid = tarantool [], :out => "/dev/null"

    50.times do
      sleep(delay)
      next unless readable? LogFile
      return if File.read(LogFile).match(/entering event loop/)
    end
    @pid = nil

    gdb_if_core
    raise "Unable to start server"
  end

  def stop_server
    return unless @pid
    if not waitpid @pid, WNOHANG then
      kill("INT", @pid)
      20.times do
        break unless readable? PidFile
        sleep(delay)
      end

      if readable? PidFile
        STDERR.puts "killing hang server"
        kill("KILL", @pid)
      end

      waitpid @pid
    else
      STDERR.puts "server prematurely exit"
    end

    @pid = nil
    if $?.signaled? and $?.termsig != Signal.list['INT'] then
      gdb_if_core
      raise "#{File.basename Binary} exited on uncaught signal #{$?.termsig}"
    end
  end

  def init_workdir
    root = getwd
    rm_rf @test_dir if exists? @test_dir
    mkdir @test_dir
    cd @test_dir do
      mkdir "scripts"
      ln_s Binary, "tarantool"
      ln_s root + "/.gdbinit", ".gdbinit"
      ln_s root + "/.gdb_history", ".gdb_history"
      @connect_string, config_data = config
      File.open(ConfigFile, "w+") do |io|
        io.write config_data
      end

      waitpid(tarantool ['--init-storage'], :out => "/dev/null", :err => "/dev/null")
    end
  end

  def connect_to_box
    LogPpProxy.new(SilverBox.new(@connect_string))
  end

  def with_server
    raise unless block_given?
    init_workdir

    cd @test_dir do
      begin
        start_server
        yield connect_to_box
      ensure
        stop_server
      end
    end
  end

  def with_env
    raise unless block_given?
    init_workdir

    cd @test_dir do
      begin
        yield self
      ensure
        stop_server
      end
    end
  end
end


tests = Dir.glob("test/*_test_*.rb")

if $options[:match]
  tests = tests.select {|filename| $options[:match].match(filename) }
  if tests.length == 0
    puts "No tests were selected"
    exit
  end
end


tests.sort.each do |filename|
  print "running #{filename.sub(%r{.*test/},'')}..."


  test_output = LogPpProxy.capture_stdout do
    require filename
  end

  filename.sub!(/\.rb$/, '')
  out = filename + '.out'
  if FileTest.readable?(out)
    if File.read(out) != test_output
      if $options[:force_ok]
        puts "ok"
      else
        puts "error"
      end
      rej = filename + '.rej'
      File.save(rej, test_output)
      puts `diff -u5 #{out} #{rej}` if $options[:diff]
      File.save(out, test_output) if $options[:force_ok]
    else
      puts "ok"
    end
  else
    puts "new"
    File.save(out, test_output)
    puts `diff -u /dev/null #{out}` if $options[:diff]
  end
end
