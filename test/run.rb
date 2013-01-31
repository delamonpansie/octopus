#!/usr/bin/ruby1.9.1

%w{fileutils erb stringio optparse timeout}.each {|f| require f}
$:.push('test/lib')
require 'options'

class File
  def self.save(filename, contents)
    f = File.new(filename, 'w+')
    f.write(contents)
  ensure
    f.close if f
  end
end


tests = Dir.glob("test/*_test_*.rb")

if $options[:match]
  tests = tests.select {|filename| $options[:match].match(filename) }
end

if tests.length == 0
  puts "No tests were selected"
  exit
end

tests.sort!

if $options[:stdout] then
  tests.each do |filename|
    print "running #{filename.sub(%r{.*test/},'')}..."
    puts "\n"
    system(filename)
  end
  exit
end

tests.each do |filename|
  print "running #{filename.sub(%r{.*test/},'')}..."

  test_output = `#{filename}`

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
