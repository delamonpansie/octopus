$options = {}

OptionParser.new do |opt|
  opt.banner = "Usage: #{$0} ..."

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

  opt.on('--stdout', 'Print test output to stdout') do |v|
    $options[:stdout] = v
  end

end.parse!
