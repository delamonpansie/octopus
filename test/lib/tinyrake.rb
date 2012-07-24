require 'rake'

class TinyRake
  include Rake::TaskManager
  RakeFileUtils.verbose_flag = false

  def task(*args, &block)
    define_task(Rake::Task, *args, &block)
  end

  def file(*args, &block)
    define_task(Rake::FileTask, *args, &block)
  end

  def invoke(task)
    self[task].invoke
  end

  def options
    @options = OpenStruct.new
  end
end

class TinyRakeEmbed
  @@task = []

  def initialize
    @rake = TinyRake.new
    @@task.each do |method, args, block|
      @rake.send method, *args do
        instance_eval &block if block
      end
    end
  end

  [:task, :file].each do |m|
    define_method m, ->(*args, &block){ @rake.send m, *args, &block }
    (class << self; self; end).instance_eval do
      define_method m, ->(*args, &block){ @@task.push [m, args, block] }
    end
  end

  def invoke(*tasks)
    tasks.each do |task|
      @rake.invoke task
    end
  end
  def self.task_proxy(*names)
    names.each do |name|
      define_method name { @rake.invoke name }
    end
  end
end
