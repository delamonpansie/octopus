$: << File.dirname($0) + '/../client/ruby'
require 'silverbox'

class SilverBox
  LOG_OVERRIDE = %w{ping insert delete select update_fields lua pks object_space= create_index create_object_space drop_object_space drop_index truncate create_shard}

  LOG_OVERRIDE.map(&:to_sym).each do |name|
    orig_name = "#{name}_nolog".to_sym
    alias_method orig_name, name
    define_method name, ->(*args, &block) do
      log "# #{@connect_name or "box"}.#{name}(#{args.map{|arg| arg.inspect}.join(', ')})\n"
      ret_value = self.__send__(orig_name, *args, &block)
      log "#{ret_value.inspect}\n\n"
      ret_value
    end
  end

  attr_writer :connect_name
end
