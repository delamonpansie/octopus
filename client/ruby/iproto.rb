#
# Copyright (C) 2009, 2010, 2013 Mail.RU
# Copyright (C) 2009, 2010, 2013 Yuriy Vostrikov
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#

require 'socket'

IPROTO_PING = 0xff00

include Socket::Constants

class IProtoError < RuntimeError
end

if not String.instance_methods.member?("bytesize") then
  class String
    def bytesize
      self.size
    end
  end
end

class IProto
  @@sync = 0

  def initialize(server, param = {})
    if server.is_a? Numeric  then
      host, port = 0, server
    else
      host, port = server.split(/:/)
    end
    @end_point = [host, port.to_i]

    if param[:logger] == :log4r then
      require 'log4r'
      logger = Log4r::Logger.new 'iproto'
      logger.level = Log4r::DEBUG
      logger.outputters = Log4r::Outputter.stderr
      param[:logger] = logger
    elsif param[:logger] and param[:logger] != :stderr then
      raise "Unknown logger: #{param[:logger]}"
    end

    param[:reconnect] = true unless param.has_key?(:reconnect)

    [:logger, :reconnect].each do |p|
      instance_variable_set "@#{p}", param[p] if param.has_key? p
    end

    reconnect
  end

  attr_reader :sock

  def debug
    if @logger == :stderr then
      STDERR.puts yield
    elsif @logger then
      @logger.debug yield
    end
  end

  def hexdump(string)
    string.unpack('C*').map{ |c| "%02x" % c }.join(' ')
  end

  def next_sync
    @@sync += 1
    if @@sync > 0xffffffff
      @@sync = 0
    end
    @@sync
  end

  def reconnect
    @sock = TCPSocket.new(*@end_point)
    @sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)
  end

  def close
    @sock.close unless @sock.closed
  end

  def send(message)
    begin
      reconnect if @sock.closed? and @reconnect

      sync = self.next_sync
      payload = message[:raw] || message[:data].pack(message[:pack] || 'L*')

      buf = [message[:code], payload.bytesize, sync].pack('L3')
      debug { "#{@end_point} => send hdr #{buf.unpack('L*').map{ |c| "%010i" % c }.join(' ')}" }

      buf << payload
      debug { "#{@end_point} => send bdy #{hexdump(payload)}" }

      @sock.write(buf)

      header = @sock.read(12)
      raise IProtoError, "can't read header" unless header
      header = header.unpack('L3')
      debug { "#{@end_point} => recv hdr #{header.map{ |c| "%010i" % c }.join(' ')}" }

      raise IProtoError, "response.sync:#{header[2]} != message.sync:#{sync}" if header[2] != sync
      raise IProtoError, "response.code:#{header[0]} != message.code:#{message[:code]}" if header[0] != message[:code]

      data = @sock.read(header[1])
      debug { "#{@end_point} => recv bdy #{hexdump(data)}" }
      data
    rescue Exception => exc
      @sock.close
      raise exc
    end
  end

  def msg(message)
    begin
      reply = send message
      result = pre_process_reply message, reply
    rescue IProtoError => exc
      if exc.to_s =~ /code: 0x4102, message: '([\d.]+):(\d+)/
      then
        @end_point = [$1, $2]
        reconnect
        retry
      else
        raise
      end
    end

    return yield(result) if block_given?
    result
  end

  def ping
    send :code => IPROTO_PING, :raw => ''
    :pong
  end

  def pre_process_reply(message, data)
    if message[:unpack]
      data.unpack(message[:unpack])
    else
      data
    end
  end
end

class IProtoRetCode < IProto
  def pre_process_reply(message, reply)
    raise IProtoError, "too small response" if reply.nil? or reply.bytesize < 4
    ret_code = reply.slice!(0, 4).unpack('L')[0]
    raise IProtoError, "{code: #{'0x%x' % ret_code}, message: '#{reply}'}" if ret_code != 0
    super message, reply
  end
end
