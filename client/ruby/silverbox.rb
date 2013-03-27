#
# Copyright (C) 2009, 2010, 2011 Mail.RU
# Copyright (C) 2009, 2010, 2011 Yuriy Vostrikov
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

require 'iproto'

# FIXME: ugly wrapper. Is there a better way?
class Quad
  def initialize(i)
    @__value = i
  end
  def to_i
    @__value
  end
end

def q(i)
  Quad.new(i)
end

class SilverBox < IProtoRetCode
  BOX_RETURN_TUPLE = 0x01

  def initialize(host = '0:33013', param = {})
    @object_space = param[:object_space] || 0
    super(host, param)
  end

  attr_accessor :object_space

  def pack_field(value)
    case value
    when Quad
      [8, value.to_i].pack('wQ')
    when Integer
      [4, value].pack('wL')
    when String
      fail "string too long" if value.bytesize > 1024 * 1024
      [value.bytesize, value].pack('wa*')
    else
      fail "unsupported field class #{value.class}"
    end
  end

  def pack_key(key)
    if key.is_a? Array then
      result = ""
      result <<= [key.length].pack("L")
      key.each do |subkey|
        result <<= pack_field(subkey)
      end
      result
    else
      [1].pack("L") + pack_field(key)
    end
  end

  # poor man emulation of perl's pack("w/a*")
  def pack(values, pattern)
    raw = []
    pattern.split(/\s+/).each do |fmt|
      fail "not enough values given" unless values[0]

      case fmt
      when 'l', 'L', 'a*', 'C'
        raw << [values.shift].pack(fmt)
      when /^(L|w)\/$/
        raw << [values[0].length].pack($1)
      when 'field'
        raw << pack_field(values.shift)
      when 'field*'
        values.shift.each {|x| raw << pack_field(x) }
      when 'key'
        raw << pack_key(values.shift)
      when 'key*'
        values.shift.each { |x| raw << pack_key(x) }
      else
        fail "unknown pack format: '#{fmt}'"
      end
    end
    raw.join ''
  end

  def unpack_field!(data)
    # is there an efficient way to simulate perl's unpack("w/a") ?
    byte_size = data.unpack('w')[0]
    data.slice!(0 .. [byte_size].pack('w').bytesize - 1)
    if byte_size > 0
      return data.slice!(0 .. byte_size - 1)
    else
      return ''
    end
  end

  def unpack_tuple!(data)
    tuple = []
    byte_size, cardinality = data.slice!(0 .. 7).unpack("LL")
    tuple_data = data.slice!(0 .. byte_size - 1)
    cardinality.times do
      tuple << unpack_field!(tuple_data)
    end
    tuple
  end

  def unpack_reply!(reply, param)
    tuples_affected = reply.slice!(0 .. 3).unpack('L')[0]
    if param[:return_tuple]
      tuples = []
      tuples_affected.times do
        tuples << unpack_tuple!(reply)
      end
      tuples
    else
      tuples_affected
    end
  end

  private :pack_field, :pack_key, :pack
  private :unpack_field!, :unpack_tuple!, :unpack_reply!

  def insert(tuple, param = {})
    object_space = param[:object_space] || @object_space
    flags = 0
    flags |= BOX_RETURN_TUPLE if param[:return_tuple]

    tuple = [tuple] if tuple.is_a?(Integer)
    reply = msg :code => 13, :raw => pack([object_space, flags, tuple], 'L L L/ field*')
    unpack_reply!(reply, param)
  end

  def delete(key, param = {})
    object_space = param[:object_space] || @object_space

    tuples_affected, = msg(:code => 20, :raw => pack([object_space, key], 'L key')).unpack('L')
    tuples_affected
  end

  def select(*keys)
    param = keys[-1].is_a?(Hash) ? keys.pop : {}
    keys = keys[0] if keys[0].is_a? Array
    return [] if keys.length == 0
    object_space = param[:object_space] || @object_space
    offset = param[:offset] || 0
    limit = param[:limit] || 4294967295
    index = param[:index] || 0

    reply = msg :code => 17, :raw => pack([object_space, index, offset, limit, keys], 'L L L L L/ key*')
    unpack_reply!(reply, :return_tuple => true)
  end

  def update_fields(key, *ops)
    return [] if ops.length == 0
    param = ops[-1].is_a?(Hash) ? ops.pop : {}
    object_space = param[:object_space] || @object_space
    flags = 0
    flags |= BOX_RETURN_TUPLE if param[:return_tuple]
    ops.map! do |op|
      fail "op must be Array" unless op.is_a? Array
      case op[1]
      when :set
        op = [op[0], 0x00, pack_field(op[2])].pack("LCa*")
      when :add
        op = [op[0], 0x01, 4, op[2]].pack("LCwI")
      when :and
        op = [op[0], 0x02, 4, op[2]].pack("LCwL")
      when :or
        op = [op[0], 0x03, 4, op[2]].pack("LCwL")
      when :xor
        op = [op[0], 0x04, 4, op[2]].pack("LCwL")
      when :splice
        op = [op[0], 0x05, pack_field([4, op[2], 4, op[3], pack_field(op[4])].pack("wLwLa*"))].pack("LCa*")
      when :delete
        op = [op[0], 0x06, pack_field("")].pack("LCa*")
      when :insert
        op = [op[0], 0x07, pack_field(op[2])].pack("LCa*")
      else
        fail "unsupported op: '#{op[1]}'"
      end
    end

    reply = msg :code => 19, :raw => pack([object_space, flags, key, ops.length, ops.join('')], 'L L key L a*')
    unpack_reply!(reply, param)
  end

  def lua(func_name, *args)
    param = args[-1].is_a?(Hash) ? args.pop : {}

    reply = msg :code => 22, :raw => pack([0, func_name, args], 'L field L/ field*')
    unpack_reply!(reply, :return_tuple => true)
  end

  def leader
    msg :code => 90, :raw => ''
  end

  def pks(*args)
    param = args[-1].is_a?(Hash) ? args.pop : {}
    object_space = param[:object_space] || @object_space
    func_name = 'user_proc.get_all_pkeys'
    args.unshift object_space.to_s
    reply = msg :code => 22, :raw => pack([0, func_name, args], 'L field L/ field*')

    pks = []
    count = reply.slice!(0 .. 3).unpack('L')[0]
    count.times do
      pks << unpack_field!(reply)
    end
    pks
  end
end

