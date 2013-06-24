iproto = Proto("iproto","IProto")

ipro_msg = ProtoField.string("iproto.msg","Message")
ipro_len = ProtoField.string("iproto.len","Length")
ipro_sync = ProtoField.string("iproto.sync","Sync")

iproto.fields = {ipro_msg, ipro_len, ipro_sync}

function iproto.dissector(buffer, pinfo, tree)
  local buflen = buffer:len()
  local bufoff = 0

  local len = 0

  while buflen > 12 do
    local msg = buffer(bufoff, 4):le_uint()
    len = buffer(bufoff + 4, 4):le_uint()
    local sync = buffer(bufoff + 8, 4):le_uint()

    if buflen < 12 + len then
      pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
      return bufoff
    end

    pinfo.cols.protocol = iproto.name

    local subtree_packet = tree:add(iproto, buffer(bufoff, 12 + len), "IProto packet (" ..12 + len.. ")")
    local subtree_header = subtree_packet:add_le(buffer(bufoff, 12), "IProto header (12)")

    subtree_header:add(ipro_msg,  msg)
    subtree_header:add(ipro_len,  len)
    subtree_header:add(ipro_sync, sync)

    if len > 0 then
      local subtree_data = subtree_packet:add_le(buffer(bufoff + 12, len), "IProto data (" ..len.. ")")
    end

    buflen = buflen - (12 + len)
    bufoff = bufoff + (12 + len)
  end

  if buflen ~= 0 then
    pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
  end

  return bufoff
end

tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(1665, iproto)
tcp_table:add(33013, iproto)
