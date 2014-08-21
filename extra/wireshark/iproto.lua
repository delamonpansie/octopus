local dport, sport = Field.new('tcp.dstport'), Field.new('tcp.srcport')

local iproto = Proto("iproto","IProto")
local iproto_msg = ProtoField.uint32("iproto.msg", "Message")
local iproto_len = ProtoField.uint32("iproto.len", "Length")
local iproto_sync = ProtoField.uint32("iproto.sync", "Sync")
local iproto_data = ProtoField.bytes("iproto.data", "Data")
local iproto_errcode = ProtoField.uint32("iproto.err_code", "ErrCode", base.HEX)
local iproto_errmsg = ProtoField.string("iproto.err_msg", "ErrMessage")
iproto.fields = {iproto_msg, iproto_len, iproto_sync, iproto_data,
                 iproto_errcode, iproto_errmsg}

local iproto_port
local iproto_data_dissector
function iproto.dissector(buf, pinfo, tree)
    if buf:len() < 12 then
        pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
        return
    end

    local offt = 0
    while true do
        local header = buf:range(offt, 12)
        local msg = header:range(0, 4):le_uint()
        local data_len = header:range(4, 4):le_uint()
        local packet_len = 12 + data_len
        if packet_len > 4 * 1024 * 1024 then
            -- header looks like a garbage
            return
        end
        if buf:len() < packet_len then
            pinfo.desegment_len = packet_len - buf:len()
            pinfo.desegment_offset = offt
            return
        end
        local packet = buf:range(offt, packet_len) -- header & packet overlaps

        pinfo.cols.protocol = iproto.name

        local typ
        if msg == 0xff00 then
            typ = "IProto ping"
        elseif iproto_port[tostring(sport())] then
            typ = "IProto reply"
        elseif iproto_port[tostring(dport())] then
            typ = "IProto request"
        else
            typ = "IProto packet"
        end
        local subtree_packet = tree:add(iproto, packet, typ)
        local subtree_header = subtree_packet:add(header, "Header")

        subtree_header:add_le(iproto_msg,  header:range(0, 4))
        subtree_header:add_le(iproto_len,  header:range(4, 4))
        subtree_header:add_le(iproto_sync, header:range(8, 4))

        if data_len > 0 then
            local subtree_data = subtree_packet:add(iproto_data, packet:range(12, data_len))

            if iproto_data_dissector then
                local command = packet:range(12, data_len)
                iproto_data_dissector(msg, command, subtree_data)
            end
        end

        if buf:len() == offt + packet_len then
            return
        end
        offt = offt + packet_len
    end
end


local box = Proto("silverbox","Silverbox")
local box_objspace = ProtoField.uint32("silverbox.obj_space", "ObjectSpace")
local box_index = ProtoField.uint32("silverbox.index", "Index")
local box_offset = ProtoField.int32("silverbox.offset", "Offset")
local box_limit = ProtoField.int32("silverbox.limit", "Limit")
local box_flags = ProtoField.uint32("silverbox.flags", "Flags")
local box_count = ProtoField.uint32("silverbox.count", "Count")
local box_tuple = ProtoField.bytes("silverbox.tuple", "Tuple", base.HEX)
local box_tuple_cardinality = ProtoField.int32("silverbox.tuple.cardinality", "Cardinality")
local box_tuple_blen = ProtoField.int32("silverbox.tuple.blen", "Len")
local box_tuple_field = ProtoField.bytes("silverbox.tuple.field", "Field")
local box_tuple_field_len = ProtoField.int32("silverbox.tuple.field.len", "Len")
local box_tuple_field_data = ProtoField.string("silverbox.tuple.field.data", "Data")
local box_tuple_field_datau32 = ProtoField.int32("silverbox.tuple.field.datau32", "Data")

box.fields = {box_objspace, box_index, box_offset, box_limit, box_flags, box_count,
              box_tuple, box_tuple_cardinality, box_tuple_blen,
              box_tuple_field, box_tuple_field_len,
              box_tuple_field_data, box_tuple_field_datau32}

local function box_insert(buf, tree)
    tree:add_le(box_objspace, buf:range(0, 4))
end

local function varint(buf, offt)
    local r = 0
    for i = 0, 4 do
        local b = buf:range(offt + i, 1):uint()
        r = bit.bor(bit.lshift(r, 7), bit.band(b, 0x7f))
        if b < 0x80 then
            return i + 1, r
        end
    end
end

local function field_dissect(buf, offt, vlen, blen, tree)
        local field = tree:add(box_tuple_field, buf:range(offt, vlen + blen))
        field:add(box_tuple_field_len, buf:range(offt, vlen), blen)
        if blen == 4 then
            field:add(box_tuple_field_datau32, buf:range(offt + vlen, blen))
        elseif blen > 0 then
            field:add(box_tuple_field_data, buf:range(offt + vlen, blen))
        end
end

local function tuple_dissect(buf, tree, hasblen)
    local offt, blen, cardinality
    if hasblen then
        blen = buf:range(0, 4)
        cardinality = buf:range(4, 4)
        offt = 8
    else
        cardinality = buf:range(0, 4)
        offt = 4
    end
    local fields = {}

    for i = 1, cardinality:le_uint() do
        local vlen, blen = varint(buf, offt)
        table.insert(fields, {offt, vlen, blen})
        offt = offt + vlen + blen
    end

    tree = tree:add(box_tuple, buf:range(0, offt), "Tuple")
    if blen then
        tree:add_le(box_tuple_blen, blen)
    end
    tree:add_le(box_tuple_cardinality, cardinality)
    for _, v in ipairs(fields) do
        local offt, vlen, blen = unpack(v)
        field_dissect(buf, offt, vlen, blen, tree)
    end

    if buf:len() == offt then
        return offt
    else
        return offt, buf:range(offt)
    end
end


local function box_select(buf, tree)
    tree = tree:add(buf, "BOX_SELECT")

    tree:add_le(box_objspace, buf:range(0, 4))
    tree:add_le(box_index, buf:range(4, 4))
    tree:add_le(box_offset, buf:range(8, 4))
    tree:add_le(box_limit, buf:range(12, 4))
    local key_count = buf:range(16, 4)
    local key_tree = tree:add(key_count, "Keys: " .. key_count:le_uint())
    buf = buf:range(20)
    for i = 1, key_count:le_uint() do
        _, buf = tuple_dissect(buf, key_tree)
    end
end

local function box_insert(buf, tree)
    tree = tree:add(buf, "BOX_INSERT")
    tree:add_le(box_objspace, buf:range(0, 4))
    tree:add_le(box_flags, buf:range(4, 4))
    tuple_dissect(buf:range(8), tree)
end

local function box_delete(buf, tree)
    tree = tree:add(buf, "BOX_DELETE")
    tree:add_le(box_objspace, buf:range(0, 4))
    tuple_dissect(buf:range(4), tree)
end

local op_decode = {[0] = "set", [1] = "add", [2] = "and",
                   [3] = "or", [4] = "xor", [5] = "splice",
                   [6] = "delete", [7] = "insert"}

local function box_update_fields(buf, tree)
    tree = tree:add(buf, "BOX_UPDATE_FIELDS")
    tree:add_le(box_objspace, buf:range(0, 4))
    tree:add_le(box_flags, buf:range(4, 4))
    _, buf = tuple_dissect(buf:range(8), tree:add("Key"))

    local op_count = buf:range(0, 4)
    tree = tree:add(op_count, "Ops: " .. op_count:le_uint())
    buf = buf:range(4)
    for i = op_count:le_uint(),1,-1  do
        local op_tree = tree:add("Op")
        op_tree:add_le(buf:range(0, 4), "Idx: " .. buf:range(0, 4):le_uint())
        local op_txt = op_decode[buf:range(4, 1):le_uint()]
        op_tree:add_le(buf:range(4, 1), "Op:" .. op_txt)
        local vlen, blen = 0, 0
        vlen, blen = varint(buf, 5)
        field_dissect(buf, 5, vlen, blen, op_tree)
        if i > 1 then
            buf = buf:range(5 + vlen + blen)
        end
    end
end

local box_op = {
    [13] = box_insert,
    [17] = box_select,
    [19] = box_update_fields,
    [20] = box_delete
}

local function box_reply(buf, tree)
    tree:add_le(iproto_errcode, buf:range(0, 4))
    local ret_code = buf:range(0, 4):le_uint()
    if ret_code > 0 then
        tree:add(iproto_errmsg, buf:range(4))
    else
        tree:add_le(box_count, buf:range(4, 4))
        local count = buf:range(4, 4):le_uint()
        if buf:len() == 8 then
            return
        else
            buf = buf:range(8)
        end
        for i = 1, count do
            _, buf = tuple_dissect(buf, tree, true)
            if not buf then
                return
            end
        end
    end
end

local function box_dissector(cmd, buf, tree)
    if iproto_port[tostring(dport())] then
        if box_op[cmd] then
            box_op[cmd](buf, tree)
        end
    elseif iproto_port[tostring(sport())] then
        box_reply(buf, tree)
    else
        tree:add("unknow port")
    end
end

iproto_data_dissector = box_dissector
iproto_port = { ["33013"] = 1, ["3313"] = 1}

local tcp_table = DissectorTable.get("tcp.port")
for k, _ in pairs(iproto_port) do
    tcp_table:add(tonumber(k), iproto)
end
