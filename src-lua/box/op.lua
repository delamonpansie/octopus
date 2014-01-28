local packer = packer
local tonumber, ipairs, select = tonumber, ipairs, select

module(...)

pack = {}

function pack.add(n, ...)
    local flags = 3 -- return tuple + add tuple flags
    local req = packer()

    req:u32(n)
    req:u32(flags)
    req:u32(select('#', ...))
    for i = 1, select('#', ...) do
        req:field(select(i, ...))
    end
    return 13, req:pack()
end

function pack.replace(n, ...)
        local flags = 1 -- return tuple
        local req = packer()

        req:u32(n)
        req:u32(flags)
        req:u32(select('#', ...))
        for i = 1, select('#', ...) do
            req:field(select(i, ...))
        end
        return 13, req:pack()
end

function pack.delete(n, key)
        local key_len = 1
        local req = packer()

        req:u32(n)
        req:u32(key_len)
        req:field(key)
        return 20, req:pack()
end

function pack.update(n, key, ...)
        local ops = {...}
        local flags, key_cardinality = 1, 1
        local req = packer()

        req:u32(tonumber(n))
        req:u32(flags)
        req:u32(key_cardinality)
        req:field(key)
        req:u32(#ops)
        for k, op in ipairs(ops) do
                req:u32(op[1])
                if (op[2] == "set") then
                        req:u8(0)
                        req:field(op[3])
                elseif (op[2] == "add") then
                        req:u8(1)
                        req:field_u32(op[3])
                elseif (op[2] == "and") then
                        req:u8(2)
                        req:field_u32(op[3])
                elseif (op[2] == "or") then
                        req:u8(3)
                        req:field_u32(op[3])
                elseif (op[2] == "xor") then
                        req:u8(4)
                        req:field_u32(op[3])
                elseif (op[2] == "splice") then
                        req:u8(5)
                        local s = packer()
                        if (op[3] ~= nil) then
                                s:field_u32(op[3])
                        else
                                s:u8(0)
                        end
                        if (op[4] ~= nil) then
                                s:field_u32(op[4])
                        else
                                s:u8(0)
                        end
			s:field(op[5])
			local buf, len = s:pack()
			req:varint(len)
			req:raw(buf, len)
                elseif (op[2] == "delete") then
                        req:string("\006\000")
                elseif (op[2] == "insert") then
                        req:u8(7)
                        req:field(op[3])
                end
        end
        return 19, req:pack()
end
