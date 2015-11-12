local ffi = require 'ffi'
local assertarg = assertarg
local varint32 = varint32
local charbuf = ffi.typeof('char *')

local function decode_varint32(ptr, offt)
    local value, len = varint32.read(ffi.cast(charbuf, ptr) + offt)
    return value, offt + len
end

local decode = {}
function decode.varint32(obj, offt) return obj:datacast('varint32', offt) end
function decode.string(obj, offt, len) return obj:datacast('string', offt, len) end
function decode.u8(obj, offt) return obj:datacast('uint8_t', offt) end
function decode.u16(obj, offt) return obj:datacast('uint16_t', offt) end
function decode.u32(obj, offt) return obj:datacast('uint32_t', offt) end

local bytes_to_int = {}
local cast = bytes_to_int -- legacy compat of cast.u32()
local strlen = string.len
for _, v in ipairs({8, 16, 32}) do
    local t = 'uint' .. v .. '_t'
    local pt = 'const ' .. t .. ' *'

    local function f(str)
        assertarg(str, 'string', 1)
        assert(strlen(str) == ffi.sizeof(t),
               "Invalid bytes length: !!want " .. ffi.sizeof(t) .. " but given " .. strlen(str))
        return ffi.cast(pt, str)[0]
    end
    bytes_to_int[v] = f
    bytes_to_int['u' .. v] = f
end

local bytes_to_arr = {}
for _, v in ipairs({8, 16, 32}) do
    local t = 'uint' .. v .. '_t'
    local pt = 'const ' .. t .. ' *'

    local function f(str)
        assertarg(str, 'string', 1)
        assert(strlen(str) % ffi.sizeof(t) == 0,
               "Invalid bytes length: " .. strlen(str) .. " isn't multiple of " .. ffi.sizeof(t))

        local ptr = ffi.cast(pt, str)
        local r = {}
        for i = 1, strlen(str) / ffi.sizeof(t) do
            r[i] = tonumber(ptr[i - 1])
        end
        return r
    end
    bytes_to_arr[v] = f
    bytes_to_arr['u' .. v] = f
end

return { decode_varint32 = decode_varint32,
         decode = decode,
         bytes_to_int = bytes_to_int,
         cast = bytes_to_int,
         bytes_to_arr = bytes_to_arr }
