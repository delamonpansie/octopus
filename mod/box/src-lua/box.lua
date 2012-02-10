
local error, print, type, pairs, ipairs, table =
      error, print, type, pairs, ipairs, table

local string, tostring =
      string, tostring

local tou32, tofield = string.tou32, string.tofield
local netmsg = netmsg

module(...)

user_proc = {}

-- make useful aliases
space = object_space
for k,v in pairs(object_space) do
        object_space[tostring(k)] = v
end

function select(n, ...)
        local index = object_space[n].index[0]
        local result = {}
        for k, v in pairs({...}) do
                result[k] = index[v]
        end
        return result
end

function replace(n, ...)
        local tuple = {...}
        local flags = 0
        local req = {}

        table.insert(req, tou32(n))
        table.insert(req, tou32(flags))
        table.insert(req, tou32(#tuple))
        for k, v in pairs(tuple) do
                table.insert(req, tofield(v))
        end
        dispatch(13, table.concat(req))
end

function delete(n, key)
        local key_len = 1
        local req = {}

        table.insert(req, tou32(n))
        table.insert(req, tou32(key_len))
        table.insert(req, tofield(key))
        dispatch(20, table.concat(req))
end

function wrap(proc_body)
        if type(proc_body) == "string" then
                proc_body = loadstring(code)
        end
        if type(proc_body) ~= "function" then
                return nil
        end

        local function proc(out, object_space, ...)
                local retcode, result = proc_body(object_space, ...)

                if type(result) == "table" then
                        netmsg.add_iov(out, tou32(#result))

                        for k, v in pairs(result) do
                                netmsg.add_iov(out, v)
                        end
                elseif type(result) == "number" then
                        netmsg.add_iov(out, tou32(result))
                else
                        error("unexpected type of result:" .. type(result))
                end

                return retcode
        end

        return proc
end

function tuple(...)
        local f, bsize = {...}, 0
        for k, v in ipairs(f) do
                f[k] = string.tofield(v)
                bsize = bsize + #f[k]
        end
        table.insert(f, 1, string.tou32(#f))
        table.insert(f, 1, string.tou32(bsize))
        return table.concat(f)
end
