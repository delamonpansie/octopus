
local error, print, type, pairs, ipairs, setfenv, getmetatable, table =
      error, print, type, pairs, ipairs, setfenv, getmetatable, table

local netmsg, string = netmsg, string

module(...)

user_proc = {}
REPLACE = 13
UPDATE_FIELDS = 19
DELETE = 20


-- TODO: select from multi column indexes
function select(namespace, ...)
        local index = namespace.index[0]
        local result = {}
        for k, v in pairs({...}) do
                result[k] = index[v]
        end
        return result
end

function replace(namespace, ...)
        error("unimplemented")
        local tuple = {...}
        local flags = 0
        local txn = txn.alloc()
        local req = tbuf.alloc()
        tbuf.append(req, "uuu", namespace.n, flags, #tuple)
        for k, v in pairs(tuple) do
                tbuf.append(req, "f", v)
        end
        dispatch(txn, REPLACE, req)
end

function delete(namespace, key)
        error("unimplemented");
        local txn = txn.alloc()
        local req = tbuf.alloc()
        local key_len = 1
        tbuf.append(req, "uuf", namespace.n, key_len, key)
        dispatch(txn, DELETE, req)
end


function defproc(name, proc_body, env)
        if type(proc_body) == "string" then
                proc_body = loadstring(code)
        end
        if type(proc_body) ~= "function" then
                return nil
        end

        local function proc(out, namespace, ...)
                local retcode, result = proc_body(out, namespace, ...)

                if type(result) == "table" then
                        netmsg.add_iov(out, string.tou32(#result))

                        for k, v in pairs(result) do
                                netmsg.add_iov(out, v)
                        end
                elseif type(result) == "number" then
                        netmsg.add_iov(out, string.tou32(result))
                else
                        error("unexpected type of result:" .. type(result))
                end

                return retcode
        end

        setfenv(proc, env or {})
        user_proc[name] = proc
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

defproc('xxx', 
        function (out, n)
                local x = {}
                for i = 1, 10000 do
                        local s = "aaaaaaa"..i
                        table.insert(x, tuple(s))
                end
                return 0, x
        end)