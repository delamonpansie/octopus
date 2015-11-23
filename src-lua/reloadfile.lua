local jit = require 'jit'
local reload_files = {}
local reload_modules = {}
local reload_lock = {"it is reload loop lock"}

local function print_warn(name, msg)
    say_warn("reloadfile(\"%s\"): %s", name, msg)
end

local fix_reload_filename

local function register_reload(name)
    if reload_files[name] == nil then
        local modulename = name:gsub("^.*/", ""):gsub("%.lua$", "")
        if reload_modules[modulename] then
            print_warn(name, "could not register cause module '"..modulename.."' already registered")
            return false
        end
        if package.loaded[modulename] then
            print_warn(name, "could not register cause module '"..modulename.."' already loaded")
            return false
        end
        local stat = {
            name = name,
            modulename = modulename,
            ctm = 0,
            err_ctm = 0,
            err_tm = 0
        }
        reload_files[name] = stat
        table.insert(reload_files, name)
        reload_modules[modulename] = stat
        fix_reload_filename(name)
    end
    return reload_files[name]
end

function fix_reload_filename(name)
    local stat = reload_files[name]
    local filename = stat.name:gsub('%.lua$',''):gsub('([%w_-])%.([%w_-])', '%1/%2') .. '.lua'
    local r, v = pcall(os.ctime, filename)
    if r then
        stat.filename = filename
        return true
    end
    local err = ''
    if not filename:match('/') then
        r, err = package.searchpath(stat.modulename, package.path)
        if r then
            stat.filename = r
            return true
        end
    end
    print_warn(name, "fix_reload_filename: ".. v .. ";" .. err:gsub('\n\t', ','))
end

local function do_reload(name)
    local stat = reload_modules[name]
    local file = assert(io.open(stat.filename))
    local version
    local function loader()
        local chunk = file:read(version == nil and 256 or 4096)
        if version == nil and chunk then
            local v = chunk:match("VERSION%s*=%s*(%S*)")
            version = v or false
        end
        return chunk
    end
    local mod, err = load(loader, stat.filename)
    file:close()
    assert(mod, err)
    local res = { mod(name) }
    if version then
        say_info("reloadfile '%s': {ver=%s, file='%s', require='%s'}",
            stat.name, version, stat.filename, name)
    else
        say_info("reloadfile '%s': {file='%s', require='%s'}",
            stat.name, stat.filename, name)
    end
    return unpack(res)
end

local function reload_fullpath_loader(name)
    local stat = reload_modules[name]
    if not stat then
        return nil
    end
    if not stat.filename then
        if not fix_reload_filename(stat.name) then
            return "'"..name.."' could not be found by reload_file"
        end
    end
    return do_reload
end
table.insert(package.loaders, 1, reload_fullpath_loader)

local function first_load(stat)
    local r, v = pcall(os.ctime, stat.filename)
    if r then
        local r, err = xpcall(require, debug.traceback, stat.modulename)
        if r then
            stat.ctm = v
            say_info("reloadfile(\"%s\") succeed", stat.name)
        else
            stat.err_ctm = v
            stat.err_tm = os.time()
            print_warn(stat.name, err)
        end
    else
        stat.filename = nil
        print_warn(stat.name, "check_reload: "..v)
    end
end

local function check_reload(name)
    local stat = reload_files[name]
    if not stat.filename and not fix_reload_filename(name) then
        return
    end
    local r, v = pcall(os.ctime, stat.filename)
    if r then
        if v > stat.ctm and (v > stat.err_ctm or os.time() > stat.err_tm + 5) then
            if v >= os.time()-1 then
                fiber.sleep(1)
                return check_reload(name)
            end
            package.loaded[stat.modulename] = nil
            local r, err = xpcall(require, debug.traceback, stat.modulename)
            if r then
                say_info("reloadfile(\"%s\") succeed", name)
                stat.ctm = v
            else
                stat.err_ctm = v
                stat.err_tm = os.time()
                print_warn(name, err)
            end
        end
    else
        stat.filename = nil
        print_warn(name, "check_reload: "..v)
        return check_reload(name)
    end
end

local loop_unlocked = false
local function unlock_reload_loop()
    if #fiber._locks[reload_lock] == 1 then
        fiber._unlock(reload_lock)
    end
    loop_unlocked = true
end

local function reload_loop()
    while true do
        if not loop_unlocked then
            fiber._lock(reload_lock)
        end
        loop_unlocked = false
        for _, name in ipairs(reload_files) do
            check_reload(name)
        end
    end
end

local function reload_queue_pusher()
    while true do
        fiber.sleep(1)
        unlock_reload_loop()
    end
end

fiber._lock(reload_lock)
fiber.loop("core:reload_loop", reload_loop, {mutable=false})
fiber.loop("core:reload_loop_pusher", reload_queue_pusher, {mutable=false})

function reloadfile(name)
    assertarg(name, 'string', 1)
    local stat = register_reload(name)
    if stat.filename then
        first_load(stat, name)
    end
    unlock_reload_loop()
end
return reloadfile
