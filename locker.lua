local cjson = require 'cjson.safe'
local ngx_shared = ngx.shared
local sleep = ngx.sleep
local locker = {nil,nil}

--you should set nginx.conf use nginx.shared.locker
--add "lua_shared_dict lock_dict 10k;" in http {}

--setup the lock
--this return a lock_dict object
function setup()
    local lock_dict = ngx.shared.lock_dict
    if not lock_dict then 
        return nil, "no lock_dict,please add 'lua_shared_dict lock_dict 10k;' in http {}"
    end
    return lock_dict
end

function check(lock_dict_object)
    if not lock_dict_object then 
        return false, "no lock_dict object"
    else 
        return true
    end
end

--this will get the dict. If successful,return a tbale
--return object, ok, err
function getDict(lock_dict, lock_name)
    if not lock_name then 
        return nil, false, "getDict failed: no lock_name"
    end
    if not lock_dict then 
        return nil, false, "getDict failed: no lock_dict object,lock_name:" .. lock_name
    end
    local lock_str = lock_dict:get(lock_name)
    if not lock_str then 
        return nil, false, "getDict failed: lock_name: " .. lock_name .. " do not exist"
    end
    local lock_data = cjson.decode(lock_str)
    if not lock_data then
        return nil, false, "getDict failed: cjson decode failed"
    end
    if type(lock_data) ~= 'table' then 
        return nil, false, "getDict failed: lock_dat is not a table"
    end
    return lock_data, true
end

--lock_data is a table value
--return ok, err
function setDict(lock_dict, lock_name,lock_data)
    if not lock_dict then 
        return false, "setDict failed: No lock_dict object"
    end
    if not lock_name then 
        return false, "setDict failed: No lock_name"
    end
    if not lock_data then 
        return false, "setDict failed: No lock_data"
    end
    if type(lock_data) ~= 'table' then 
        return false, "setDict failed: lock_data is not a table value"
    end
    local lock_str = cjson.encode(lock_data)
    if not lock_str then 
        return false, "setDict failed: cjson encode failed"
    end
    local ok, err = lock_dict:set(lock_name,lock_str)
    if not ok then 
        return ok, "setDict failed ->" .. err
    end
    return ok
end

function doLock(lock_dict, lock_data)
    
    if not lock_data then 
        return false, "doLock failed: no lock_data"
    end

    if type(lock_data) ~= 'table' then 
        return false, "doLock failed: lock_data is not a table"
    end
    lock_data.lock = "locked"
    lock_data.worker = ngx.worker.pid()
    local ok, err = setDict(lock_dict, lock_data)
    return ok, "doLock faied ->" .. err
end

function competeLock(lock_dict, lock_data)
    if not lock_data then 
        return false, "competeLock failed: no lock_data"
    end

    if type(lock_data) ~= 'table' then 
        return false, "competeLock failed: lock_data is not a table"
    end
    local count = 0
    while lock_data.lock == "unlocked" and count < lock_data.compete_time do
        ngx.sleep(0.001)
    end
    
    if lock_data.lock == "unlocked" then 
        return false, "competeLock failed: too many compete times"
    end
    local ok, err = doLock(lock_dict, lock_data)
    if not ok then 
        return false, "competeLock failed in doLock ->" .. err
    end
    return true
end

function competeLock_forced(lock_dict, lock_data)
    --TODO compete the lock forcibly
end

--new function return a table and error
--if error is false then new function create a table successfully
function locker.new(lock_dict, lock_name, sleep_time, compete_time)
    if not lock_name then 
        return false, "new lock failed: no dict_name!"
    end
    local new_lock = {nil,nil,nil}
    new_lock.name = lock_name
    new_lock.sleep_time = sleep_time or 0.001
    new_lock.compete_time = compete_time or 10
    new_lock.lock = "unlocked"
    new_lock.woker = 0
    local ok, err = check(lock_dict) 
    if not ok then 
        return false, "new lock failed ->" .. err
    end
    local ok, err = setDict(lock_dict,new_lock)
    return ok, "new lock failed ->" .. err
end


--compete for lock 
--if forced is true then it will compete the lock until it's true
function locker.lock(lock_dict, lock_name, forced)
    local lock_data, ok, err = getDict(lock_dict, lock_name)
    if not ok then 
        return false, "lock failed ->" .. err
    end
    if lock_data.lock = "unlocked" then 
        local ok, err = doLock(lock_dict, lock_data)
        if not ok then 
            return false, "lock failed ->" .. err
        else
            return ok
        end
    elseif not force then 
        local ok, err = competeLock(lock_dict, lock_data)    
        if not ok then 
            return false, "lock failed ->" .. err
        else
            return ok
        end
    elseif forced then 
        local ok,err = competeLock_forced(lock_dict, lock_data)
        if not ok then 
            return false, "lock failed ->" .. err
        else
            return ok
        end
    else 
        return false, "lock failed: unkown error"
    end
end

function locker.unlock(lock_dict, lock_name)
    local lock_data, ok, err = getDict(lock_dict, lock_name)
    if not ok then 
        return false, "unlock failed ->" .. err
    end
    
    if not lock_data.worker = ngx.worker.pid() then 
        return false, "unlock failed: wrong worker pid:" .. ngx.worker.pid()
    end
    
    lock_data.worker = 0
    lock_data.lock = "unlocked"
    local ok, err = setDict(lock_dict, lock_data)
    if not ok then 
        return false, "unlock failed ->" .. err
    end

    return true
end

function locker.run_with_lock(lock_name, func)
    while(not self.lock(lock_dict, lock_name, false)) do
        self.lock(lock_dict, lock_name, false)
    end
    local result = func(...)
    self.unlock(lock_dict, lock_name)
    return result or false
end
