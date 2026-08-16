--[[
IP黑名单安全模块

职责边界：
  仅做应用层校验——对请求来源IP进行黑名单匹配。
  本模块不负责DDoS防护，DDoS防护由腾讯云安全组承担。

容量限制：
  IP黑名单共享字典(ip_blacklist)容量上限为1000条。
  超出时旧条目将按TTL自动过期淘汰，或由同步机制覆盖。

相关模块：
  - SQL注入特征正则匹配、XSS payload检测由上游应用层(WAF规则)负责
  - 本模块仅聚焦IP级别的访问控制
]]

local cjson = require "cjson.safe"

local _M = {}

local function exit_forbidden()
    ngx.status = ngx.HTTP_FORBIDDEN
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({
        error = "forbidden",
        message = "Access denied"
    }))
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

function _M.check()
    local client_ip = ngx.var.remote_addr
    if not client_ip then
        return
    end

    local blacklist_dict = ngx.shared.ip_blacklist
    if not blacklist_dict then
        ngx.log(ngx.ERR, "shared dict not found, failing closed")
        ngx.exit(503)
        return
    end

    local ok, blocked = pcall(function()
        return blacklist_dict:get(client_ip)
    end)

    if not ok then
        ngx.log(ngx.ERR, "ip blacklist lookup failed: ", blocked)
        exit_forbidden()
        return
    end

    if blocked then
        ngx.log(ngx.WARN, "blocked request from blacklisted IP: ", client_ip)
        exit_forbidden()
        return
    end
end

function _M.add_ip(ip, ttl)
    if not ip or ip == "" then
        return false, "missing ip"
    end

    local blacklist_dict = ngx.shared.ip_blacklist
    if not blacklist_dict then
        return false, "ip_blacklist shared dict not found"
    end

    local ok, err = pcall(function()
        if ttl and ttl > 0 then
            return blacklist_dict:set(ip, "1", ttl)
        else
            return blacklist_dict:set(ip, "1")
        end
    end)

    if not ok then
        ngx.log(ngx.ERR, "failed to add IP to blacklist: ", err)
        return false, err
    end

    ngx.log(ngx.INFO, "IP blacklisted: ", ip, " TTL: ", ttl or "permanent")
    return true
end

function _M.remove_ip(ip)
    if not ip or ip == "" then
        return false, "missing ip"
    end

    local blacklist_dict = ngx.shared.ip_blacklist
    if not blacklist_dict then
        return false, "ip_blacklist shared dict not found"
    end

    local ok, err = pcall(function()
        return blacklist_dict:delete(ip)
    end)

    if not ok then
        ngx.log(ngx.ERR, "failed to remove IP from blacklist: ", err)
        return false, err
    end

    ngx.log(ngx.INFO, "IP removed from blacklist: ", ip)
    return true
end

return _M
