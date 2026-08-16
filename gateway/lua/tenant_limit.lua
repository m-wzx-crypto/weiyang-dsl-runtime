local cjson = require "cjson.safe"

local _M = {}

local TIER_QUOTAS = {
    basic = 100,
    pro = 500,
    enterprise = 2000
}

local WINDOW_SIZE = 60

local function get_tenant_quota(tenant_id)
    local quota_dict = ngx.shared.tenant_quota
    if not quota_dict then
        return TIER_QUOTAS.basic
    end

    local ok, tier = pcall(function()
        return quota_dict:get(tenant_id)
    end)

    if not ok or not tier then
        return TIER_QUOTAS.basic
    end

    return TIER_QUOTAS[tier] or TIER_QUOTAS.basic
end

local function exit_rate_limited(retry_after)
    ngx.status = ngx.HTTP_TOO_MANY_REQUESTS
    ngx.header["Content-Type"] = "application/json"
    ngx.header["Retry-After"] = tostring(retry_after or 1)
    ngx.say(cjson.encode({
        error = "rate_limited",
        message = "Tenant rate limit exceeded",
        retry_after = retry_after or 1
    }))
    ngx.exit(ngx.HTTP_TOO_MANY_REQUESTS)
end

function _M.check()
    local tenant_id = ngx.var.http_x_auth_tenant_id
    if not tenant_id or tenant_id == "" then
        return
    end

    local limit_dict = ngx.shared.tenant_limit
    if not limit_dict then
        ngx.log(ngx.ERR, "shared dict not found, failing closed")
        ngx.exit(503)
        return
    end

    local quota = get_tenant_quota(tenant_id)
    local now = ngx.now()
    local window_key = math.floor(now / WINDOW_SIZE)
    local key = "tenant:" .. tenant_id .. ":" .. window_key

    local ok, count, err = pcall(function()
        -- I9: 窗口 TTL 从 120s 改为 65s (WINDOW_SIZE + 5),避免旧窗口 key 残留过久占用内存
        return limit_dict:incr(key, 1, 0, 65)
    end)

    if not ok then
        ngx.log(ngx.ERR, "tenant limit incr failed: ", count)
        ngx.exit(503)
        return
    end

    if err then
        ngx.log(ngx.ERR, "tenant limit incr error: ", err)
        ngx.exit(503)
        return
    end

    if count > quota then
        ngx.log(ngx.WARN, "tenant rate limit exceeded: ", tenant_id, " count=", count, " quota=", quota)
        exit_rate_limited(1)
        return
    end
end

function _M.check_with_params(tenant_id, quota_override)
    if not tenant_id or tenant_id == "" then
        return
    end

    local limit_dict = ngx.shared.tenant_limit
    if not limit_dict then
        ngx.log(ngx.ERR, "shared dict not found, failing closed")
        ngx.exit(503)
        return
    end

    local quota = quota_override or get_tenant_quota(tenant_id)
    local window_key = math.floor(ngx.now() / WINDOW_SIZE)
    local key = "tenant:" .. tenant_id .. ":" .. window_key

    local ok, count, err = pcall(function()
        -- I9: 窗口 TTL 从 120s 改为 65s (WINDOW_SIZE + 5),避免旧窗口 key 残留过久占用内存
        return limit_dict:incr(key, 1, 0, 65)
    end)

    if not ok then
        ngx.log(ngx.ERR, "tenant limit incr failed: ", count)
        return
    end

    if err then
        ngx.log(ngx.ERR, "tenant limit incr error: ", err)
        return
    end

    if count > quota then
        return false, count, quota
    end

    return true, count, quota
end

return _M
