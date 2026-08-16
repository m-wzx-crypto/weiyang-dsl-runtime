local cjson = require "cjson.safe"

local _M = {}

local USER_LIMIT = 20
local USER_WINDOW = 60

local SENSITIVE_PATHS = {
    ["/api/ai"] = true,
    ["/api/bff/auth/login"] = true,
    ["/api/bff/admin/auth/login"] = true,
}

local function is_sensitive_path(uri)
    for pattern, _ in pairs(SENSITIVE_PATHS) do
        -- 精确前缀匹配：要求后续字符为 / 或字符串结束，避免 /api/ai 误匹配 /api/airdrop
        if uri == pattern or uri:sub(1, #pattern + 1) == pattern .. "/" then
            return true
        end
    end
    return false
end

local function exit_rate_limited(retry_after)
    ngx.status = ngx.HTTP_TOO_MANY_REQUESTS
    ngx.header["Content-Type"] = "application/json"
    ngx.header["Retry-After"] = tostring(retry_after or 60)
    ngx.say(cjson.encode({
        error = "rate_limited",
        message = "User rate limit exceeded",
        retry_after = retry_after or 60
    }))
    ngx.exit(ngx.HTTP_TOO_MANY_REQUESTS)
end

function _M.check()
    local uri = ngx.var.uri
    if not is_sensitive_path(uri) then
        return
    end

    local user_id = ngx.var.http_x_auth_user_id
    if not user_id or user_id == "" then
        return
    end

    local limit_dict = ngx.shared.user_limit
    if not limit_dict then
        ngx.log(ngx.ERR, "shared dict not found, failing closed")
        ngx.exit(503)
        return
    end

    local path_prefix = "/ai"
    if uri:sub(1, #"/api/bff/auth/login") == "/api/bff/auth/login" then
        path_prefix = "/login"
    elseif uri:sub(1, #"/api/bff/admin/auth/login") == "/api/bff/admin/auth/login" then
        path_prefix = "/admin-login"
    end

    local now_sec = math.floor(ngx.now())
    local window_key = math.floor(now_sec / USER_WINDOW)
    local key = "user:" .. user_id .. ":" .. path_prefix .. ":" .. window_key

    local ok, count, err = pcall(function()
        return limit_dict:incr(key, 1, 0, USER_WINDOW + 1)
    end)

    if not ok then
        ngx.log(ngx.ERR, "user limit incr failed: ", count)
        return
    end

    if err then
        ngx.log(ngx.ERR, "user limit incr error: ", err)
        return
    end

    if count > USER_LIMIT then
        ngx.log(ngx.WARN, "user rate limit exceeded: ", user_id, " path=", path_prefix, " count=", count)
        local remaining_time = USER_WINDOW - (now_sec % USER_WINDOW)
        exit_rate_limited(remaining_time)
        return
    end
end

return _M
