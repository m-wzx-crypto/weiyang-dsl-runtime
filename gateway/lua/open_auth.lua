---@diagnostic disable: undefined-field
local cjson = require "cjson.safe"

local _M = {}

local function exit_unauthorized(msg)
    ngx.status = ngx.HTTP_UNAUTHORIZED
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({
        error = "unauthorized",
        message = msg
    }))
    ngx.exit(ngx.HTTP_UNAUTHORIZED)
end

local function exit_too_many_requests(msg)
    ngx.status = 429
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({
        error = "rate_limit_exceeded",
        message = msg
    }))
    ngx.exit(429)
end

local function compute_sha256(key)
    local resty_sha256 = require "resty.sha256"
    local str = require "resty.string"
    local sha256 = resty_sha256:new()
    sha256:update(key)
    return str.to_hex(sha256:final())
end

local function get_redis_connection()
    local redis = require "resty.redis"
    local red = redis:new()
    red:set_timeouts(1000, 1000, 1000)

    local redis_host = os.getenv("REDIS_HOST") or "redis"
    local redis_port = tonumber(os.getenv("REDIS_PORT")) or 6379
    local redis_password = os.getenv("REDIS_PASSWORD") or ""

    local ok, err = red:connect(redis_host, redis_port)
    if not ok then
        return nil, err
    end

    if redis_password and redis_password ~= "" then
        local ok2, auth_err = red:auth(redis_password)
        if not ok2 then
            return nil, auth_err
        end
    end

    return red, nil
end

local function check_api_rate_limit(red, tenant_id, api_rate_limit)
    if not api_rate_limit or api_rate_limit <= 0 then
        return true
    end

    if api_rate_limit == -1 then
        return true
    end

    local rate_limit_key = "api_rate:" .. tenant_id
    local current, err = red:incr(rate_limit_key)
    if err then
        ngx.log(ngx.ERR, "failed to increment api rate limit: ", err)
        return true
    end

    if current == 1 then
        red:expire(rate_limit_key, 60)
    end

    if current > api_rate_limit then
        return false, api_rate_limit
    end

    return true
end

function _M.verify()
    local api_key = ngx.var.http_x_api_key
    if not api_key or api_key == "" then
        exit_unauthorized("Missing X-API-Key header")
        return
    end

    local key_hash = compute_sha256(api_key)
    if not key_hash then
        exit_unauthorized("Invalid API key")
        return
    end

    local red, err = get_redis_connection()
    if not red then
        ngx.log(ngx.ERR, "failed to connect to redis for api key validation: ", err)
        exit_unauthorized("Internal error")
        return
    end

    local cache_key = "api_key:" .. key_hash
    local cached = red:get(cache_key)

    local tenant_id
    local scopes_str
    local api_rate_limit_str

    if cached and cached ~= ngx.null then
        local cached_data = cjson.decode(cached)
        if cached_data then
            tenant_id = cached_data.tenant_id
            scopes_str = cached_data.scopes
            api_rate_limit_str = cached_data.api_rate_limit
        end
    else
        local biz_host = os.getenv("BIZ_GRPC_HOST") or "biz"
        local biz_port = os.getenv("BIZ_GRPC_PORT") or "8080"
        local x_internal_secret = os.getenv("X_INTERNAL_SECRET") or ""

        local http = require "resty.http"
        local httpc = http.new()
        local res, err2 = httpc:request_uri("http://" .. biz_host .. ":" .. biz_port .. "/api/apikeys/validate", {
            method = "POST",
            headers = {
                ["Content-Type"] = "application/json",
                ["X-Internal-Secret"] = x_internal_secret,
                ["X-Api-Key-Hash"] = key_hash,
            },
            body = cjson.encode({ key_hash = key_hash }),
            timeout = 5000,
        })

        if not res or res.status ~= 200 then
            ngx.log(ngx.ERR, "failed to validate api key via biz service: ", err2)
            red:set_keepalive(10000, 100)
            exit_unauthorized("Invalid API key")
            return
        end

        local result = cjson.decode(res.body)
        if not result or not result.valid then
            red:set_keepalive(10000, 100)
            exit_unauthorized("Invalid API key")
            return
        end

        tenant_id = result.tenant_id
        scopes_str = cjson.encode(result.scopes or {})
        api_rate_limit_str = tostring(result.api_rate_limit or 1000)

        local cache_data = cjson.encode({
            tenant_id = tenant_id,
            scopes = scopes_str,
            api_rate_limit = api_rate_limit_str,
        })
        -- I4: 缓存 TTL 从 3600s (1h) 降为 300s (5min),降低密钥吊销后的放行窗口
        -- I12: 检查 setex 返回值,失败时记录警告而非静默忽略
        local cache_ok, cache_err = red:setex(cache_key, 300, cache_data)
        if not cache_ok then
            ngx.log(ngx.WARN, "failed to cache api key: ", cache_err)
        end
    end

    red:set_keepalive(10000, 100)

    if not tenant_id or tenant_id == "" then
        exit_unauthorized("Invalid API key")
        return
    end

    local api_rate_limit = tonumber(api_rate_limit_str) or 1000
    if api_rate_limit ~= -1 then
        local red2 = get_redis_connection()
        if red2 then
            local ok, limit = check_api_rate_limit(red2, tenant_id, api_rate_limit)
            red2:set_keepalive(10000, 100)
            if not ok then
                exit_too_many_requests("API rate limit exceeded (limit: " .. tostring(limit) .. " req/min)")
                return
            end
        end
    end

    local api_user_id = "apikey:" .. key_hash:sub(1, 16)

    ngx.req.set_header("X-Auth-Tenant-Id", tostring(tenant_id))
    ngx.req.set_header("X-Auth-User-Id", api_user_id)
    ngx.req.set_header("X-Api-Key-Scopes", scopes_str or "[]")
    -- 安全:API Key 永远不是平台管理员;显式清空以覆盖客户端可能伪造的同名头。
    ngx.req.set_header("X-Is-Platform-Admin", "")

    ngx.ctx.auth_payload = {
        sub = api_user_id,
        tid = tenant_id,
        rid = "",
        scopes = scopes_str,
    }
end

return _M
