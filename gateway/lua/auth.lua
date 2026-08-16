local jwt = require "resty.jwt"
local cjson = require "cjson.safe"

local _M = {}

-- S3: 限制 JWT 仅允许 HS256 算法,防止 alg 混淆攻击 (如 none / RS256 降级)
jwt:set_alg_whitelist({ HS256 = true })

-- I10: 平台管理员默认租户 ID,取代魔法值,常量定义见 ngx_defs.lua
local DEFAULT_PLATFORM_TENANT_ID = "00000000-0000-0000-0000-000000000000"

-- S4: 模块加载时校验 JWT_SECRET 非空,缺失记录 EMERG (实际 fail-closed 在 verify_jwt 内)
do
  local secret = os.getenv("JWT_SECRET")
  if not secret or secret == "" then
    ngx.log(ngx.EMERG, "JWT_SECRET environment variable is required")
  end
end

local PUBLIC_PATHS = {
    ["/api/bff/auth/login"] = true,
    ["/api/bff/auth/register"] = true,
    ["/api/bff/auth/sms/send"] = true,
    ["/api/bff/auth/refresh"] = true,
    ["/api/bff/auth/wechat/callback"] = true,
    ["/api/bff/tenant/join"] = true,
    ["/api/bff/health"] = true,
    ["/api/bff/admin/auth/login"] = true,
    ["/api/bff/admin/auth/refresh"] = true,
    ["/api/bff/version/check"] = true,
    ["/api/bff/tenants/current"] = true,
    ["/api/bff/tenants/current/status"] = true,
    ["/api/bff/tenants"] = true,
}

local PUBLIC_PREFIXES = {
    "/api/bff/public",
    "/api/bff/files/share/",
}

local PROTECTED_PREFIXES = {
    "/api/ai",
    "/api/biz",
    "/api/bff/ai",
}

local function starts_with(uri, prefix)
    if uri:sub(1, #prefix) ~= prefix then
        return false
    end
    return #uri == #prefix or uri:sub(#prefix + 1, #prefix + 1) == "/"
end

local function is_public_path(uri)
    for _, prefix in ipairs(PROTECTED_PREFIXES) do
        if starts_with(uri, prefix) then
            return false
        end
    end

    if PUBLIC_PATHS[uri] then
        return true
    end

    for _, prefix in ipairs(PUBLIC_PREFIXES) do
        if starts_with(uri, prefix) then
            return true
        end
    end

    return false
end

local function exit_unauthorized(msg)
    ngx.status = ngx.HTTP_UNAUTHORIZED
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({
        error = "unauthorized",
        message = msg
    }))
    ngx.exit(ngx.HTTP_UNAUTHORIZED)
end

-- S4: 服务端配置错误时返回 500,避免静默放行
local function exit_server_error(msg)
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({
        error = "server_error",
        message = msg
    }))
    ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end

function _M.verify_jwt()
    local uri = ngx.var.uri

    -- I3: 每次请求重新读取 JWT_SECRET,支持热更新 (无需 reload worker)
    local jwt_secret = os.getenv("JWT_SECRET")
    -- S4: JWT_SECRET 缺失时 fail-closed,即使是公共路径也返回 500
    if not jwt_secret or jwt_secret == "" then
        ngx.log(ngx.ERR, "JWT_SECRET environment variable is not set")
        exit_server_error("server configuration error")
        return
    end

    -- 安全:无条件清除客户端可能伪造的身份/管理员头。
    -- 这些头只允许由网关在验签 JWT 后注入;若不在此清除,
    -- 公共路径上客户端伪造的 X-Is-Platform-Admin/X-Auth-User-Id 会原样透传给 BFF。
    ngx.req.clear_header("X-Is-Platform-Admin")
    ngx.req.clear_header("X-Auth-User-Id")
    ngx.req.clear_header("X-Auth-Tenant-Id")
    ngx.req.clear_header("X-Tenant-Id")

    if is_public_path(uri) then
        local auth_header = ngx.var.http_x_auth_token
        if auth_header then
            local token = auth_header
            if token and token ~= "" then
                local ok, jwt_obj = pcall(jwt.verify, jwt, jwt_secret, token)
                if ok and jwt_obj and jwt_obj.verified and jwt_obj.payload then
                    local jwt_tid = jwt_obj.payload.tid
                    if jwt_tid then
                        ngx.req.set_header("X-Auth-Tenant-Id", tostring(jwt_tid))
                    else
                        ngx.req.set_header("X-Auth-Tenant-Id", "")
                    end
                    return
                end
            end
        end
        ngx.req.set_header("X-Auth-Tenant-Id", "")
        return
    end

    local auth_header = ngx.var.http_x_auth_token
    if not auth_header or auth_header == "" then
        exit_unauthorized("Missing or invalid token")
        return
    end

    local token = auth_header

    local ok, jwt_obj = pcall(jwt.verify, jwt, jwt_secret, token)
    if not ok then
        ngx.log(ngx.ERR, "JWT verification threw error: ", jwt_obj)
        exit_unauthorized("Missing or invalid token")
        return
    end

    if not jwt_obj then
        exit_unauthorized("Missing or invalid token")
        return
    end

    if not jwt_obj.verified then
        local reason = jwt_obj.reason or "invalid or expired token"
        ngx.log(ngx.WARN, "JWT verification failed: ", reason)
        exit_unauthorized("Missing or invalid token")
        return
    end

    local payload = jwt_obj.payload
    if not payload then
        exit_unauthorized("Missing or invalid token")
        return
    end

    local jti = payload.jti
    if jti then
        local blacklist = ngx.shared.jwt_blacklist
        if blacklist then
            local blacklisted = blacklist:get(jti)
            if blacklisted then
                ngx.log(ngx.WARN, "JWT token is blacklisted: ", jti)
                exit_unauthorized("Missing or invalid token")
                return
            end
        end
    end

    local tid = payload.tid
    local uid = payload.sub or payload.uid
    local exp = payload.exp

    local now = ngx.time()
    if exp and exp < now then
        ngx.log(ngx.WARN, "JWT token expired: exp=", exp, " now=", now)
        exit_unauthorized("Missing or invalid token")
        return
    end

    local is_platform_admin = payload.is_platform_admin

    if not tid or not uid then
        if not is_platform_admin then
            exit_unauthorized("Missing or invalid token")
            return
        end
        tid = DEFAULT_PLATFORM_TENANT_ID
    end

    ngx.req.set_header("X-Auth-Tenant-Id", tostring(tid))
    ngx.req.set_header("X-Auth-User-Id", tostring(uid))

    if is_platform_admin then
        ngx.req.set_header("X-Is-Platform-Admin", "true")
    else
        ngx.req.set_header("X-Is-Platform-Admin", "")
    end

    ngx.ctx.auth_payload = {
        sub = uid,
        tid = tid,
        rid = payload.rid,
        exp = exp,
        jti = jti,
        iat = payload.iat,
        is_platform_admin = is_platform_admin
    }
end

function _M.invalidate_jwt(jti, exp)
    if not jti then
        return false, "missing jti"
    end

    local blacklist = ngx.shared.jwt_blacklist
    if not blacklist then
        return false, "jwt_blacklist shared dict not found"
    end

    local now = ngx.time()
    local ttl
    if exp and exp > now then
        ttl = exp - now
    else
        ttl = 3600
    end

    local ok, err = blacklist:set(jti, "1", ttl)
    if not ok then
        ngx.log(ngx.ERR, "failed to add JWT to blacklist: ", err)
        return false, err
    end

    ngx.log(ngx.INFO, "JWT blacklisted: ", jti, " TTL: ", ttl)
    return true
end

function _M.get_auth_payload()
    return ngx.ctx.auth_payload
end

function _M.get_tenant_id()
    if ngx.ctx.auth_payload then
        return ngx.ctx.auth_payload.tid
    end
    return ngx.var.http_x_auth_tenant_id
end

function _M.get_user_id()
    if ngx.ctx.auth_payload then
        return ngx.ctx.auth_payload.sub
    end
    return ngx.var.http_x_auth_user_id
end

return _M
