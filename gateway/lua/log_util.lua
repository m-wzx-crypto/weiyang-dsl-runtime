local cjson = require "cjson.safe"

local _M = {}

local LOG_API_URL = os.getenv("LOG_API_URL") or ""
local LOG_API_ENABLED = LOG_API_URL ~= ""
-- I6: 优先使用独立的 LOG_API_SECRET,缺失时回退到 X_INTERNAL_SECRET 并记录警告
local LOG_API_SECRET = os.getenv("LOG_API_SECRET")
if not LOG_API_SECRET or LOG_API_SECRET == "" then
    LOG_API_SECRET = os.getenv("X_INTERNAL_SECRET") or ""
    if LOG_API_SECRET ~= "" then
        ngx.log(ngx.WARN, "LOG_API_SECRET not set, falling back to X_INTERNAL_SECRET")
    end
end

-- I5: 脱敏 URI 中的敏感查询参数 (token/password/code/key/secret),避免日志泄漏
local function sanitize_uri(uri)
    if not uri then return uri end
    local sanitized, err = ngx.re.sub(uri, [[([?&](?:token|password|code|key|secret)=)[^&]+]], "$1***", "i")
    if err then
        return uri
    end
    return sanitized
end

local function build_log_entry()
    local auth_payload = ngx.ctx.auth_payload

    return {
        timestamp = ngx.now(),
        remote_addr = ngx.var.remote_addr,
        request_method = ngx.var.request_method,
        -- I5: 记录前对 request_uri 脱敏,避免敏感参数泄漏到日志
        request_uri = sanitize_uri(ngx.var.request_uri),
        status = ngx.var.status,
        body_bytes_sent = ngx.var.body_bytes_sent,
        request_time = ngx.var.request_time,
        upstream_response_time = ngx.var.upstream_response_time,
        http_referer = ngx.var.http_referer,
        http_user_agent = ngx.var.http_user_agent,
        tenant_id = ngx.var.http_x_auth_tenant_id or "",
        user_id = ngx.var.http_x_auth_user_id or "",
        request_id = ngx.var.request_id or "",
        scheme = ngx.var.scheme,
        server_name = ngx.var.server_name,
    }
end

local function async_send_log(premature, entry)
    if premature then return end
    if not LOG_API_ENABLED then return end

    local http = require "resty.http"
    local httpc = http:new()
    httpc:set_timeouts(500, 1000, 1000)

    local ok, err = pcall(function()
        local res, err2 = httpc:request_uri(LOG_API_URL, {
            method = "POST",
            body = cjson.encode(entry),
            headers = {
                ["Content-Type"] = "application/json",
                ["X-Internal-Secret"] = LOG_API_SECRET,
            },
        })
        if not res then
            ngx.log(ngx.ERR, "failed to send log to API: ", err2)
        end
    end)

    if not ok then
        ngx.log(ngx.ERR, "failed to send log to API: ", err)
    end
end

function _M.log_request()
    local ok, entry = pcall(build_log_entry)
    if not ok then
        ngx.log(ngx.ERR, "failed to build log entry: ", entry)
        return
    end

    if LOG_API_ENABLED then
        local ok2, err = ngx.timer.at(0, async_send_log, entry)
        if not ok2 then
            ngx.log(ngx.ERR, "failed to create async log timer: ", err)
        end
    end

    local tenant_id = entry.tenant_id or "-"
    local user_id = entry.user_id or "-"
    local request_time = entry.request_time or "-"

    ngx.log(ngx.INFO,
        "tenant=", tenant_id,
        " user=", user_id,
        " method=", entry.request_method or "-",
        " uri=", entry.request_uri or "-",
        " status=", entry.status or "-",
        " time=", request_time,
        " upstream_time=", entry.upstream_response_time or "-"
    )
end

return _M
