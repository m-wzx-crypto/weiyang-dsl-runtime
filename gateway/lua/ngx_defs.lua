---@meta

-- I10: 平台管理员默认租户 ID 常量,供 auth.lua 等模块引用,取代魔法值
DEFAULT_PLATFORM_TENANT_ID = "00000000-0000-0000-0000-000000000000"

---@class ngx
---@field shared table<string, ngx.shared.DICT>
---@field var ngx.var
---@field header table
---@field status number
---@field location ngx.location
---@field ERR number
---@field EMERG number
---@field WARN number
---@field INFO number
---@field DEBUG number
---@field HTTP_OK number
---@field HTTP_CREATED number
---@field HTTP_BAD_REQUEST number
---@field HTTP_UNAUTHORIZED number
---@field HTTP_FORBIDDEN number
---@field HTTP_NOT_FOUND number
---@field HTTP_TOO_MANY_REQUESTS number
---@field HTTP_INTERNAL_SERVER_ERROR number
---@field req ngx.req
---@field ctx ngx.ctx
---@field re ngx.re
---@field timer ngx.timer
ngx = {}

---@class ngx.var
---@field http_x_auth_tenant_id string
---@field http_x_auth_user_id string
---@field http_x_auth_token string
---@field http_x_tenant_id string
---@field http_authorization string
---@field remote_addr string
---@field uri string
---@field request_uri string
---@field request_method string
---@field host string
---@field status string
---@field body_bytes_sent string
---@field request_time string
---@field upstream_response_time string
---@field http_referer string
---@field http_user_agent string
---@field request_id string
---@field scheme string
---@field server_name string

---@class ngx.shared.DICT
---@field get fun(self: ngx.shared.DICT, key: string): any, string?
---@field set fun(self: ngx.shared.DICT, key: string, value: any, exptime?: number): boolean, string?
---@field incr fun(self: ngx.shared.DICT, key: string, value: number, init?: number, init_ttl?: number): number, string?
---@field delete fun(self: ngx.shared.DICT, key: string): boolean
---@field flush_all fun(self: ngx.shared.DICT)
---@field flush_expired fun(self: ngx.shared.DICT, max_count?: number): number

---@class ngx.req
---@field set_header fun(header_name: string, header_value: string)
---@field clear_header fun(header_name: string)

---@class ngx.ctx
---@field auth_payload ngx.ctx.auth_payload?

---@class ngx.ctx.auth_payload
---@field sub string
---@field tid string
---@field rid string?
---@field exp number?
---@field jti string?
---@field iat number?

---@param ... any
function ngx.log(...) end

---@param msg string
function ngx.say(msg) end

---@param code number
function ngx.exit(code) end

---@return number
function ngx.now() end

---@return number
function ngx.time() end

---@return string
function ngx.md5(str) end

---@class ngx.location
---@field capture fun(self: ngx.location, uri: string, opts?: table): table

---@param uri string
---@param opts table?
---@return table
function ngx.location.capture(uri, opts) end

---@class ngx.re
---@field sub fun(subject: string, regex: string, replace: string, options?: string): string, string?
---@field gsub fun(subject: string, regex: string, replace: string, options?: string): string, number, string?
---@field match fun(subject: string, regex: string, options?: string): table?, string?
ngx.re = {}

---@class ngx.timer
---@field at fun(delay: number, callback: fun(premature: boolean, ...: any), ...: any): boolean, string?
ngx.timer = {}

---@class cjson
---@field encode fun(v: any): string
---@field decode fun(str: string): any
cjson = {}

---@class cjson.safe
---@field encode fun(v: any): string
---@field decode fun(str: string): any, string?
