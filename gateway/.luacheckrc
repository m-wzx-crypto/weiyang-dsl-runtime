-- LuaCheck configuration for OpenResty/NGINX Lua modules

-- Global objects provided by OpenResty/NGINX
read_globals = {
    "ngx",
    "table",
    "string",
    "math",
    "os",
    "io",
    "package",
    "coroutine",
    "debug",
    "bit",
    "jit",
    "ndk",
    "cjson",
    "resty",
    "redis",
    "cpt",
}

-- Ignore unused loop variables (common pattern)
unused_args = false

-- Allow redefining globals (needed for modules)
allow_defined_top = true

-- Max line length
max_line_length = 120

-- Ignore these patterns
ignore = {
    "611", -- line too long (handled by stylua)
    "212", -- unused argument (self is common in methods)
}

-- Additional include paths
include_files = {
    "lua/",
    "lua/resty/",
}