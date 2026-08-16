FROM openresty/openresty:alpine-fat

RUN apk add --no-cache openssl

COPY nginx.conf /usr/local/openresty/nginx/conf/nginx.conf
RUN sed -i 's|pid /var/run/nginx.pid|pid /tmp/nginx.pid|' /usr/local/openresty/nginx/conf/nginx.conf
COPY lua /usr/local/openresty/nginx/lua
COPY ssl /usr/local/openresty/nginx/ssl
COPY entrypoint.sh /entrypoint.sh

RUN mkdir -p /certs /var/log/nginx \
    /usr/local/openresty/nginx/client_body_temp \
    /usr/local/openresty/nginx/proxy_temp \
    /usr/local/openresty/nginx/fastcgi_temp \
    /usr/local/openresty/nginx/uwsgi_temp \
    /usr/local/openresty/nginx/scgi_temp && \
    chown -R nobody:nobody /certs /var/log/nginx \
    /usr/local/openresty/nginx/client_body_temp \
    /usr/local/openresty/nginx/proxy_temp \
    /usr/local/openresty/nginx/fastcgi_temp \
    /usr/local/openresty/nginx/uwsgi_temp \
    /usr/local/openresty/nginx/scgi_temp && \
    chmod +x /entrypoint.sh /usr/local/openresty/nginx/ssl/gen_self_signed.sh

USER nobody

EXPOSE 80 443

# I13: 容器健康检查,定期探测 /health 端点,失败自动重启
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD wget -q -O- http://127.0.0.1/health || exit 1

WORKDIR /usr/local/openresty/nginx/conf

ENTRYPOINT ["/entrypoint.sh"]
