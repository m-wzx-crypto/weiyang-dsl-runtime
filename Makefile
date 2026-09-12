# Weiyang DSL Runtime — 本地门禁入口。
#
# ROADMAP §6 的约定:每个 module 的 go build / go vet / go test -race 必须全绿,
# 折叠精确性由性质测试保证。CI(.github/workflows/ci.yml)与本地跑的是同一套门禁:
#
#   make ci      # 与 CI 等价的全量门禁
#
# store-postgres 的集成测试(postgres_test.go)需要环境变量 TEST_POSTGRES_DSN /
# TEST_POSTGRES_DRIVER,缺任一则跳过;make test-pg 负责起库并注入。

MODULES := dsl store-postgres

# 本地集成测试的 Postgres(docker-compose.base.yml 的服务与账号)
PG_COMPOSE   := docker compose -f docker-compose.base.yml
PG_CONTAINER := modumind-postgres
PG_TEST_DSN  := postgres://devuser:devpass@localhost:5432/dsl_test?sslmode=disable

.PHONY: help ci fmt fmt-check vet test test-pg

help: ## 列出可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

ci: fmt-check vet test ## CI 同款全量门禁(ROADMAP §6)

fmt: ## gofmt 所有 module
	gofmt -w $(MODULES)

fmt-check: ## 格式检查(只读,不改文件)
	@out="$$(gofmt -l $(MODULES))"; \
	if [ -n "$$out" ]; then echo "gofmt needed for:"; echo "$$out"; exit 1; fi

vet: ## go build + go vet 所有 module
	@for m in $(MODULES); do echo "== $$m: build/vet"; (cd $$m && go build ./... && go vet ./...) || exit 1; done

test: ## go test -race 所有 module(无 Postgres 时集成测试自动跳过)
	@for m in $(MODULES); do echo "== $$m: test -race"; (cd $$m && go test ./... -race -timeout 300s) || exit 1; done

test-pg: ## 起本地 Postgres 并跑 store-postgres 集成测试
	$(PG_COMPOSE) up -d postgres
	@until $(PG_COMPOSE) exec -T postgres pg_isready -U devuser -d modumind >/dev/null 2>&1; do sleep 1; done
	@$(PG_COMPOSE) exec -T postgres psql -U devuser -d modumind -tAc \
		"SELECT 1 FROM pg_database WHERE datname='dsl_test'" | grep -q 1 \
		|| $(PG_COMPOSE) exec -T postgres createdb -U devuser dsl_test
	cd store-postgres && TEST_POSTGRES_DSN="$(PG_TEST_DSN)" TEST_POSTGRES_DRIVER=pgx \
		go test ./... -race -v
