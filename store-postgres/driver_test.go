package storepostgres

// 测试基建:为 postgres_test.go 的集成测试注册 database/sql 驱动。
//
// postgres_test.go 的约定是"本包刻意不绑定任何驱动,由宿主的测试基建提供
// blank import"——本文件就是那块基建:CI 与 make test-pg 用 pgx,
// 生产宿主可按需换成任何已注册驱动。非测试代码保持零驱动依赖。
import (
	_ "github.com/jackc/pgx/v5/stdlib"
)
