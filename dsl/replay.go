package dsl

// replay.go — M3a(W6):任意点重放并从该点继续。
//
// ResumeAt 只折叠 Seq <= upto 的前缀。调用方应为后续执行绑定独立 Journal:
// 原日志在 upto 之后可能仍有旧后缀,向原实例日志直接追加会形成两个互相污染
// 的分支。新 Journal 与前缀可按需合并,用于审计这次从历史点分出的执行。

// ResumeAt 重建 upto(含)时刻的可继续执行 Runtime。upto 沿用 FoldTo 的 Seq
// 语义;恢复幂等表、等待槽、undo 栈与节点访问计数的方式与 Resume 一致。
func ResumeAt(def *ProcessDef, occs []Occurrence, upto int64, sideEffects SideEffectExecutor, opts ...RuntimeOption) (*Runtime, error) {
	ctx, err := FoldTo(def, occs, upto)
	if err != nil {
		return nil, err
	}
	all := append([]RuntimeOption{WithExecutionContext(ctx)}, opts...)
	return NewRuntime(def, sideEffects, all...), nil
}
