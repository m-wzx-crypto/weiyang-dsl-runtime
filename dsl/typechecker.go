package dsl

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// typedEnvCache 按 schema 结构签名缓存 buildTypedEnv 的产物:reflect.StructOf
// 成本不低,validator 会对每个 when 表达式各构建一次,同一 schema 重复构建纯属浪费。
// 产物是只读的反射值,跨表达式共享安全。
var typedEnvCache sync.Map // signature string -> interface{}

// buildTypedEnv 根据 TypeSchema 构建一个可供 expr 静态类型检查的强类型 env。
//
// 原理：我们无法在运行期动态声明 Go struct 类型，但 reflect.StructOf 可以产生一个
// 结构体 reflect.Type，expr 对其 reflect，因此顶层标识符（schema.Vars 的 key）与对象
// 子字段都能获得具体 Go 类型，从而让 expr 在编译期完成类型推断。字段名通过
// `expr:"<name>"` tag 显式绑定到 DSL 标识符，避免大小写/下划线差异。
func buildTypedEnv(schema *TypeSchema) (interface{}, error) {
	if schema == nil {
		return map[string]interface{}{}, nil
	}
	sig := schemaSignature(schema)
	if v, ok := typedEnvCache.Load(sig); ok {
		return v, nil
	}
	names := make([]string, 0, len(schema.Vars))
	for name := range schema.Vars {
		names = append(names, name)
	}
	sort.Strings(names) // 保证结果稳定可复现

	fields := make([]reflect.StructField, 0, len(names))
	for _, name := range names {
		goType, err := toGoType(schema.Vars[name])
		if err != nil {
			return nil, fmt.Errorf("variable %q: %w", name, err)
		}
		fields = append(fields, toStructField(name, goType))
	}

	st := reflect.StructOf(fields)
	env := reflect.New(st).Elem().Interface()
	typedEnvCache.Store(sig, env)
	return env, nil
}

// schemaSignature 生成 schema 的结构签名(变量名 + 类型结构的确定性序列化),
// 作为 typedEnvCache 的 key。
func schemaSignature(schema *TypeSchema) string {
	names := make([]string, 0, len(schema.Vars))
	for name := range schema.Vars {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(':')
		writeTypeSig(&b, schema.Vars[name])
		b.WriteByte(';')
	}
	return b.String()
}

func writeTypeSig(b *strings.Builder, t *Type) {
	if t == nil {
		b.WriteString("any")
		return
	}
	switch t.Kind {
	case TypeArray:
		b.WriteString("[]")
		writeTypeSig(b, t.Elem)
	case TypeObject:
		b.WriteString("object{")
		for _, f := range t.Fields {
			b.WriteString(f.Name)
			if f.Optional {
				b.WriteByte('?')
			}
			b.WriteByte(':')
			writeTypeSig(b, f.Type)
			b.WriteByte(',')
		}
		b.WriteString("}")
	case TypeEnum:
		b.WriteString("enum[")
		for _, v := range t.Enum {
			b.WriteString(v)
			b.WriteByte(',')
		}
		b.WriteString("]")
	default:
		b.WriteString(t.Kind.String())
	}
}

// toStructField 为给定标识符构造一个导出的反射字段，并用 expr tag 绑定原名。
func toStructField(ident string, goType reflect.Type) reflect.StructField {
	return reflect.StructField{
		Name: exportedName(ident),
		Type: goType,
		Tag:  reflect.StructTag(fmt.Sprintf("expr:%q", ident)),
	}
}

// toGoType 把 DSL Type 映射为对应的 Go reflect.Type。
func toGoType(t *Type) (reflect.Type, error) {
	if t == nil {
		return reflect.TypeOf((*interface{})(nil)).Elem(), nil
	}
	switch t.Kind {
	case TypeString, TypeEnum, TypeDateTime:
		return reflect.TypeOf(""), nil
	case TypeNumber, TypeMoney:
		return reflect.TypeOf(float64(0)), nil
	case TypeBoolean:
		return reflect.TypeOf(false), nil
	case TypeAny:
		return reflect.TypeOf((*interface{})(nil)).Elem(), nil
	case TypeArray:
		elem := reflect.TypeOf((*interface{})(nil)).Elem()
		if t.Elem != nil {
			ft, err := toGoType(t.Elem)
			if err != nil {
				return nil, err
			}
			elem = ft
		}
		return reflect.SliceOf(elem), nil
	case TypeObject:
		fields := make([]reflect.StructField, 0, len(t.Fields))
		for _, f := range t.Fields {
			ft, err := toGoType(f.Type)
			if err != nil {
				return nil, err
			}
			fields = append(fields, toStructField(f.Name, ft))
		}
		return reflect.StructOf(fields), nil
	default:
		return reflect.TypeOf((*interface{})(nil)).Elem(), nil
	}
}

// exportedName 把任意标识符转成反射可用的导出（首字母大写）字段名。
func exportedName(ident string) string {
	if ident == "" {
		return "X"
	}
	first := strings.ToUpper(ident[:1])
	return first + ident[1:]
}
