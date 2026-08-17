package dsl

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

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
	return reflect.New(st).Elem().Interface(), nil
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