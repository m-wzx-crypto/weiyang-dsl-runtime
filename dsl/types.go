package dsl

import (
	"encoding/json"
	"fmt"
	"sort"
)

// TypeKind enumerates the built-in DSL value types.
//
// 用户建议的第 6 点"真正的 Type System"：在 string / number / boolean / object 之外，
// 进一步支持 Date/Enum/Array/Optional 等可组合类型，使得 "amount > \"hello\"" 这类
// 逻辑类型错误可以在执行前被 Type Checker 发现。这里的类型抽象被 ExpressionEngine
// 的 TypeCheck 阶段消费。
type TypeKind int

const (
	TypeAny TypeKind = iota
	TypeString
	TypeNumber
	TypeBoolean
	TypeObject
	TypeArray
	TypeEnum
	TypeDateTime
	TypeMoney
)

func (k TypeKind) String() string {
	switch k {
	case TypeString:
		return "string"
	case TypeNumber:
		return "number"
	case TypeBoolean:
		return "boolean"
	case TypeObject:
		return "object"
	case TypeArray:
		return "array"
	case TypeEnum:
		return "enum"
	case TypeDateTime:
		return "date_time"
	case TypeMoney:
		return "money"
	default:
		return "any"
	}
}

// Field describes a named field on an object type. Optional 标记运行期可能缺失
// 的字段;注意当前静态类型检查(toGoType)将其与必填字段同等对待,缺失时的
// nil 防护属于运行期语义,尚未进入类型检查。
type Field struct {
	Name     string
	Type     *Type
	Optional bool
}

// Type is a DSL value type. For composite kinds it carries subtype information:
//   - TypeObject -> Fields
//   - TypeArray  -> Elem
//   - TypeEnum   -> Enum (allowed literal values)
type Type struct {
	Kind   TypeKind
	Elem   *Type   // element type for TypeArray
	Fields []*Field // field list for TypeObject
	Enum   []string // allowed values for TypeEnum
}

func (t *Type) String() string {
	if t == nil {
		return "any"
	}
	switch t.Kind {
	case TypeArray:
		if t.Elem == nil {
			return "[]any"
		}
		return "[]" + t.Elem.String()
	case TypeObject:
		return "object"
	case TypeEnum:
		if len(t.Enum) > 0 {
			return fmt.Sprintf("enum(%s,...)", t.Enum[0])
		}
		return "enum"
	default:
		return t.Kind.String()
	}
}

// TypeRegistry holds named reusable types (User, Order, Approval, Money, ...)
// so a process may declare rich domain types and reuse them across variables.
type TypeRegistry struct {
	types map[string]*Type
}

func NewTypeRegistry() *TypeRegistry {
	return &TypeRegistry{types: make(map[string]*Type)}
}

// Register associates name with t. It refuses to overwrite an existing entry.
func (r *TypeRegistry) Register(name string, t *Type) error {
	if name == "" || t == nil {
		return fmt.Errorf("type name and definition are required")
	}
	if _, exists := r.types[name]; exists {
		return fmt.Errorf("type %q is already registered", name)
	}
	r.types[name] = t
	return nil
}

func (r *TypeRegistry) Lookup(name string) (*Type, bool) {
	t, ok := r.types[name]
	return t, ok
}

// TypeSchema binds expression identifiers (top-level variables) to their declared
// types. It is the object consumed by ExpressionEngine.TypeCheck.
type TypeSchema struct {
	Registry *TypeRegistry
	Vars     map[string]*Type
}

func NewTypeSchema() *TypeSchema {
	return &TypeSchema{
		Registry: NewTypeRegistry(),
		Vars:     make(map[string]*Type),
	}
}

// Declare binds name to a type. A nil name is ignored.
func (s *TypeSchema) Declare(name string, t *Type) {
	if name == "" {
		return
	}
	if s.Vars == nil {
		s.Vars = make(map[string]*Type)
	}
	s.Vars[name] = t
}

// Convenience constructors for building schemas programmatically.
func StringType() *Type       { return &Type{Kind: TypeString} }
func NumberType() *Type       { return &Type{Kind: TypeNumber} }
func BooleanType() *Type      { return &Type{Kind: TypeBoolean} }
func AnyType() *Type          { return &Type{Kind: TypeAny} }
func DateTimeType() *Type     { return &Type{Kind: TypeDateTime} }
func MoneyType() *Type        { return &Type{Kind: TypeMoney} }

func ObjectType(fields ...*Field) *Type {
	return &Type{Kind: TypeObject, Fields: fields}
}

func ArrayType(elem *Type) *Type {
	return &Type{Kind: TypeArray, Elem: elem}
}

func EnumType(values ...string) *Type {
	return &Type{Kind: TypeEnum, Enum: values}
}

// NewField 构造一个对象类型的具名字段。
func NewField(name string, t *Type) *Field {
	return &Field{Name: name, Type: t}
}

// NewOptionalField 构造一个运行时可能缺失（视为 nil）的可选字段。
func NewOptionalField(name string, t *Type) *Field {
	return &Field{Name: name, Type: t, Optional: true}
}

// DecodeTypeSpec 把 DSL JSON 中的类型声明解码为 *Type,供 v2 数据契约使用。
//
// 支持两种写法:
//
//	字符串简写: "string" | "number" | "boolean" | "date" | "money" | "any"
//	对象形式:   {"type":"object","fields":{...}} | {"type":"array","elem":...}
//	            {"type":"enum","values":[...]}
//
// 对象的 fields 与数组的 elem 递归支持同样的两种写法,因此可以声明任意深度的
// 嵌套结构(嵌套对象/对象数组等)。
func DecodeTypeSpec(raw json.RawMessage) (*Type, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("type spec is empty")
	}
	var name string
	if err := json.Unmarshal(raw, &name); err == nil {
		return simpleType(name)
	}

	var spec struct {
		Type   string                     `json:"type"`
		Fields map[string]json.RawMessage `json:"fields"`
		Elem   json.RawMessage            `json:"elem"`
		Values []string                   `json:"values"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("invalid type spec %s: %w", string(raw), err)
	}

	switch spec.Type {
	case "object":
		if len(spec.Fields) == 0 {
			return ObjectType(), nil
		}
		names := make([]string, 0, len(spec.Fields))
		for n := range spec.Fields {
			names = append(names, n)
		}
		sort.Strings(names)
		fields := make([]*Field, 0, len(names))
		for _, n := range names {
			ft, err := DecodeTypeSpec(spec.Fields[n])
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", n, err)
			}
			fields = append(fields, NewField(n, ft))
		}
		return ObjectType(fields...), nil
	case "array":
		if spec.Elem == nil {
			return ArrayType(AnyType()), nil
		}
		elem, err := DecodeTypeSpec(spec.Elem)
		if err != nil {
			return nil, fmt.Errorf("array elem: %w", err)
		}
		return ArrayType(elem), nil
	case "enum":
		if len(spec.Values) == 0 {
			return nil, fmt.Errorf("enum type requires at least one value")
		}
		return EnumType(spec.Values...), nil
	case "":
		return nil, fmt.Errorf("type spec %s missing \"type\" field", string(raw))
	default:
		return simpleType(spec.Type)
	}
}

// simpleType 把类型名映射为内置标量类型。
func simpleType(name string) (*Type, error) {
	switch name {
	case "string", "enum":
		return StringType(), nil
	case "number", "money":
		return NumberType(), nil
	case "boolean":
		return BooleanType(), nil
	case "date", "date_time", "datetime":
		return DateTimeType(), nil
	case "any":
		return AnyType(), nil
	default:
		return nil, fmt.Errorf("unknown type %q", name)
	}
}