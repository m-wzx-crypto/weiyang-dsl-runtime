package dsl

import "fmt"

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

// Field describes a named field on an object type. Optional fields may be absent
// at runtime; type checking treats them as possibly-nil.
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