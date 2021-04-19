package entity

import (
	"reflect"
)

type EntityField struct {
	Name           string
	DBName         string
	Id             bool
	FieldType      reflect.Type
	StructField    reflect.StructField
	StructTags     StructTags
	Entity         *Entity
	InlineIndex    []int
	ReflectValueOf func(reflect.Value) reflect.Value
	ValueOf        func(reflect.Value) (value interface{}, zero bool)
}

// structField: reflect field
// structTags: represents field information, such as whether it is an inline model, name of database field, etc
// index: the field
func newField(structField reflect.StructField, structTags StructTags, inlineIndex []int) (entityField *EntityField, err error) {

	// get inline entity
	var entity *Entity
	if structTags.Inline {
		entity, err = newEntityByModelType(structField.Type, inlineIndex)
		if err != nil {
			return nil, err
		}
	}

	field := &EntityField{
		Name:        structField.Name,
		DBName:      structTags.Name,
		StructTags:  structTags,
		Id:          structTags.Name == "_id",
		FieldType:   structField.Type,
		StructField: structField,
		InlineIndex: inlineIndex,
		Entity:      entity,
	}

	field.setupValuerAndSetter(inlineIndex)

	return field, nil
}

// create valuer, setter when parse struct
func (th *EntityField) setupValuerAndSetter(index []int) {

	// ValueOf
	switch {
	case len(index) == 1:
		th.ValueOf = func(value reflect.Value) (interface{}, bool) {
			fieldValue := reflect.Indirect(value).Field(index[0])
			return fieldValue.Interface(), fieldValue.IsZero()
		}
	case len(index) == 2 && index[0] >= 0:
		th.ValueOf = func(value reflect.Value) (interface{}, bool) {
			fieldValue := reflect.Indirect(value).Field(index[0]).Field(index[1])
			return fieldValue.Interface(), fieldValue.IsZero()
		}
	default:
		th.ValueOf = func(value reflect.Value) (interface{}, bool) {
			v := reflect.Indirect(value)

			for _, idx := range index {
				if idx >= 0 {
					v = v.Field(idx)
				} else {
					v = v.Field(-idx - 1)

					if v.Type().Elem().Kind() == reflect.Struct {
						if !v.IsNil() {
							v = v.Elem()
						} else {
							return nil, true
						}
					} else {
						return nil, true
					}
				}
			}
			return v.Interface(), v.IsZero()
		}
	}

	// ReflectValueOf
	switch {
	case len(index) == 1:
		if th.FieldType.Kind() == reflect.Ptr {
			th.ReflectValueOf = func(value reflect.Value) reflect.Value {
				fieldValue := reflect.Indirect(value).Field(index[0])
				return fieldValue
			}
		} else {
			th.ReflectValueOf = func(value reflect.Value) reflect.Value {
				return reflect.Indirect(value).Field(index[0])
			}
		}
	case len(index) == 2 && index[0] >= 0 && th.FieldType.Kind() != reflect.Ptr:
		th.ReflectValueOf = func(value reflect.Value) reflect.Value {
			return reflect.Indirect(value).Field(index[0]).Field(index[1])
		}
	default:
		th.ReflectValueOf = func(value reflect.Value) reflect.Value {
			v := reflect.Indirect(value)
			for idx, fieldIdx := range index {
				if fieldIdx >= 0 {
					v = v.Field(fieldIdx)
				} else {
					v = v.Field(-fieldIdx - 1)
				}

				if v.Kind() == reflect.Ptr {
					if v.Type().Elem().Kind() == reflect.Struct {
						if v.IsNil() {
							v.Set(reflect.New(v.Type().Elem()))
						}
					}

					if idx < len(index)-1 {
						v = v.Elem()
					}
				}
			}
			return v
		}
	}
}
