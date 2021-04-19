package entity

import (
    "reflect"
)

type EntityField struct {
    Name           string
    DBName         string
    PrimaryKey     bool
    FieldType      reflect.Type
    StructField    reflect.StructField
    StructTags StructTags
    Entity     *Entity
    Index      []int
    ReflectValueOf func(reflect.Value) reflect.Value
    ValueOf        func(reflect.Value) (value interface{}, zero bool)
}

func newField(structField reflect.StructField, structTags StructTags, entity *Entity, index []int) *EntityField {
    field := &EntityField{
        Name:        structField.Name,
        DBName:      structTags.Name,
        StructTags:  structTags,
        PrimaryKey:  structTags.Name == "_id",
        FieldType:   structField.Type,
        StructField: structField,
        Index:       index,
        Entity:      entity,
    }

    field.setupValuerAndSetter()

    return field
}

// create valuer, setter when parse struct
func (field *EntityField) setupValuerAndSetter() {

    var index []int
    if len(field.Index) > 0 {
        index = field.Index
    } else {
        index = field.StructField.Index
    }

    // ValueOf
    switch {
    case len(index) == 1:
        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
            fieldValue := reflect.Indirect(value).Field(index[0])
            return fieldValue.Interface(), fieldValue.IsZero()
        }
    case len(index) == 2 && index[0] >= 0:
        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
            fieldValue := reflect.Indirect(value).Field(index[0]).Field(index[1])
            return fieldValue.Interface(), fieldValue.IsZero()
        }
    default:
        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
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
        if field.FieldType.Kind() == reflect.Ptr {
            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
                fieldValue := reflect.Indirect(value).Field(index[0])
                return fieldValue
            }
        } else {
            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
                return reflect.Indirect(value).Field(index[0])
            }
        }
    case len(index) == 2 && index[0] >= 0 && field.FieldType.Kind() != reflect.Ptr:
        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
            return reflect.Indirect(value).Field(index[0]).Field(index[1])
        }
    default:
        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
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
