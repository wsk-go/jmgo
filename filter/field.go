package filter

import "reflect"

type FilterField struct {
    RelativeFieldName    string
    Name                 string
    Id                   bool
    FieldType            reflect.Type
    StructField          reflect.StructField
    StructTags           StructTags
    Entity               *Filter
    index                int
    inlineIndex          []int
    ReflectValueOf       func(reflect.Value) reflect.Value
    ValueOf              func(reflect.Value) (value interface{}, zero bool)
    InlineReflectValueOf func(reflect.Value) reflect.Value
    InlineValueOf        func(reflect.Value) (value interface{}, zero bool)
}

// structField: reflect field
// structTags: represents field information, such as whether it is an inline model, name of database field, etc
// index: the field
func newField(structField reflect.StructField, structTags StructTags, inlineIndex []int) (entityField *FilterField, err error) {

    // get inline entity
    var entity *Filter

    // get index on current entity field
    var index int
    if len(inlineIndex) > 0 {
        index = inlineIndex[len(inlineIndex)-1]
    }

    inlineValueOf, inlineReflectValueOf := setupValuerAndSetter(inlineIndex, structField.Type)
    valueOf, reflectValueOf := setupValuerAndSetter([]int{index}, structField.Type)

    field := &FilterField{
        RelativeFieldName:    structTags.Name,
        Name:                 structField.Name,
        StructTags:           structTags,
        Id:                   structTags.Name == "_id",
        FieldType:            structField.Type,
        StructField:          structField,
        Entity:               entity,
        ValueOf:              valueOf,
        index:                index,
        ReflectValueOf:       reflectValueOf,
        InlineReflectValueOf: inlineReflectValueOf,
        InlineValueOf:        inlineValueOf,
    }

    return field, nil
}

type ValueOfFunc func(value reflect.Value) (interface{}, bool)
type ReflectOfFunc func(value reflect.Value) reflect.Value

// create valuer, setter when parse struct
func setupValuerAndSetter(index []int, fieldType reflect.Type) (valueOf ValueOfFunc, reflectOf ReflectOfFunc) {

    // ValueOf
    switch {
    case len(index) == 1:
        valueOf = func(value reflect.Value) (interface{}, bool) {
            fieldValue := reflect.Indirect(value).Field(index[0])
            return fieldValue.Interface(), fieldValue.IsZero()
        }
    case len(index) == 2 && index[0] >= 0:
        valueOf = func(value reflect.Value) (interface{}, bool) {
            fieldValue := reflect.Indirect(value).Field(index[0]).Field(index[1])
            return fieldValue.Interface(), fieldValue.IsZero()
        }
    default:
        valueOf = func(value reflect.Value) (interface{}, bool) {
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
        if fieldType.Kind() == reflect.Ptr {
            reflectOf = func(value reflect.Value) reflect.Value {
                fieldValue := reflect.Indirect(value).Field(index[0])
                return fieldValue
            }
        } else {
            reflectOf = func(value reflect.Value) reflect.Value {
                return reflect.Indirect(value).Field(index[0])
            }
        }
    case len(index) == 2 && index[0] >= 0 && fieldType.Kind() != reflect.Ptr:
        reflectOf = func(value reflect.Value) reflect.Value {
            return reflect.Indirect(value).Field(index[0]).Field(index[1])
        }
    default:
        reflectOf = func(value reflect.Value) reflect.Value {
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

    return valueOf, reflectOf
}
