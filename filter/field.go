package filter

//type EntityField struct {
//    Name           string
//    DBName         string
//    Id     bool
//    FieldType      reflect.Type
//    StructField    reflect.StructField
//    StructTags     StructTags
//    Entity *Entity
//    OwnerSchema    *Entity
//    ReflectValueOf func(reflect.Value) reflect.Value
//    ValueOf        func(reflect.Value) (value interface{}, zero bool)
//}
//
//func newField(field reflect.StructField, structTags StructTags) *EntityField {
//
//    return &EntityField{
//        Name:           field.Name,
//        DBName:         structTags.Name,
//        StructTags:     structTags,
//        Id:     false,
//        FieldType:      field.Type,
//        StructField:    field,
//        Entity: nil,
//        OwnerSchema:    nil,
//        ReflectValueOf: nil,
//        ValueOf:        nil,
//    }
//}
//
//// create valuer, setter when parse struct
//func (field *EntityField) setupValuerAndSetter() {
//    // ValueOf
//    switch {
//    case len(field.StructField.InlineIndex) == 1:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            fieldValue := reflect.Indirect(value).Field(field.StructField.InlineIndex[0])
//            return fieldValue.Interface(), fieldValue.IsZero()
//        }
//    case len(field.StructField.InlineIndex) == 2 && field.StructField.InlineIndex[0] >= 0:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            fieldValue := reflect.Indirect(value).Field(field.StructField.InlineIndex[0]).Field(field.StructField.InlineIndex[1])
//            return fieldValue.Interface(), fieldValue.IsZero()
//        }
//    default:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            v := reflect.Indirect(value)
//
//            for _, idx := range field.StructField.InlineIndex {
//                if idx >= 0 {
//                    v = v.Field(idx)
//                } else {
//                    v = v.Field(-idx - 1)
//
//                    if v.Type().Elem().Kind() == reflect.Struct {
//                        if !v.IsNil() {
//                            v = v.Elem()
//                        } else {
//                            return nil, true
//                        }
//                    } else {
//                        return nil, true
//                    }
//                }
//            }
//            return v.Interface(), v.IsZero()
//        }
//    }
//
//    // ReflectValueOf
//    switch {
//    case len(field.StructField.InlineIndex) == 1:
//        if field.FieldType.Kind() == reflect.Ptr {
//            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//                fieldValue := reflect.Indirect(value).Field(field.StructField.InlineIndex[0])
//                return fieldValue
//            }
//        } else {
//            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//                return reflect.Indirect(value).Field(field.StructField.InlineIndex[0])
//            }
//        }
//    case len(field.StructField.InlineIndex) == 2 && field.StructField.InlineIndex[0] >= 0 && field.FieldType.Kind() != reflect.Ptr:
//        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//            return reflect.Indirect(value).Field(field.StructField.InlineIndex[0]).Field(field.StructField.InlineIndex[1])
//        }
//    default:
//        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//            v := reflect.Indirect(value)
//            for idx, fieldIdx := range field.StructField.InlineIndex {
//                if fieldIdx >= 0 {
//                    v = v.Field(fieldIdx)
//                } else {
//                    v = v.Field(-fieldIdx - 1)
//                }
//
//                if v.Kind() == reflect.Ptr {
//                    if v.Type().Elem().Kind() == reflect.Struct {
//                        if v.IsNil() {
//                            v.Set(reflect.New(v.Type().Elem()))
//                        }
//                    }
//
//                    if idx < len(field.StructField.InlineIndex)-1 {
//                        v = v.Elem()
//                    }
//                }
//            }
//            return v
//        }
//    }
//}
