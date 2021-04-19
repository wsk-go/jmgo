package filter

//func (th *Filter) ParseField(fieldStruct reflect.StructField) *FilterField {
//
//    // get name in db
//    fieldName := th.relativeFieldName(fieldStruct)
//
//    field := &FilterField{
//        Name:              fieldStruct.Name,
//        RelativeFieldName: fieldName,
//        FieldType:         fieldStruct.Type,
//        StructField:       fieldStruct,
//        Tag:               fieldStruct.Tag,
//        Schema:            th,
//    }
//
//    field.setupValuerAndSetter()
//    return field
//}
//
//func (th *Filter) relativeFieldName(fieldStruct reflect.StructField) string {
//    if name, ok := fieldStruct.Tag.Lookup("jfield"); ok && name != "" {
//        return name
//    } else {
//        s := strings.Split(fieldStruct.Name, ",")[0]
//        s = strings.Trim(s, "")
//        return strings.ToLower(s)
//    }
//}
//
//type FilterField struct {
//    Name              string
//    RelativeFieldName string
//    FieldType         reflect.Type
//    StructField       reflect.StructField
//    Tag               reflect.StructTag
//    Schema            *Filter
//    ReflectValueOf    func(reflect.Value) reflect.Value
//    ValueOf           func(reflect.Value) (value interface{}, zero bool)
//}
//
//// create valuer, setter when parse struct
//func (field *FilterField) setupValuerAndSetter() {
//    // ValueOf
//    switch {
//    case len(field.StructField.Index) == 1:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            fieldValue := reflect.Indirect(value).Field(field.StructField.Index[0])
//            return fieldValue.Interface(), fieldValue.IsZero()
//        }
//    case len(field.StructField.Index) == 2 && field.StructField.Index[0] >= 0:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            fieldValue := reflect.Indirect(value).Field(field.StructField.Index[0]).Field(field.StructField.Index[1])
//            return fieldValue.Interface(), fieldValue.IsZero()
//        }
//    default:
//        field.ValueOf = func(value reflect.Value) (interface{}, bool) {
//            v := reflect.Indirect(value)
//
//            for _, idx := range field.StructField.Index {
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
//    case len(field.StructField.Index) == 1:
//        if field.FieldType.Kind() == reflect.Ptr {
//            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//                fieldValue := reflect.Indirect(value).Field(field.StructField.Index[0])
//                return fieldValue
//            }
//        } else {
//            field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//                return reflect.Indirect(value).Field(field.StructField.Index[0])
//            }
//        }
//    case len(field.StructField.Index) == 2 && field.StructField.Index[0] >= 0 && field.FieldType.Kind() != reflect.Ptr:
//        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//            return reflect.Indirect(value).Field(field.StructField.Index[0]).Field(field.StructField.Index[1])
//        }
//    default:
//        field.ReflectValueOf = func(value reflect.Value) reflect.Value {
//            v := reflect.Indirect(value)
//            for idx, fieldIdx := range field.StructField.Index {
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
//                    if idx < len(field.StructField.Index)-1 {
//                        v = v.Elem()
//                    }
//                }
//            }
//            return v
//        }
//    }
//}
