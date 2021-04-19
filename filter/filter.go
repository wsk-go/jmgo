package filter

//var cacheStore = &sync.Map{}
//
//// ErrUnsupportedDataType unsupported data type
//var ErrUnsupportedDataType = errors.New("unsupported data type")
//
//type Filter struct {
//    RelativeFieldName string
//    ModelType         reflect.Type
//    Fields            []*FilterField
//}
//
//// get data type from dialector
//func Parse(dest interface{}, cacheStore *sync.Map) (*Filter, error) {
//
//    if dest == nil {
//        return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
//    }
//
//    modelType := reflect.Indirect(reflect.ValueOf(dest)).Type()
//
//    if modelType.Kind() != reflect.Struct {
//        if modelType.PkgPath() == "" {
//            return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
//        }
//        return nil, fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
//    }
//
//    if v, ok := cacheStore.Load(modelType); ok {
//        s := v.(*Filter)
//        return s, nil
//    }
//
//    schema := &Filter{
//        ModelType: modelType,
//    }
//
//    for i := 0; i < modelType.NumField(); i++ {
//        if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) {
//            if field := schema.ParseField(fieldStruct); field.EmbeddedSchema != nil {
//                schema.Fields = append(schema.Fields, field.EmbeddedSchema.Fields...)
//            } else {
//                schema.Fields = append(schema.Fields, field)
//            }
//        }
//    }
//
//    if v, loaded := cacheStore.LoadOrStore(modelType, schema); loaded {
//        s := v.(*Filter)
//        return s, nil
//    }
//
//    return schema, nil
//}
//
//func GetOrParse(dest interface{}) (*Filter, error) {
//    modelType := reflect.ValueOf(dest).Type()
//    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
//        modelType = modelType.Elem()
//    }
//
//    if modelType.Kind() != reflect.Struct {
//        if modelType.PkgPath() == "" {
//            return nil, errors.WithStack(fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest))
//        }
//        return nil, errors.WithStack(fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name()))
//    }
//
//    if v, ok := cacheStore.Load(modelType); ok {
//        return v.(*Filter), nil
//    }
//
//    return Parse(dest, cacheStore)
//}
