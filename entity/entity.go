package entity

import (
    "fmt"
    "github.com/pkg/errors"
    "jmongo/utils"
    "reflect"
    "strings"
    "sync"
)

var cacheStore = &sync.Map{}

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Entity struct {
    // entity name
    Name string
    // model type
    ModelType reflect.Type
    // collection
    Collection string
    //
    PrioritizedPrimaryField *EntityField
    DBNames                 []string
    PrimaryFields           []*EntityField
    PrimaryFieldDBNames     []string
    Fields                  []*EntityField
    FieldsByName            map[string]*EntityField
    FieldsByDBName          map[string]*EntityField
}

// get data type from dialector
func newEntity(dest interface{}) (*Entity, error) {

    if dest == nil {
        return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
    }

    modelType := reflect.ValueOf(dest).Type()
    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
        }
        return nil, fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
    }

    if v, ok := cacheStore.Load(modelType); ok {
        s := v.(*Entity)
        return s, nil
    }

    // get collection name for model
    modelValue := reflect.New(modelType)
    var tableName string
    if tabler, ok := modelValue.Interface().(CollectionNameSupplier); ok {
        tableName = tabler.CollectionName()
    } else {
        tableName = modelType.Name()
    }

    // extract fields from model type
    fields, err := extractFields(modelType)
    if err != nil {
        return nil, err
    }

    // create map for fields by name and by db name
    fieldsByName, fieldsByDBName := makeFieldsByNameAndByDBName(fields)

    // entity
    schema := &Entity{
        Name:           modelType.Name(),
        ModelType:      modelType,
        Collection:     tableName,
        FieldsByName:   fieldsByName,
        FieldsByDBName: fieldsByDBName,
    }

    //
    //for _, field := range schema.Fields {
    //
    //   // nonexistence or shortest path or first appear prioritized if has permission
    //   if v, ok := schema.FieldsByDBName[field.DBName]; !ok {
    //       if _, ok := schema.FieldsByDBName[field.DBName]; !ok {
    //           schema.DBNames = append(schema.DBNames, field.DBName)
    //       }
    //       schema.FieldsByDBName[field.DBName] = field
    //       schema.FieldsByName[field.Name] = field
    //
    //       if v != nil && v.PrimaryKey {
    //           for idx, f := range schema.PrimaryFields {
    //               if f == v {
    //                   schema.PrimaryFields = append(schema.PrimaryFields[0:idx], schema.PrimaryFields[idx+1:]...)
    //               }
    //           }
    //       }
    //
    //       if field.PrimaryKey {
    //           schema.PrimaryFields = append(schema.PrimaryFields, field)
    //       }
    //   }
    //
    //   if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
    //       schema.FieldsByName[field.Name] = field
    //   }
    //
    //   field.setupValuerAndSetter()
    //}
    //
    //prioritizedPrimaryField := schema.LookUpField("id")
    //if prioritizedPrimaryField == nil {
    //    prioritizedPrimaryField = schema.LookUpField("ID")
    //}
    //
    //if prioritizedPrimaryField != nil {
    //    if prioritizedPrimaryField.PrimaryKey {
    //        schema.PrioritizedPrimaryField = prioritizedPrimaryField
    //    } else if len(schema.PrimaryFields) == 0 {
    //        prioritizedPrimaryField.PrimaryKey = true
    //        schema.PrioritizedPrimaryField = prioritizedPrimaryField
    //        schema.PrimaryFields = append(schema.PrimaryFields, prioritizedPrimaryField)
    //    }
    //}
    //
    //if schema.PrioritizedPrimaryField == nil && len(schema.PrimaryFields) == 1 {
    //    schema.PrioritizedPrimaryField = schema.PrimaryFields[0]
    //}
    //
    //for _, field := range schema.PrimaryFields {
    //    schema.PrimaryFieldDBNames = append(schema.PrimaryFieldDBNames, field.DBName)
    //}
    //
    //if v, loaded := cacheStore.LoadOrStore(modelType, schema); loaded {
    //    s := v.(*Entity)
    //    return s, s.err
    //}

    return schema, schema.err
}

func extractFields(modelType reflect.Type) ([]*EntityField, error) {
    // get field
    var fields []*EntityField
    for i := 0; i < modelType.NumField(); i++ {
        structField := modelType.Field(i)
        tag := structField.Tag.Get("bson")

        // parse to get bson info
        structTags, err := parseTags(utils.LowerFirst(structField.Name), tag)
        if err != nil {
            return nil, err
        }

        // filter skip field
        if structTags.Skip {
            continue
        }

        field := newField(structField, structTags)
        fields = append(fields, field)
    }

    return fields, nil
}

func makeFieldsByNameAndByDBName(fields []*EntityField) (fieldsByName, fieldsByDBName map[string]*EntityField) {
    fieldsByName := map[string]*EntityField{}
    fieldsByDBName := map[string]*EntityField{}

    for _, field := range fields {

        if v, ok := fieldsByDBName[field.DBName]; !ok {
            fieldsByDBName[field.DBName] = v
        }

        if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
            schema.FieldsByName[field.Name] = field
        }

        field.setupValuerAndSetter()
    }
}

func (th *Entity) MakeSlice() reflect.Value {
    slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(th.ModelType)), 0, 20)
    results := reflect.New(slice.Type())
    results.Elem().Set(slice)
    return results
}

func (th *Entity) LookUpField(name string) *EntityField {
    if field, ok := th.FieldsByDBName[name]; ok {
        return field
    }
    if field, ok := th.FieldsByName[name]; ok {
        return field
    }
    return nil
}

func (th *Entity) PrimaryKeyDBName() string {
    if th.PrioritizedPrimaryField != nil {
        return th.PrioritizedPrimaryField.DBName
    }
    return "_id"
}

func GetOrParse(dest interface{}) (*Entity, error) {
    modelType := reflect.ValueOf(dest).Type()
    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, errors.WithStack(fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest))
        }
        return nil, errors.WithStack(fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name()))
    }

    if v, ok := cacheStore.Load(modelType); ok {
        return v.(*Entity), nil
    }

    return Parse(dest, cacheStore)
}

type StructTags struct {
    Name      string
    OmitEmpty bool
    MinSize   bool
    Truncate  bool
    Inline    bool
    Skip      bool
}

func parseTags(key string, tag string) (StructTags, error) {
    var st StructTags
    if tag == "-" {
        st.Skip = true
        return st, nil
    }

    for idx, str := range strings.Split(tag, ",") {
        if idx == 0 && str != "" {
            key = str
        }
        switch str {
        case "omitempty":
            st.OmitEmpty = true
        case "minsize":
            st.MinSize = true
        case "truncate":
            st.Truncate = true
        case "inline":
            st.Inline = true
        }
    }

    st.Name = key

    return st, nil
}
