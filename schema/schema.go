package schema

import (
    "fmt"
    "github.com/pkg/errors"
    "go/ast"
    "jmongo/logger"
    "reflect"
    "sync"
)

var cacheStore = &sync.Map{}

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Schema struct {
    Name                     string
    ModelType               reflect.Type
    Collection              string
    PrioritizedPrimaryField *Field
    DBNames                  []string
    PrimaryFields            []*Field
    PrimaryFieldDBNames      []string
    Fields                   []*Field
    FieldsByName             map[string]*Field
    FieldsByDBName           map[string]*Field
    err                      error
    initialized              chan struct{}
    cacheStore               *sync.Map
}

func (schema Schema) String() string {
    if schema.ModelType.Name() == "" {
        return fmt.Sprintf("%v(%v)", schema.Name, schema.Collection)
    }
    return fmt.Sprintf("%v.%v", schema.ModelType.PkgPath(), schema.ModelType.Name())
}

func (schema Schema) MakeSlice() reflect.Value {
    slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(schema.ModelType)), 0, 20)
    results := reflect.New(slice.Type())
    results.Elem().Set(slice)
    return results
}

func (schema Schema) LookUpField(name string) *Field {
    if field, ok := schema.FieldsByDBName[name]; ok {
        return field
    }
    if field, ok := schema.FieldsByName[name]; ok {
        return field
    }
    return nil
}

type CollectionNameSupplier interface {
    CollectionName() string
}

// get data type from dialector
func Parse(dest interface{}, cacheStore *sync.Map) (*Schema, error) {

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
        s := v.(*Schema)
        <-s.initialized
        return s, s.err
    }

    modelValue := reflect.New(modelType)
    var tableName string
    if tabler, ok := modelValue.Interface().(CollectionNameSupplier); ok {
        tableName = tabler.CollectionName()
    }

    schema := &Schema{
        Name:           modelType.Name(),
        ModelType:      modelType,
        Collection:     tableName,
        FieldsByName:   map[string]*Field{},
        FieldsByDBName: map[string]*Field{},
        cacheStore:     cacheStore,
        initialized:    make(chan struct{}),
    }

    defer func() {
        if schema.err != nil {
            logger.Default.Error(schema.err.Error())
            cacheStore.Delete(modelType)
        }
    }()

    for i := 0; i < modelType.NumField(); i++ {
        if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) {
            if field := schema.ParseField(fieldStruct); field.EmbeddedSchema != nil {
                schema.Fields = append(schema.Fields, field.EmbeddedSchema.Fields...)
            } else {
                schema.Fields = append(schema.Fields, field)
            }
        }
    }

    for _, field := range schema.Fields {

        // nonexistence or shortest path or first appear prioritized if has permission
        if v, ok := schema.FieldsByDBName[field.DBName]; !ok {
            if _, ok := schema.FieldsByDBName[field.DBName]; !ok {
                schema.DBNames = append(schema.DBNames, field.DBName)
            }
            schema.FieldsByDBName[field.DBName] = field
            schema.FieldsByName[field.Name] = field

            if v != nil && v.PrimaryKey {
                for idx, f := range schema.PrimaryFields {
                    if f == v {
                        schema.PrimaryFields = append(schema.PrimaryFields[0:idx], schema.PrimaryFields[idx+1:]...)
                    }
                }
            }

            if field.PrimaryKey {
                schema.PrimaryFields = append(schema.PrimaryFields, field)
            }
        }

        if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
            schema.FieldsByName[field.Name] = field
        }

        field.setupValuerAndSetter()
    }

    prioritizedPrimaryField := schema.LookUpField("id")
    if prioritizedPrimaryField == nil {
        prioritizedPrimaryField = schema.LookUpField("ID")
    }

    if prioritizedPrimaryField != nil {
        if prioritizedPrimaryField.PrimaryKey {
            schema.PrioritizedPrimaryField = prioritizedPrimaryField
        } else if len(schema.PrimaryFields) == 0 {
            prioritizedPrimaryField.PrimaryKey = true
            schema.PrioritizedPrimaryField = prioritizedPrimaryField
            schema.PrimaryFields = append(schema.PrimaryFields, prioritizedPrimaryField)
        }
    }

    if schema.PrioritizedPrimaryField == nil && len(schema.PrimaryFields) == 1 {
        schema.PrioritizedPrimaryField = schema.PrimaryFields[0]
    }

    for _, field := range schema.PrimaryFields {
        schema.PrimaryFieldDBNames = append(schema.PrimaryFieldDBNames, field.DBName)
    }


    if v, loaded := cacheStore.LoadOrStore(modelType, schema); loaded {
        s := v.(*Schema)
        <-s.initialized
        return s, s.err
    }

    return schema, schema.err
}

func getOrParse(dest interface{}) (*Schema, error) {
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
        return v.(*Schema), nil
    }

    return Parse(dest, cacheStore)
}
