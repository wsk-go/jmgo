package entity

import (
    "fmt"
    "jmongo/errortype"
    "jmongo/utils"
    "reflect"
    "sync"
)

var cacheStore = &sync.Map{}

type Entity struct {
    Name string
    ModelType reflect.Type
    Collection              string

    PrimaryField *EntityField
    DBNames      []string
    PrimaryFields           []*EntityField
    PrimaryFieldDBNames     []string
    Fields                  []*EntityField
    AllFields               []*EntityField
    FieldsByName            map[string]*EntityField
    FieldsByDBName          map[string]*EntityField
}

// get data type from dialector
func newEntity(dest interface{}) (*Entity, error) {

    if dest == nil {
        return nil, fmt.Errorf("%w: %s", errortype.ErrUnsupportedDataType, "dest is nil")
    }

    modelType := reflect.ValueOf(dest).Type()

    return newEntityByModelType(modelType, nil)
}

func newEntityByModelType(modelType reflect.Type, index []int) (*Entity, error) {

    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, fmt.Errorf("%w: %+v", errortype.ErrUnsupportedDataType, modelType.Name())
        }
        return nil, fmt.Errorf("%w: %v.%v", errortype.ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
    }

    if v, ok := cacheStore.Load(modelType); ok {
        s := v.(*Entity)
        return s, nil
    }

    // get collection name for model
    modelValue := reflect.New(modelType)
    var collectionName string
    if tabler, ok := modelValue.Interface().(CollectionNameSupplier); ok {
        collectionName = tabler.CollectionName()
    } else {
        collectionName = modelType.Name()
    }

    entity := &Entity{}

    // extract fields from model type
    fields, err := extractFields(modelType, index)
    if err != nil {
        return nil, err
    }

    // extract id field from fields
    idField := extractIdField(fields)
    if idField == nil {
        return nil, errortype.ErrIdFieldNotFound
    }

    // create map for fields by name and by db name
    fieldsByName, fieldsByDBName := makeFieldsByNameAndByDBName(fields)

    // entity
    entity.Name = modelType.Name()
    entity.ModelType = modelType
    entity.Fields = fields
    entity.Collection = collectionName
    entity.FieldsByName = fieldsByName
    entity.FieldsByDBName = fieldsByDBName

    return entity, nil
}

func extractFields(modelType reflect.Type, index []int) (fields []*EntityField, err error) {

    // get field
    for i := 0; i < modelType.NumField(); i++ {
        // clone index
        cloneIndex := make([]int, len(index), len(index)+1)
        copy(cloneIndex, index)
        cloneIndex = append(cloneIndex, i)

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

        field, err := newField(structField, structTags, index)
        if err != nil {
            return nil, err
        }
        fields = append(fields, field)
    }

    return fields, nil
}

func extractIdField(fields []*EntityField) *EntityField {
    var idField *EntityField
    for _, field := range fields {
        if field.Entity != nil {
            idField = extractIdField(field.Entity.Fields)
            if idField != nil {
                break
            }
        } else {
            if field.Id {
                idField = field
                break
            }
        }
    }

    return idField
}

func makeFieldsByNameAndByDBName(fields []*EntityField) (fieldsByName, fieldsByDBName map[string]*EntityField) {
    fieldsByName = map[string]*EntityField{}
    fieldsByDBName = map[string]*EntityField{}

    for _, field := range fields {

        if v, ok := fieldsByDBName[field.DBName]; !ok {
            fieldsByDBName[field.DBName] = v
        }

        if v, ok := fieldsByName[field.Name]; !ok {
            fieldsByName[field.Name] = v
        }
    }

    return fieldsByName, fieldsByDBName
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
    if th.PrimaryField != nil {
        return th.PrimaryField.DBName
    }
    return "_id"
}


var mutex sync.Mutex
func GetOrParse(dest interface{}) (*Entity, error) {

    modelType := reflect.ValueOf(dest).Type()
    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, fmt.Errorf("%w: %+v", errortype.ErrUnsupportedDataType, dest)
        }
        return nil, fmt.Errorf("%w: %v.%v", errortype.ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
    }

    if v, ok := cacheStore.Load(modelType); ok {
        return v.(*Entity), nil
    }

    var entity *Entity
    mutex.Lock()
    defer func() {
        mutex.Unlock()
    }()
    if _, ok := cacheStore.Load(modelType); !ok {
        var err error
        entity, err = newEntity(dest)
        if err != nil {
            return nil, err
        }
        cacheStore.Store(modelType, entity)
    }

    return entity, nil
}
