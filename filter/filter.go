package filter

import (
    "fmt"
    "github.com/pkg/errors"
    "jmongo/errortype"
    "jmongo/utils"
    "reflect"
    "sync"
)

var cacheStore = &sync.Map{}

type Filter struct {
    Name                string
    ModelType           reflect.Type
    Fields              []*FilterField
    AllFields           []*FilterField
}

// get data type from dialector
func newFilter(dest interface{}) (*Filter, error) {

    if dest == nil {
        return nil, errors.WithStack(fmt.Errorf("%w: %s", errortype.ErrUnsupportedDataType, "dest is nil"))
    }

    modelType := reflect.ValueOf(dest).Type()

    return newFilterByModelType(modelType, nil)
}

func newFilterByModelType(modelType reflect.Type, index []int) (*Filter, error) {

    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, errors.WithStack(fmt.Errorf("%w: %+v", errortype.ErrUnsupportedDataType, modelType.Name()))
        }
        return nil, errors.WithStack(fmt.Errorf("%w: %v.%v", errortype.ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name()))
    }

    if v, ok := cacheStore.Load(modelType); ok {
        s := v.(*Filter)
        return s, nil
    }

    entity := &Filter{}

    // extract fields from model type
    fields, allFields, err := extractFields(modelType, index)
    if err != nil {
        return nil, err
    }

    // extract id field from fields
    idField := extractIdField(allFields)
    if idField == nil {
        return nil, errortype.ErrIdFieldDoesNotExists
    }

    // entity
    entity.Name = modelType.Name()
    entity.ModelType = modelType
    entity.Fields = fields
    entity.AllFields = fields

    return entity, nil
}

func extractFields(modelType reflect.Type, index []int) (fields []*FilterField, allFields []*FilterField, err error) {

    // get field
    for i := 0; i < modelType.NumField(); i++ {
        // clone index
        cloneIndex := make([]int, len(index), len(index)+1)
        copy(cloneIndex, index)
        cloneIndex = append(cloneIndex, i)

        structField := modelType.Field(i)
        tag := structField.Tag.Get("jfield")

        // parse to get bson info
        structTags, err := parseTags(utils.LowerFirst(structField.Name), tag)
        if err != nil {
            return nil, nil, err
        }

        // filter skip field
        if structTags.Skip {
            continue
        }

        field, err := newField(structField, structTags, cloneIndex)
        if err != nil {
            return nil, nil, err
        }
        fields = append(fields, field)
        if field.Entity != nil {
            allFields = append(allFields, field.Entity.Fields...)
        } else {
            allFields = append(allFields, field)
        }
    }

    return fields, allFields, nil
}

func extractIdField(fields []*FilterField) *FilterField {

    var idField *FilterField
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

var mutex sync.Mutex

func GetOrParse(dest interface{}) (entity *Filter, err error) {

    modelType := reflect.ValueOf(dest).Type()
    for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    if modelType.Kind() != reflect.Struct {
        if modelType.PkgPath() == "" {
            return nil, errors.WithStack(fmt.Errorf("%w: %+v", errortype.ErrUnsupportedDataType, dest))
        }
        return nil, errors.WithStack(fmt.Errorf("%w: %v.%v", errortype.ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name()))
    }

    if v, ok := cacheStore.Load(modelType); ok {
        return v.(*Filter), nil
    }

    mutex.Lock()
    defer func() {
        mutex.Unlock()
    }()
    if _, ok := cacheStore.Load(modelType); !ok {
        entity, err = newFilter(dest)
        if err != nil {
            return nil, err
        }
        cacheStore.Store(modelType, entity)
    }

    return entity, nil
}
