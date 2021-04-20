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
    Collection              string
    PrioritizedPrimaryField *EntityField
    DBNames                 []string
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
        return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
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
            return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, modelType.Name())
        }
        return nil, fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
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

func extractFields(modelType reflect.Type, index []int) ([]*EntityField, []*EntityField, error) {

    // get field
    var fields []*EntityField
    var allFields []*EntityField
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
            return nil, nil, err
        }

        // filter skip field
        if structTags.Skip {
            continue
        }

        // struct field and mongo
        var entity *Entity
        if len(index) == 0 && (structField.Anonymous || (structField.Type.Kind() == reflect.Struct && structTags.Inline)) {

            entity, err = newEntityByModelType(structField.Type, index)
            if err != nil {
                return nil, nil, err
            }

            // get all fields
            allFields = append(fields, entity.Fields...)
        }

        field := newField(structField, structTags, entity, nil)
        fields = append(fields, field)
    }

    return fields, allFields, nil
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

        field.setupValuerAndSetter()
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
    if th.PrioritizedPrimaryField != nil {
        return th.PrioritizedPrimaryField.DBName
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
            return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
        }
        return nil, fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
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
