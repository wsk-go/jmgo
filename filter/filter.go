package filter

import (
	"fmt"
	"github.com/JackWSK/jmongo/errortype"
	"github.com/pkg/errors"
	"reflect"
	"sync"
)

var cacheStore = &sync.Map{}

type Filter struct {
	Name      string
	ModelType reflect.Type
	Fields    []*FilterField
}

// get data type from dialector
func newFilter(dest any) (*Filter, error) {

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
	fields, err := extractFields(modelType, index)
	if err != nil {
		return nil, err
	}

	// entity
	entity.Name = modelType.Name()
	entity.ModelType = modelType
	entity.Fields = fields

	return entity, nil
}

func extractFields(modelType reflect.Type, index []int) (fields []*FilterField, err error) {

	// get field
	for i := 0; i < modelType.NumField(); i++ {
		// clone index
		cloneIndex := make([]int, len(index), len(index)+1)
		copy(cloneIndex, index)
		cloneIndex = append(cloneIndex, i)

		structField := modelType.Field(i)
		tag := structField.Tag.Get("bson")

		// parse to get bson info
		structTags, err := parseTags(structField.Name, tag)
		if err != nil {
			return nil, err
		}

		// filter skip field
		if structTags.Skip {
			continue
		}

		if structField.Anonymous {
			subFields, err := extractFields(structField.Type, cloneIndex)
			if err != nil {
				return nil, err
			}
			fields = append(fields, subFields...)
		} else {
			field, err := newField(structField, structTags, cloneIndex)
			if err != nil {
				return nil, err
			}

			fields = append(fields, field)
		}
	}

	return fields, nil
}

var mutex sync.Mutex

func GetOrParse(dest any) (entity *Filter, err error) {

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
