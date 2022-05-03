package collection

import (
	"code.aliyun.com/jgo/jmongo/entity"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func makeFindOneOptions(schema *entity.Entity, filter IFilter) (*options.FindOneOptions, error) {
	option := options.FindOne()

	// 设置偏移
	if filter.Skip() > 0 {
		option.SetSkip(int64(filter.Skip()))
	}

	// 设置projection
	projection, err := makeProjection(schema, filter.Includes(), filter.Excludes())
	if err != nil {
		return nil, err
	}
	if len(projection) > 0 {
		option.SetProjection(projection)
	}

	// 设置sort
	sort, err := makeSort(schema, filter.Sorts())
	if err != nil {
		return nil, err
	}
	if len(sort) > 0 {
		option.SetSort(sort)
	}

	return option, nil

}

func makeFindOption(schema *entity.Entity, filter IFilter) ([]*options.FindOptions, error) {
	option := options.Find()

	// 设置偏移
	if filter.Skip() > 0 {
		option.SetSkip(int64(filter.Skip()))
	}

	// 设置偏移
	if filter.Limit() > 0 {
		option.SetLimit(int64(filter.Limit()))
	}

	// 设置projection
	projection, err := makeProjection(schema, filter.Includes(), filter.Excludes())
	if err != nil {
		return nil, err
	}

	if len(projection) > 0 {
		option.SetProjection(projection)
	}

	// 设置sort
	sort, err := makeSort(schema, filter.Sorts())
	if err != nil {
		return nil, err
	}
	if len(sort) > 0 {
		option.SetSort(sort)
	}

	return []*options.FindOptions{option}, nil

}

func makeProjection(schema *entity.Entity, includes []string, excludes []string) (bson.D, error) {

	if len(includes) == 0 && len(excludes) == 0 {
		return nil, nil
	}

	var projection bson.D

	for _, include := range includes {
		field := schema.LookUpField(include)
		if field == nil {
			return nil, errors.New(fmt.Sprintf("field %s not found in model %s", include, schema.Name))
		}

		projection = append(projection, primitive.E{
			Key:   field.DBName,
			Value: 1,
		})
	}

	for _, exclude := range excludes {
		field := schema.LookUpField(exclude)
		if field == nil {
			return nil, errors.New(fmt.Sprintf("field %s not found in model %s", exclude, schema.Name))
		}

		projection = append(projection, primitive.E{
			Key:   field.DBName,
			Value: 0,
		})
	}
	return projection, nil
}

// 创建排序
func makeSort(schema *entity.Entity, sorts []*Sort) (bson.D, error) {

	var d bson.D = make([]primitive.E, len(sorts))
	for index, sort := range sorts {
		field := schema.LookUpField(sort.Field)
		if field == nil {
			return nil, errors.New(fmt.Sprintf("field %s not found in model %s", sort.Field, schema.Name))
		}
		var asc = 1
		if !sort.Asc {
			asc = -1
		}
		d[index] = primitive.E{
			Key:   field.DBName,
			Value: asc,
		}
	}

	return d, nil
}
