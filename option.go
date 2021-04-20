package jmongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"jmongo/errortype"
	"jmongo/entity"
)

// 排序
type Sort struct {
	Field string
	Asc   bool
}

type FindOption struct {
	skip        int
	limit       int
	total       *int64
	includes    []string
	excludes    []string
	sorts       []*Sort
	findOneOpts []*options.FindOneOptions
	findOpts    []*options.FindOptions
}

func Option() *FindOption {
	return &FindOption{}
}

func (th *FindOption) Offset(offset int) *FindOption {
	th.skip = offset
	return th
}

func (th *FindOption) Limit(limit int) *FindOption {
	th.limit = limit
	return th
}

func (th *FindOption) WithTotal(total *int64) *FindOption {
	th.total = total
	return th
}

/// 要选择的属性，注意用模型定义的属性名字，而不是
func (th *FindOption) AddIncludes(includes ...string) *FindOption {
	th.includes = append(th.includes, includes...)
	return th
}

/// 不选择的属性
func (th *FindOption) AddExcludes(excludes ...string) *FindOption {
	th.excludes = append(th.excludes, excludes...)
	return th
}

// 排序
// - fieldName: 属性名字
// - asc: 是否从小到大排序
func (th *FindOption) AddOrder(fieldName string, asc bool) *FindOption {
	th.sorts = append(th.sorts, &Sort{
		Field: fieldName,
		Asc:   asc,
	})
	return th
}


// 复制options不存在的配置
// 如果options中有属性与当前配置冲突,则使用当前配置
func (th *FindOption) Merge(options []*FindOption) *FindOption {
	if len(options) == 0 {
		return th
	}
	return Merge(append(options, th))
}

/// 进行合成
func Merge(options []*FindOption) *FindOption {

	if options == nil || len(options) == 0 {
		return nil
	}

	if len(options) == 1 {
		return options[0]
	}

	current := Option()

	for _, o := range options {

		if o.skip > 0 {
			current.skip = o.skip
		}

		if o.limit > 0 {
			current.limit = o.limit
		}

		if o.total != nil {
			current.total = o.total
		}

		if o.excludes != nil {
			current.excludes = append(current.excludes, o.excludes...)
		}

		if o.includes != nil {
			current.includes = append(current.includes, o.includes...)
		}

		if o.sorts != nil {
			current.sorts = append(current.sorts, o.sorts...)
		}
	}

	return current
}

func (th *FindOption) makeFindOneOptions(schema *entity.Entity) ([]*options.FindOneOptions, error) {
	option := options.FindOne()

	// 设置偏移
	if th.skip > 0 {
		option.SetSkip(int64(th.skip))
	}

	// 设置projection
	projection, err := th.makeProjection(schema, th.includes, th.excludes)
	if err != nil {
		return nil, err
	}
	if len(projection) > 0 {
		option.SetProjection(projection)
	}

	// 设置sort
	sort, err := th.makeSort(schema, th.sorts)
	if err != nil {
		return nil, err
	}
	if len(sort) > 0 {
		option.SetSort(sort)
	}

	return []*options.FindOneOptions{option}, nil

}

func (th *FindOption) makeFindOption(schema *entity.Entity) ([]*options.FindOptions, error) {
	option := options.Find()

	// 设置偏移
	if th.skip > 0 {
		option.SetSkip(int64(th.skip))
	}

	// 设置偏移
	if th.limit > 0 {
		option.SetLimit(int64(th.limit))
	}

	// 设置projection
	projection, err := th.makeProjection(schema, th.includes, th.excludes)
	if err != nil {
		return nil, err
	}
	if len(projection) > 0 {
		option.SetProjection(projection)
	}

	// 设置sort
	sort, err := th.makeSort(schema, th.sorts)
	if err != nil {
		return nil, err
	}
	if len(sort) > 0 {
		option.SetSort(sort)
	}

	return []*options.FindOptions{option}, nil

}

func (th *FindOption) makeProjection(schema *entity.Entity, includes []string, excludes []string) (bson.D, error) {

	if len(includes) == 0 && len(excludes) == 0 {
		return nil, nil
	}

	var projection bson.D

	for _, include := range th.includes {
		field := schema.LookUpField(include)
		if field == nil {
			return nil, errortype.New(fmt.Sprintf("field %s not found in model %s", include, schema.Name))
		}

		projection = append(projection, primitive.E{
			Key:   field.DBName,
			Value: 1,
		})
	}

	for _, exclude := range th.excludes {
		field := schema.LookUpField(exclude)
		if field == nil {
			return nil, errortype.New(fmt.Sprintf("field %s not found in model %s", exclude, schema.Name))
		}

		projection = append(projection, primitive.E{
			Key:   field.DBName,
			Value: 0,
		})
	}
	return projection, nil
}

// 创建排序
func (th *FindOption) makeSort(schema *entity.Entity, sorts []*Sort) (bson.D, error) {

	var d bson.D = make([]primitive.E, len(sorts))
	for index, sort := range th.sorts {
		field := schema.LookUpField(sort.Field)
		if field == nil {
			return nil, errortype.New(fmt.Sprintf("field %s not found in model %s", sort.Field, schema.Name))
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
