package jmongo

import (
	"context"
	"fmt"
	"github.com/JackWSK/jmongo/entity"
	"github.com/JackWSK/jmongo/errortype"
	filterPkg "github.com/JackWSK/jmongo/filter"
	"github.com/JackWSK/jmongo/utils"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"
)

type Collection[MODEL any] struct {
	schema          *entity.Entity
	collection      *mongo.Collection
	lastResumeToken bson.Raw
	client          *Client
}

func NewCollection[MODEL any](model MODEL, database *Database, opts ...*options.CollectionOptions) *Collection[MODEL] {
	schema, err := entity.GetOrParse(model)
	if err != nil {
		panic(err)
	}
	col := database.db.Collection(schema.Collection, opts...)

	return &Collection[MODEL]{
		collection: col,
		schema:     schema,
		client:     database.client,
	}
}

func (th *Collection[MODEL]) Client() *Client {
	return th.client
}

// FindOneByFilter find one by filter
func (th *Collection[MODEL]) FindOneByFilter(ctx context.Context, filter any, opts ...*options.FindOneOptions) (MODEL, error) {

	var out MODEL

	convertedFilter, _, err := th.convertFilter(filter)
	if err != nil {
		return out, err
	}

	// 查找
	one := th.collection.FindOne(ctx, convertedFilter, opts...)
	err = one.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return out, nil
		}
		return out, err
	}

	// 解析
	err = one.Decode(&out)
	if err != nil {
		return out, err
	}

	return out, nil
}

type Page interface {
	GetOffset() int64

	GetLength() int64

	GetWithTotal() *int64
}

func (th *Collection[MODEL]) FindPageByFilter(ctx context.Context, page Page, filter any, opts ...*options.FindOptions) ([]MODEL, error) {
	opts = append(opts, options.Find().SetSkip(page.GetLength()).SetSkip(page.GetOffset()))
	return th.FindByFilterWithTotal(ctx, filter, page.GetWithTotal(), opts...)
}

// FindByFilterWithTotal get page
func (th *Collection[MODEL]) FindByFilterWithTotal(ctx context.Context, filter any, total *int64, opts ...*options.FindOptions) ([]MODEL, error) {

	convertedFilter, _, err := th.convertFilter(filter)
	if err != nil {
		return nil, err
	}

	if total != nil {
		count, err := th.count(ctx, convertedFilter)
		if err != nil {
			return nil, err
		}
		*total = count
	}

	// 查询
	cursor, err := th.collection.Find(ctx, convertedFilter, opts...)

	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()
	var out []MODEL
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Find filter type is any,you can use bson.M,bson.D...
func (th *Collection[MODEL]) Find(ctx context.Context, filter any, opts ...*options.FindOptions) ([]MODEL, error) {

	convertedFilter, _, err := th.convertFilter(filter)
	if err != nil {
		return nil, err
	}

	// 查询
	cursor, err := th.collection.Find(ctx, convertedFilter, opts...)

	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()
	var out []MODEL
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (th *Collection[MODEL]) mustConvertFilter(filter any) (any, error) {
	query, count, err := th.convertFilter(filter)

	if err != nil {
		return NotIn{}, err
	}

	if count == 0 {
		return nil, errortype.ErrFilterNotContainAnyCondition
	}

	return query, nil
}

func (th *Collection[MODEL]) convertFilter(filter any) (any, int, error) {

	switch v := filter.(type) {
	// 原生M,直接返回
	case bson.M:
		return v, len(v), nil
		// 原生D,直接返回
	case bson.D:
		return v, len(v), nil
	}

	kind := reflect.Indirect(reflect.ValueOf(filter)).Kind()
	// regard as id if kind is not struct
	if kind != reflect.Struct {
		if kind == reflect.Slice || kind == reflect.Array {
			return bson.M{th.schema.IdDBName(): bson.M{"$in": utils.TryMapToObjectId(filter)}}, 0, nil
		} else {
			return bson.M{th.schema.IdDBName(): utils.TryMapToObjectId(filter)}, 0, nil
		}
	}

	filterSchema, err := filterPkg.GetOrParse(filter)
	if err != nil {
		return nil, 0, err
	}

	query := bson.M{}
	value := reflect.ValueOf(filter)
	err = th.fillToQuery(value, filterSchema, query)
	if err != nil {
		return nil, 0, err
	}

	return query, len(query), err
}

// begin iter all fields in filter
func (th *Collection[MODEL]) fillToQuery(value reflect.Value, filterSchema *filterPkg.Filter, query bson.M) error {
	for _, filterField := range filterSchema.Fields {
		fieldValue := filterField.ReflectValueOf(value)
		// continue if field value is zero
		if fieldValue.IsZero() {
			continue
		}

		entityField, err := th.mustSchemaField(filterField.RelativeFieldName)
		if err != nil {
			return err
		}
		object := fieldValue.Interface()
		// handle by the field itself
		if o, ok := object.(FilterOperator); ok {
			err := o.handle(entityField, filterField, query)
			if err != nil {
				return err
			}
		} else { // default handle
			fieldType := filterField.FieldType

			if fieldType.Kind() == reflect.Slice || fieldType.Kind() == reflect.Array {
				query[entityField.DBName] = bson.M{"$in": object}
			} else {
				query[entityField.DBName] = object
			}
		}
	}

	return nil
}
func (th *Collection[MODEL]) Aggregate(ctx context.Context, pipeline any, results any, opts ...*options.AggregateOptions) error {
	cursor, err := th.collection.Aggregate(ctx, pipeline, opts...)

	if err != nil {
		return err
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()

	err = cursor.All(ctx, results)

	return err
}

func (th *Collection[MODEL]) Count(ctx context.Context, filter any) (int64, error) {
	query, _, err := th.convertFilter(filter)
	if err != nil {
		return 0, err
	}
	return th.count(ctx, query)
}

func (th *Collection[MODEL]) Exists(ctx context.Context, filter any) (bool, error) {
	query, _, err := th.convertFilter(filter)
	if err != nil {
		return false, err
	}
	count, err := th.count(ctx, query)
	return count > 0, err
}

func (th *Collection[MODEL]) count(ctx context.Context, filter any, opts ...*options.AggregateOptions) (int64, error) {
	type Count struct {
		Count int64 `bson:"count"`
	}

	filter = bson.A{
		bson.M{
			"$match": filter,
		},
		bson.M{
			"$count": "count",
		},
	}
	cursor, err := th.collection.Aggregate(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()

	var results []*Count
	err = cursor.All(ctx, &results)
	if err != nil {
		return 0, err
	}

	if len(results) != 0 {
		return results[0].Count, err
	}
	return 0, nil
}

// 获取属性对应的schemaField
func (th *Collection[MODEL]) mustSchemaField(fieldName string) (*entity.EntityField, error) {

	schemaField := th.schema.LookUpField(fieldName)

	if schemaField == nil {
		return nil, errors.WithStack(fmt.Errorf("fieldName name %s can not be found in %s", fieldName, th.schema.ModelType.Name()))
	}

	return schemaField, nil
}

// InsertOne inert one
func (th *Collection[MODEL]) InsertOne(ctx context.Context, model MODEL, opts ...*options.InsertOneOptions) error {

	if d, ok := any(model).(BeforeSave); ok {
		d.BeforeSave()
		// 校验模型
		if err := Validate.Struct(model); err != nil {
			return errors.WithStack(err)
		}
	}

	result, err := th.collection.InsertOne(ctx, model, opts...)
	if err != nil {
		return err
	}

	if d, ok := any(model).(AfterSave); ok {
		d.AfterSave(result.InsertedID)
	}

	return nil
}

// InsertMany 创建一组内容
func (th *Collection[MODEL]) InsertMany(ctx context.Context, models []MODEL, opts ...*options.InsertManyOptions) error {

	var ms []any
	for _, model := range models {
		if d, ok := any(model).(BeforeSave); ok {
			d.BeforeSave()
		}
		// 校验模型
		if err := Validate.Struct(model); err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, model)
	}

	result, err := th.collection.InsertMany(ctx, ms, opts...)
	if err != nil {
		return err
	}

	for i, model := range models {
		if d, ok := any(model).(AfterSave); ok {
			l := len(result.InsertedIDs)
			if i < l {
				d.AfterSave(result.InsertedIDs[i])
			}
		}
	}

	return nil
}

// UpdateOneByFilter 返回参数: match 表示更新是否成功
func (th *Collection[MODEL]) UpdateOneByFilter(ctx context.Context, filter any, model MODEL, opts ...*options.UpdateOptions) (bool, error) {

	result, err := th.doUpdate(ctx, filter, model, false, opts)
	if err != nil {
		return false, err
	}

	return result.ModifiedCount > 0, err
}

func (th *Collection[MODEL]) UpdateMany(ctx context.Context, filter any, model MODEL, opts ...*options.UpdateOptions) (int64, error) {

	result, err := th.doUpdate(ctx, filter, model, true, opts)
	if err != nil {
		return 0, err
	}

	return result.ModifiedCount, err
}

func (th *Collection[MODEL]) doUpdate(ctx context.Context, filter any, model any, multi bool, opts []*options.UpdateOptions) (*mongo.UpdateResult, error) {

	if d, ok := model.(BeforeUpdate); ok {
		d.BeforeUpdate()
	}

	query, count, err := th.convertFilter(filter)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, errors.WithStack(errortype.ErrFilterNotContainAnyCondition)
	}

	update, err := th.mapToUpdate(model)
	if err != nil {
		return nil, err
	}

	var result *mongo.UpdateResult

	if multi {
		result, err = th.collection.UpdateMany(ctx, query, update, opts...)
		if err != nil {
			return nil, err
		}
	} else {
		result, err = th.collection.UpdateOne(ctx, query, update, opts...)
		if err != nil {
			return nil, err
		}
	}

	if d, ok := model.(AfterUpdate); ok {
		d.AfterUpdate()
	}

	return result, nil
}

func (th *Collection[MODEL]) mapToUpdate(model any) (bson.M, error) {
	value := reflect.ValueOf(model)

	update := bson.M{}
	for _, field := range th.schema.Fields {
		object, zero := field.ValueOf(value)
		// continue if field value is zero
		if zero {
			continue
		}
		// handle by the field itself
		update[field.DBName] = object
	}

	return bson.M{
		"$set": update,
	}, nil
}

func (th *Collection[MODEL]) FindAndModify(ctx context.Context, filter any, document any, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	return th.collection.FindOneAndUpdate(ctx, filter, document, opts...)
}

func (th *Collection[MODEL]) DeleteOne(ctx context.Context, filter any) (bool, error) {

	query, count, err := th.convertFilter(filter)
	if err != nil {
		return false, err
	}

	if count == 0 {
		return false, errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
	}

	result, err := th.collection.DeleteOne(ctx, query)
	if err != nil {
		return false, err
	}
	return result.DeletedCount > 0, nil
}

func (th *Collection[MODEL]) Delete(ctx context.Context, filter any) (bool, error) {
	count, err := th.doDelete(ctx, filter, true)
	return count > 0, err
}

func (th *Collection[MODEL]) doDelete(ctx context.Context, filter any, multi bool) (int64, error) {

	query, count, err := th.convertFilter(filter)
	if err != nil {
		return 0, err
	}

	if count == 0 {
		return 0, errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
	}

	var result *mongo.DeleteResult
	if multi {
		result, err = th.collection.DeleteMany(ctx, query)
	} else {
		result, err = th.collection.DeleteOne(ctx, query)
	}

	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

func (th *Collection[MODEL]) EnsureIndex(model *mongo.IndexModel) (string, error) {
	return th.collection.Indexes().CreateOne(context.Background(), *model)
}

// listen: 出错直接使用panic
func (th *Collection[MODEL]) Watch(opts *options.ChangeStreamOptions, matchStage bson.D, listen func(stream *mongo.ChangeStream) error) {

	for {
		time.After(1 * time.Second)
		func() {
			defer func() {
				err := recover()
				if err != nil {
					if DefaultLogger != nil {
						DefaultLogger.Error(fmt.Sprintf("同步出现异常: %+v", err))
					}
				}
			}()

			// 设置恢复点
			if th.lastResumeToken != nil {
				opts.SetResumeAfter(th.lastResumeToken)
				opts.SetStartAtOperationTime(nil)
				opts.SetStartAfter(nil)
			}

			changeStream, err := th.collection.Watch(context.TODO(), mongo.Pipeline{matchStage}, opts)
			if err != nil {
				panic(err)
			}

			defer func() {
				_ = changeStream.Close(context.TODO())
			}()

			// 错误需要重新恢复
			for true {
				if changeStream.Next(context.TODO()) {
					if changeStream.Err() == nil {

						err := listen(changeStream)
						if err != nil {
							panic(err)
						} else {
							th.lastResumeToken = changeStream.ResumeToken()
						}
					}
				}
			}
		}()
	}
}

//func (th *Collection[MODEL, FILTER]) Must(failFunc func() error) *MustExecutor[MODEL, FILTER] {
//	return &MustExecutor[MODEL, FILTER]{
//		operator:       th,
//		notExistsHandler: failFunc,
//	}
//}

//type MustExecutor[MODEL any, FILTER any] struct {
//	operator *Collection[MODEL, FILTER]
//	// 不存在的时候的的自定义异常
//	notExistsHandler func() error
//}
//
//// FindOne 当数据不存在，回调FailError方法
//func (th *MustExecutor[MODEL, FILTER]) FindOne(ctx context.Context, filter any, out any, options ...*FindOption) error {
//
//	ok, err := th.operator.FindOneByFilter(ctx, filter, out, options...)
//
//	if err != nil {
//		return err
//	}
//
//	if !ok {
//		return th.notExistsHandler()
//	}
//
//	return nil
//}
//
//// Exists 当数据不存在，回调FailError方法
//func (th *MustExecutor[MODEL, FILTER]) Exists(ctx context.Context, filter any) error {
//	ok, err := th.operator.Exists(ctx, filter)
//	if err != nil {
//		return err
//	}
//
//	if !ok {
//		return th.notExistsHandler()
//	}
//
//	return nil
//}
//
//func (th *MustExecutor[MODEL, FILTER]) UpdateOneByFilter(ctx context.Context, filter any, model any) error {
//	ok, err := th.operator.UpdateOneByFilter(ctx, filter, model)
//	if err != nil {
//		return err
//	}
//
//	if !ok {
//		return th.notExistsHandler()
//	}
//
//	return nil
//}
//
//// DeleteOne 根据filter来更新一个
//func (th *MustExecutor[MODEL, FILTER]) DeleteOne(ctx context.Context, filter any) error {
//	ok, err := th.operator.DeleteOne(ctx, filter)
//
//	if err != nil {
//		return err
//	}
//
//	if !ok {
//		return th.notExistsHandler()
//	}
//
//	return nil
//}
//
//// Delete 根据filter来更新一个
//func (th *MustExecutor[MODEL, FILTER]) Delete(ctx context.Context, filter interface{}) error {
//
//	ok, err := th.operator.Delete(ctx, filter)
//
//	if err != nil {
//		return err
//	}
//
//	if !ok {
//		return th.notExistsHandler()
//	}
//
//	return nil
//}
