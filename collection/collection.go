package collection

import (
	"code.aliyun.com/jgo/jmongo/entity"
	filterPkg "code.aliyun.com/jgo/jmongo/filter"
	"code.aliyun.com/jgo/jmongo/utils"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"sync"
)

var cache sync.Map

type Collection[MODEL any, FILTER IFilter] struct {
	schema          *entity.Entity
	collection      *mongo.Collection
	lastResumeToken bson.Raw
}

// New a collection
func New[MODEL any, FILTER IFilter](db *mongo.Database, model MODEL) *Collection[MODEL, FILTER] {
	schema, err := entity.GetOrParse(model)
	if err != nil {
		panic(err)
	}

	// try getting from cache
	if v, ok := cache.Load(schema.ModelType); ok {
		return v.(*Collection[MODEL, FILTER])
	}

	col := db.Collection(schema.Collection)
	collection := &Collection[MODEL, FILTER]{collection: col, schema: schema}
	cache.Store(schema.ModelType, collection)
	return collection
}

//func (th *Collection) checkModel(out any) error {
//	modelType := entity.GetModelType(out)
//
//	if !th.schema.ModelType.AssignableTo(modelType) {
//		return errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
//	}
//
//	return nil
//}

// FindOneByFilter FindOne 封装了一下mongo的查询方法
func (th *Collection[MODEL, FILTER]) FindOneByFilter(ctx context.Context, filter FILTER) (model MODEL, err error) {

	query, _, err := th.convertFilter(filter)
	if err != nil {
		return model, err
	}

	opt, err := makeFindOneOptions(th.schema, filter)
	if err != nil {
		return model, err
	}

	// 查找
	one := th.collection.FindOne(ctx, query, opt)
	err = one.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return model, nil
		}
		return model, err
	}

	// 解析
	m := reflect.New(th.schema.ModelType).Interface()
	err = one.Decode(m)
	if err != nil {
		return model, err
	}

	mm, _ := MODEL.(m)
	return mm, nil
}

//
//// cond: if value is not bson.M or bson.D or struct, is value will be used as id
//func (th *Collection) Find(ctx context.Context, filter any, out any, opts ...*FindOption) error {
//
//	if err := th.checkModel(out); err != nil {
//		return err
//	}
//
//	if filter == nil {
//		return errors.New("value of filter can not be nil")
//	}
//
//	filter, _, err := th.convertFilter(filter)
//	if err != nil {
//		return err
//	}
//
//	var mongoOpts []*options.FindOptions
//	if len(opts) > 0 {
//		opt := Merge(opts)
//		mongoOpts, err = opt.makeFindOption(th.schema)
//		if err != nil {
//			return err
//		}
//
//		if opt.total != nil {
//			count, err := th.count(ctx, filter)
//			if err != nil {
//				return err
//			}
//			*opt.total = count
//		}
//	}
//
//	// 查询
//	cursor, err := th.collection.Find(ctx, filter, mongoOpts...)
//
//	if err != nil {
//		return err
//	}
//
//	defer func() {
//		_ = cursor.Close(ctx)
//	}()
//
//	err = cursor.All(ctx, out)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (th *Collection) mustConvertFilter(filter any) (any, error) {
//	query, count, err := th.convertFilter(filter)
//
//	if err != nil {
//		return NotIn{}, err
//	}
//
//	if count == 0 {
//		return nil, errortype.ErrFilterNotContainAnyCondition
//	}
//
//	return query, nil
//}

func (th *Collection[MODEL, FILTER]) convertFilter(filter FILTER) (any, int, error) {

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
func (th *Collection[MODEL, FILTER]) fillToQuery(value reflect.Value, filterSchema *filterPkg.Filter, query bson.M) error {
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

//func (th *Collection) Aggregate(ctx context.Context, pipeline any, results any, opts ...*options.AggregateOptions) error {
//	cursor, err := th.collection.Aggregate(ctx, pipeline, opts...)
//
//	if err != nil {
//		return err
//	}
//
//	defer func() {
//		_ = cursor.Close(ctx)
//	}()
//
//	err = cursor.All(ctx, results)
//
//	return err
//}
//
//func (th *Collection) Count(ctx context.Context, filter any) (int64, error) {
//	query, _, err := th.convertFilter(filter)
//	if err != nil {
//		return 0, err
//	}
//	return th.count(ctx, query)
//}
//
//func (th *Collection) Exists(ctx context.Context, filter any) (bool, error) {
//	query, _, err := th.convertFilter(filter)
//	if err != nil {
//		return false, err
//	}
//	count, err := th.count(ctx, query)
//	return count > 0, err
//}
//
//func (th *Collection) count(ctx context.Context, filter any, opts ...*options.AggregateOptions) (int64, error) {
//	type Count struct {
//		Count int64 `bson:"count"`
//	}
//
//	filter = bson.A{
//		bson.M{
//			"$match": filter,
//		},
//		bson.M{
//			"$count": "count",
//		},
//	}
//	cursor, err := th.collection.Aggregate(ctx, filter, opts...)
//	if err != nil {
//		return 0, err
//	}
//
//	defer func() {
//		_ = cursor.Close(ctx)
//	}()
//
//	var results []*Count
//	err = cursor.All(ctx, &results)
//	if err != nil {
//		return 0, err
//	}
//
//	if len(results) != 0 {
//		return results[0].Count, err
//	}
//	return 0, nil
//}
//
// 获取属性对应的schemaField
func (th *Collection) mustSchemaField(fieldName string) (*entity.EntityField, error) {

	schemaField := th.schema.LookUpField(fieldName)

	if schemaField == nil {
		return nil, errors.WithStack(fmt.Errorf("fieldName name %s can not be found in %s", fieldName, th.schema.ModelType.Name()))
	}

	return schemaField, nil
}

//
//// 创建单个元素
//func (th *Collection) InsertOne(ctx context.Context, model any, opts ...*options.InsertOneOptions) error {
//
//	if err := th.checkModel(model); err != nil {
//		return err
//	}
//
//	if d, ok := model.(BeforeSave); ok {
//		d.BeforeSave()
//	}
//
//	result, err := th.collection.InsertOne(ctx, model, opts...)
//	if err != nil {
//		return err
//	}
//
//	if d, ok := model.(AfterSave); ok {
//		d.AfterSave(result.InsertedID)
//	}
//
//	return nil
//}
//
//// 创建一组内容
//func (th *Collection) InsertMany(ctx context.Context, models []any, opts ...*options.InsertManyOptions) error {
//
//	if err := th.checkModel(models); err != nil {
//		return err
//	}
//
//	for _, model := range models {
//		if d, ok := model.(BeforeSave); ok {
//			d.BeforeSave()
//		}
//	}
//
//	result, err := th.collection.InsertMany(ctx, models, opts...)
//	if err != nil {
//		return err
//	}
//
//	for i, model := range models {
//		if d, ok := model.(AfterSave); ok {
//			l := len(result.InsertedIDs)
//			if i < l {
//				d.AfterSave(result.InsertedIDs[i])
//			}
//		}
//	}
//
//	return nil
//}
//
//// 返回参数: match 表示更新是否成功
//func (th *Collection) UpdateOne(ctx context.Context, filter any, model any, opts ...*options.UpdateOptions) (bool, error) {
//
//	result, err := th.doUpdate(ctx, filter, model, false, opts)
//	if err != nil {
//		return false, err
//	}
//
//	return result.ModifiedCount > 0, err
//}
//
//func (th *Collection) UpdateMany(ctx context.Context, filter any, model any, opts ...*options.UpdateOptions) (int64, error) {
//
//	result, err := th.doUpdate(ctx, filter, model, true, opts)
//	if err != nil {
//		return 0, err
//	}
//
//	return result.ModifiedCount, err
//}
//
//func (th *Collection) doUpdate(ctx context.Context, filter any, model any, multi bool, opts []*options.UpdateOptions) (*mongo.UpdateResult, error) {
//
//	if err := th.checkModel(model); err != nil {
//		return nil, err
//	}
//
//	if d, ok := model.(BeforeUpdate); ok {
//		d.BeforeUpdate()
//	}
//
//	query, count, err := th.convertFilter(filter)
//	if err != nil {
//		return nil, err
//	}
//
//	if count == 0 {
//		return nil, errors.WithStack(errortype.ErrFilterNotContainAnyCondition)
//	}
//
//	update, err := th.mapToUpdate(model)
//	if err != nil {
//		return nil, err
//	}
//
//	var result *mongo.UpdateResult
//
//	if multi {
//		result, err = th.collection.UpdateMany(ctx, query, update, opts...)
//		if err != nil {
//			return nil, err
//		}
//	} else {
//		result, err = th.collection.UpdateOne(ctx, query, update, opts...)
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	if d, ok := model.(AfterUpdate); ok {
//		d.AfterUpdate()
//	}
//
//	return result, nil
//}
//
//func (th *Collection) mapToUpdate(model any) (bson.M, error) {
//	value := reflect.ValueOf(model)
//
//	update := bson.M{}
//	for _, field := range th.schema.Fields {
//		object, zero := field.ValueOf(value)
//		// continue if field value is zero
//		if zero {
//			continue
//		}
//		// handle by the field itself
//		update[field.DBName] = object
//	}
//
//	return bson.M{
//		"$set": update,
//	}, nil
//}
//
//func (th *Collection) FindAndModify(ctx context.Context, filter any, document any, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
//	return th.collection.FindOneAndUpdate(ctx, filter, document, opts...)
//}
//
//func (th *Collection) DeleteOne(ctx context.Context, filter any) (bool, error) {
//
//	query, count, err := th.convertFilter(filter)
//	if err != nil {
//		return false, err
//	}
//
//	if count == 0 {
//		return false, errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
//	}
//
//	result, err := th.collection.DeleteOne(ctx, query)
//	if err != nil {
//		return false, err
//	}
//	return result.DeletedCount > 0, nil
//}
//
//func (th *Collection) Delete(ctx context.Context, filter any) (bool, error) {
//	count, err := th.doDelete(ctx, filter, true)
//	return count > 0, err
//}
//
//func (th *Collection) doDelete(ctx context.Context, filter any, multi bool) (int64, error) {
//
//	query, count, err := th.convertFilter(filter)
//	if err != nil {
//		return 0, err
//	}
//
//	if count == 0 {
//		return 0, errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
//	}
//
//	var result *mongo.DeleteResult
//	if multi {
//		result, err = th.collection.DeleteMany(ctx, query)
//	} else {
//		result, err = th.collection.DeleteOne(ctx, query)
//	}
//
//	if err != nil {
//		return 0, err
//	}
//
//	return result.DeletedCount, nil
//}
//
//func (th *Collection) EnsureIndex(model *mongo.IndexModel) (string, error) {
//	return th.collection.Indexes().CreateOne(context.Background(), *model)
//}
//
//// listen: 出错直接使用panic
//func (th *Collection) Watch(opts *options.ChangeStreamOptions, matchStage bson.D, listen func(stream *mongo.ChangeStream) error) {
//
//	for {
//		time.After(1 * time.Second)
//		func() {
//			defer func() {
//				err := recover()
//				if err != nil {
//					if DefaultLogger != nil {
//						DefaultLogger.Error(fmt.Sprintf("同步出现异常: %+v", err))
//					}
//				}
//			}()
//
//			// 设置恢复点
//			if th.lastResumeToken != nil {
//				opts.SetResumeAfter(th.lastResumeToken)
//				opts.SetStartAtOperationTime(nil)
//				opts.SetStartAfter(nil)
//			}
//
//			changeStream, err := th.collection.Watch(context.TODO(), mongo.Pipeline{matchStage}, opts)
//			if err != nil {
//				panic(err)
//			}
//
//			defer func() {
//				_ = changeStream.Close(context.TODO())
//			}()
//
//			// 错误需要重新恢复
//			for true {
//				if changeStream.Next(context.TODO()) {
//					if changeStream.Err() == nil {
//
//						err := listen(changeStream)
//						if err != nil {
//							panic(err)
//						} else {
//							th.lastResumeToken = changeStream.ResumeToken()
//						}
//					}
//				}
//			}
//		}()
//	}
//}
//
//func (th *Collection) Must(failFunc func() error) *MustExecutor {
//	return &MustExecutor{
//		collection:       th,
//		notExistsHandler: failFunc,
//	}
//}
//
//type MustExecutor struct {
//	collection *Collection
//	// 不存在的时候的的自定义异常
//	notExistsHandler func() error
//}
//
//// 当数据不存在，回调FailError方法
//func (th *MustExecutor) FindOne(ctx context.Context, filter any, out any, options ...*FindOption) error {
//
//	ok, err := th.collection.FindOne(ctx, filter, out, options...)
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
//// 当数据不存在，回调FailError方法
//func (th *MustExecutor) Exists(ctx context.Context, filter any) error {
//	ok, err := th.collection.Exists(ctx, filter)
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
//func (th *MustExecutor) UpdateOne(ctx context.Context, filter any, model any) error {
//	ok, err := th.collection.UpdateOne(ctx, filter, model)
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
//// 根据filter来更新一个
//func (th *MustExecutor) DeleteOne(ctx context.Context, filter any) error {
//	ok, err := th.collection.DeleteOne(ctx, filter)
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
//// 根据filter来更新一个
//func (th *MustExecutor) Delete(ctx context.Context, filter any) error {
//
//	ok, err := th.collection.Delete(ctx, filter)
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
