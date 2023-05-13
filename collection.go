package jmgo

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wsk-go/jmgo/entity"
	"github.com/wsk-go/jmgo/errortype"
	filterPkg "github.com/wsk-go/jmgo/filter"
	"github.com/wsk-go/jmgo/internal/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"
)

type Collection[MODEL any, ID any] struct {
	schema          *entity.Entity
	collection      *mongo.Collection
	lastResumeToken bson.Raw
	client          *Client
}

func NewCollection[MODEL any, ID any](model MODEL, database *Database, opts ...*options.CollectionOptions) *Collection[MODEL, ID] {
	schema, err := entity.GetOrParse(model)
	if err != nil {
		panic(err)
	}
	col := database.db.Collection(schema.Collection, opts...)

	return &Collection[MODEL, ID]{
		collection: col,
		schema:     schema,
		client:     database.client,
	}
}

func (th *Collection[MODEL, ID]) Client() *Client {
	return th.client
}

func (th *Collection[MODEL, ID]) FindOneById(ctx context.Context, id ID, opts ...*options.FindOneOptions) (MODEL, error) {
	return th.FindOneByFilter(ctx, bson.M{th.schema.IdField.DBName: id}, opts...)
}

func (th *Collection[MODEL, ID]) IdExists(ctx context.Context, id ID) (bool, error) {
	c, err := th.Count(ctx, bson.M{th.schema.IdField.DBName: id})
	return c > 0, err
}

func (th *Collection[MODEL, ID]) IdsExistsNumber(ctx context.Context, ids []ID) (int64, error) {
	return th.Count(ctx, bson.M{th.schema.IdField.DBName: bson.M{"$in": ids}})
}

// FindOneByFilter find one by filter
func (th *Collection[MODEL, ID]) FindOneByFilter(ctx context.Context, filter any, opts ...*options.FindOneOptions) (MODEL, error) {

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

	GetCountTotal() bool
}

func (th *Collection[MODEL, ID]) FindPage(ctx context.Context, page Page, filter any, opts ...*options.FindOptions) ([]MODEL, int64, error) {
	opts = append(opts, options.Find().SetSkip(page.GetLength()).SetSkip(page.GetOffset()))
	return th.FindWithTotal(ctx, filter, page.GetCountTotal(), opts...)
}

// FindWithTotal get page
func (th *Collection[MODEL, ID]) FindWithTotal(ctx context.Context, filter any, countTotal bool, opts ...*options.FindOptions) ([]MODEL, int64, error) {

	convertedFilter, _, err := th.convertFilter(filter)
	if err != nil {
		return nil, 0, err
	}

	var total int64
	if countTotal {
		count, err := th.count(ctx, convertedFilter)
		if err != nil {
			return nil, 0, err
		}
		total = count
	}

	// 查询
	cursor, err := th.collection.Find(ctx, convertedFilter, opts...)

	if err != nil {
		return nil, 0, err
	}

	defer func() {
		_ = cursor.Close(ctx)
	}()
	var out []MODEL
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// Find filter type is any,you can use bson.M,bson.D...
func (th *Collection[MODEL, ID]) Find(ctx context.Context, filter any, opts ...*options.FindOptions) ([]MODEL, error) {

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

func (th *Collection[MODEL, ID]) mustConvertFilter(filter any) (any, error) {
	query, count, err := th.convertFilter(filter)

	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, errortype.ErrFilterNotContainAnyCondition
	}

	return query, nil
}

func (th *Collection[MODEL, ID]) convertFilter(filter any) (any, int, error) {

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
func (th *Collection[MODEL, ID]) fillToQuery(value reflect.Value, filterSchema *filterPkg.Filter, query bson.M) error {
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

func (th *Collection[MODEL, ID]) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {

	// handle
	var updateModels []any
	for _, model := range models {
		switch v := model.(type) {
		case *mongo.UpdateOneModel:
			updateModels = append(updateModels, v.Update)
			filter, err := th.mustConvertFilter(v.Filter)
			if err != nil {
				return nil, err
			}
			v.SetFilter(filter)

			err = th.tryCallBeforeUpdateHook(v.Update)
			if err != nil {
				return nil, err
			}

			doc, err := th.mapToUpdate(v.Update)
			if err != nil {
				return nil, err
			}
			v.SetUpdate(doc)
		case *mongo.UpdateManyModel:
			filter, err := th.mustConvertFilter(v.Filter)
			if err != nil {
				return nil, err
			}
			v.SetFilter(filter)

			err = th.tryCallBeforeUpdateHook(v.Update)
			if err != nil {
				return nil, err
			}

			doc, err := th.mapToUpdate(v.Update)
			if err != nil {
				return nil, err
			}
			v.SetUpdate(doc)
		case *mongo.DeleteOneModel:
			filter, err := th.mustConvertFilter(v.Filter)
			if err != nil {
				return nil, err
			}
			v.SetFilter(filter)
		case *mongo.DeleteManyModel:
			filter, err := th.mustConvertFilter(v.Filter)
			if err != nil {
				return nil, err
			}
			v.SetFilter(filter)
		case *mongo.ReplaceOneModel:
			filter, err := th.mustConvertFilter(v.Filter)
			if err != nil {
				return nil, err
			}
			v.SetFilter(filter)
		case *mongo.InsertOneModel:
			err := th.tryCallBeforeSaveHook(v.Document)
			if err != nil {
				return nil, err
			}
		}
	}

	// write models to mongodb
	result, err := th.collection.BulkWrite(ctx, models, opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// call hook for insert one and update one
	for i, model := range models {
		if insertion, ok := model.(*mongo.InsertOneModel); ok {
			th.tryCallAfterSaveHook(insertion.Document, result.UpsertedIDs[int64(i)])
		}
	}
	for _, model := range updateModels {
		th.tryCallAfterUpdateHook(model)
	}
	return result, nil
}

func (th *Collection[MODEL, ID]) NewUpdateOneModel(filter any, model MODEL) *mongo.UpdateOneModel {
	return mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(model)
}

func (th *Collection[MODEL, ID]) NewUpdateManyModel(filter any, model MODEL) *mongo.UpdateManyModel {
	return mongo.NewUpdateManyModel().SetFilter(filter).SetUpdate(model)
}

func (th *Collection[MODEL, ID]) NewInsertOneModel(model MODEL) *mongo.InsertOneModel {
	return mongo.NewInsertOneModel().SetDocument(model)
}

func (th *Collection[MODEL, ID]) NewDeleteOneModel(filter any) *mongo.DeleteOneModel {
	return mongo.NewDeleteOneModel().SetFilter(filter)
}

func (th *Collection[MODEL, ID]) NewDeleteManyModel(filter any) *mongo.DeleteManyModel {
	return mongo.NewDeleteManyModel().SetFilter(filter)
}

func (th *Collection[MODEL, ID]) Aggregate(ctx context.Context, pipeline any, results any, opts ...*options.AggregateOptions) error {
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

func (th *Collection[MODEL, ID]) Count(ctx context.Context, filter any, opts ...*options.CountOptions) (int64, error) {
	query, _, err := th.convertFilter(filter)
	if err != nil {
		return 0, err
	}
	return th.count(ctx, query, opts...)
}

func (th *Collection[MODEL, ID]) Exists(ctx context.Context, filter any, opts ...*options.CountOptions) (bool, error) {
	query, _, err := th.convertFilter(filter)
	if err != nil {
		return false, err
	}
	count, err := th.count(ctx, query, opts...)
	return count > 0, err
}

func (th *Collection[MODEL, ID]) count(ctx context.Context, filter any, opts ...*options.CountOptions) (int64, error) {
	//type Count struct {
	//	Count int64 `bson:"count"`
	//}

	//filter = bson.A{
	//	bson.M{
	//		"$match": filter,
	//	},
	//	bson.M{
	//		"$count": "count",
	//	},
	//}
	count, err := th.collection.CountDocuments(ctx, filter, opts...)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return count, nil
}

// 获取属性对应的schemaField
func (th *Collection[MODEL, ID]) mustSchemaField(fieldName string) (*entity.EntityField, error) {

	schemaField := th.schema.LookUpField(fieldName)

	if schemaField == nil {
		return nil, errors.WithStack(fmt.Errorf("fieldName name %s can not be found in %s", fieldName, th.schema.ModelType.Name()))
	}

	return schemaField, nil
}

// InsertOne inert one
func (th *Collection[MODEL, ID]) InsertOne(ctx context.Context, model MODEL, opts ...*options.InsertOneOptions) error {

	if err := th.tryCallBeforeSaveHook(model); err != nil {
		return err
	}

	result, err := th.collection.InsertOne(ctx, model, opts...)
	if err != nil {
		return err
	}

	th.tryCallAfterSaveHook(model, result.InsertedID)

	return nil
}

// InsertMany 创建一组内容
func (th *Collection[MODEL, ID]) InsertMany(ctx context.Context, models []MODEL, opts ...*options.InsertManyOptions) error {

	var ms = make([]any, 0, len(models))
	for _, model := range models {
		err := th.tryCallBeforeSaveHook(model)
		if err != nil {
			return err
		}
		ms = append(ms, model)
	}

	result, err := th.collection.InsertMany(ctx, ms, opts...)
	if err != nil {
		return err
	}

	for i, model := range models {
		th.tryCallAfterSaveHook(model, result.InsertedIDs[i])
	}

	return nil
}

func (th *Collection[MODEL, ID]) UpdateOneById(ctx context.Context, id ID, model MODEL, opts ...*options.UpdateOptions) (bool, error) {
	return th.UpdateOne(ctx, bson.M{th.schema.IdDBName(): id}, model, opts...)
}

func (th *Collection[MODEL, ID]) UpdateOne(ctx context.Context, filter any, model MODEL, opts ...*options.UpdateOptions) (bool, error) {

	result, err := th.doUpdate(ctx, filter, model, false, opts)
	if err != nil {
		return false, err
	}

	return result.ModifiedCount > 0, err
}

func (th *Collection[MODEL, ID]) UpdateMany(ctx context.Context, filter any, model MODEL, opts ...*options.UpdateOptions) (int64, error) {

	result, err := th.doUpdate(ctx, filter, model, true, opts)
	if err != nil {
		return 0, err
	}

	return result.ModifiedCount, err
}

func (th *Collection[MODEL, ID]) doUpdate(ctx context.Context, filter any, model any, multi bool, opts []*options.UpdateOptions) (*mongo.UpdateResult, error) {

	err := th.tryCallBeforeUpdateHook(model)
	if err != nil {
		return nil, err
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

	th.tryCallAfterUpdateHook(model)

	return result, nil
}

func (th *Collection[MODEL, ID]) mapToUpdate(model any) (bson.M, error) {
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

func (th *Collection[MODEL, ID]) FindAndModify(ctx context.Context, filter any, document any, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	return th.collection.FindOneAndUpdate(ctx, filter, document, opts...)
}

func (th *Collection[MODEL, ID]) DeleteOneById(ctx context.Context, id ID) (bool, error) {
	return th.DeleteOne(ctx, bson.M{th.schema.IdDBName(): id})
}
func (th *Collection[MODEL, ID]) DeleteOne(ctx context.Context, filter any) (bool, error) {

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

func (th *Collection[MODEL, ID]) Delete(ctx context.Context, filter any) (bool, error) {
	count, err := th.doDelete(ctx, filter, true)
	return count > 0, err
}

func (th *Collection[MODEL, ID]) doDelete(ctx context.Context, filter any, multi bool) (int64, error) {

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

func (th *Collection[MODEL, ID]) EnsureIndex(model *mongo.IndexModel) (string, error) {
	return th.collection.Indexes().CreateOne(context.Background(), *model)
}

// listen: 出错直接使用panic
func (th *Collection[MODEL, ID]) Watch(opts *options.ChangeStreamOptions, matchStage bson.D, listen func(stream *mongo.ChangeStream) error) {

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

func (th *Collection[MODEL, ID]) tryCallBeforeSaveHook(model any) error {
	if d, ok := model.(BeforeSave); ok {
		err := d.BeforeSave()
		if err != nil {
			return err
		}
		// 校验模型
		if err := th.validate(model); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (th *Collection[MODEL, ID]) validate(model any) error {
	if th.client.Validate == nil {
		return defaultValidate(model)
	} else {
		return th.client.Validate(model)
	}
}

func (th *Collection[MODEL, ID]) tryCallAfterSaveHook(model any, id any) {
	if d, ok := model.(AfterSave); ok {
		d.AfterSave(id)
	}
}

func (th *Collection[MODEL, ID]) tryCallBeforeUpdateHook(model any) error {
	if d, ok := model.(BeforeUpdate); ok {
		err := d.BeforeUpdate()
		if err != nil {
			return err
		}
	}
	return nil
}

func (th *Collection[MODEL, ID]) tryCallAfterUpdateHook(model any) {
	if d, ok := model.(AfterUpdate); ok {
		d.AfterUpdate()
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
//func (th *MustExecutor[MODEL, FILTER]) UpdateOne(ctx context.Context, filter any, model any) error {
//	ok, err := th.operator.UpdateOne(ctx, filter, model)
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
