package jmongo

import (
    "context"
    "fmt"
    "github.com/pkg/errors"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "jmongo/entity"
    "jmongo/errortype"
    filterPkg "jmongo/filter"
    "reflect"
    "time"
)

type Collection struct {
    schema          *entity.Entity
    collection      *mongo.Collection
    lastResumeToken bson.Raw
}

func NewMongoCollection(collection *mongo.Collection, schema *entity.Entity) *Collection {
    return &Collection{collection: collection, schema: schema}
}

func (th *Collection) checkModel(out interface{}) error {
    modelType := entity.GetModelType(out)

    if !th.schema.ModelType.AssignableTo(modelType) {
        return errors.WithStack(errortype.ErrModelTypeNotMatchInCollection)
    }

    return nil
}

// 封装了一下mongo的查询方法
func (th *Collection) FindOne(ctx context.Context, filter interface{}, out interface{}, opts ...*FindOption) (found bool, err error) {
    if err := th.checkModel(out); err != nil {
        return false, err
    }

    if filter == nil {
        filter = bson.M{}
    }

    filter, _, err = th.convertFilter(filter)
    if err != nil {
        return false, err
    }

    // 转化成mongo的配置选项
    var mongoOpts []*options.FindOneOptions
    if len(opts) > 0 {
        mongoOpts, err = Merge(opts).makeFindOneOptions(th.schema)
        if err != nil {
            return false, errors.WithStack(err)
        }
    }

    // 查找
    one := th.collection.FindOne(ctx, filter, mongoOpts...)
    err = one.Err()
    if err != nil {
        if err == mongo.ErrNoDocuments {
            return false, nil
        }
        return false, err
    }

    // 解析
    err = one.Decode(out)
    if err != nil {
        return false, err
    }

    return true, nil
}

// cond: if value is not bson.M or bson.D or struct, is value will be used as id
func (th *Collection) Find(ctx context.Context, filter interface{}, out interface{}, opts ...*FindOption) error {

    if err := th.checkModel(out); err != nil {
        return err
    }

    if filter == nil {
        return errors.New("value of filter can not be nil")
    }

    filter, _, err := th.convertFilter(filter)
    if err != nil {
        return err
    }

    var mongoOpts []*options.FindOptions
    if len(opts) > 0 {
        opt := Merge(opts)
        mongoOpts, err = opt.makeFindOption(th.schema)
        if err != nil {
            return err
        }

        if opt.total != nil {
            count, err := th.count(ctx, filter)
            if err != nil {
                return err
            }
            *opt.total = count
        }
    }

    // 查询
    cursor, err := th.collection.Find(ctx, filter, mongoOpts...)

    if err != nil {
        return err
    }

    defer func() {
        _ = cursor.Close(ctx)
    }()

    err = cursor.All(ctx, out)
    if err != nil {
        return err
    }

    return nil
}

func (th *Collection) mustConvertFilter(filter interface{}) (interface{}, error) {
    query, count, err := th.convertFilter(filter)

    if err != nil {
        return NotIn{}, err
    }

    if count == 0 {
        return nil, errortype.ErrFilterNotContainAnyCondition
    }

    return query, nil
}

func (th *Collection) convertFilter(filter interface{}) (interface{}, int, error) {

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
        return bson.M{th.schema.IdDBName(): filter}, 0, nil
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
func (th *Collection) fillToQuery(value reflect.Value, filterSchema *filterPkg.Filter, query bson.M) error {
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
            query[entityField.DBName] = object
        }
    }

    return nil
}
func (th *Collection) Aggregate(ctx context.Context, pipeline interface{}, results interface{}, opts ...*options.AggregateOptions) error {
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

func (th *Collection) count(ctx context.Context, filter interface{}, opts ...*options.AggregateOptions) (int64, error) {
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
func (th *Collection) mustSchemaField(fieldName string) (*entity.EntityField, error) {

    schemaField := th.schema.LookUpField(fieldName)

    if schemaField == nil {
        return nil, errors.WithStack(fmt.Errorf("fieldName name %s can not be found in %s", fieldName, th.schema.ModelType.Name()))
    }

    return schemaField, nil
}

// 创建单个元素
func (th *Collection) InsertOne(ctx context.Context, model interface{}, opts ...*options.InsertOneOptions) error {

    if err := th.checkModel(model); err != nil {
        return err
    }

    if d, ok := model.(BeforeSave); ok {
        d.BeforeSave()
    }

    result, err := th.collection.InsertOne(ctx, model, opts...)
    if err != nil {
        return err
    }

    if d, ok := model.(AfterSave); ok {
        d.AfterSave(result.InsertedID)
    }

    return nil
}

// 创建一组内容
func (th *Collection) InsertMany(ctx context.Context, models []interface{}, opts ...*options.InsertManyOptions) error {

    if err := th.checkModel(models); err != nil {
        return err
    }

    for _, model := range models {
        if d, ok := model.(BeforeSave); ok {
            d.BeforeSave()
        }
    }

    result, err := th.collection.InsertMany(ctx, models, opts...)
    if err != nil {
        return err
    }

    for i, model := range models {
        if d, ok := model.(AfterSave); ok {
            l := len(result.InsertedIDs)
            if i < l {
                d.AfterSave(result.InsertedIDs[i])
            }
        }
    }

    return nil
}

// 返回参数: match 表示更新是否成功
func (th *Collection) UpdateOne(ctx context.Context, filter interface{}, model interface{}, opts ...*options.UpdateOptions) (bool, error) {

    result, err := th.doUpdate(ctx, filter, model, false, opts)
    if err != nil {
        return false, err
    }

    return result.ModifiedCount > 0, err
}

func (th *Collection) UpdateMany(ctx context.Context, filter interface{}, model interface{}, opts ...*options.UpdateOptions) (int64, error) {

    result, err := th.doUpdate(ctx, filter, model, true, opts)
    if err != nil {
        return 0, err
    }

    return result.ModifiedCount, err
}

func (th *Collection) doUpdate(ctx context.Context, filter interface{}, model interface{}, multi bool, opts []*options.UpdateOptions) (*mongo.UpdateResult, error) {

    if err := th.checkModel(model); err != nil {
        return nil, err
    }

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

func (th *Collection) mapToUpdate(model interface{}) (bson.M, error) {
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

func (th *Collection) FindAndModify(ctx context.Context, filter interface{}, document interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
    return th.collection.FindOneAndUpdate(ctx, filter, document, opts...)
}

func (th *Collection) EnsureIndex(model *mongo.IndexModel) (string, error) {
    return th.collection.Indexes().CreateOne(context.Background(), *model)
}

// listen: 出错直接使用panic
func (th *Collection) Watch(opts *options.ChangeStreamOptions, matchStage bson.D, listen func(stream *mongo.ChangeStream) error) {

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
