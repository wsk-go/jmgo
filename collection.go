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
    "jmongo/utils"
    filterPkg "jmongo/filter"
    "reflect"
    "time"
)

type Collection struct {
    collection      *mongo.Collection
    lastResumeToken bson.Raw
}

func NewMongoCollection(collection *mongo.Collection) *Collection {
    return &Collection{collection: collection}
}

// cond: if value is not bson.M or bson.D or struct, is value will be used as id
func (th *Collection) Find(ctx context.Context, filter interface{}, out interface{}, opts ...*FindOption) error {

    if filter == nil {
        return errors.New("value of filter can not be nil")
    }


    schema, err := entity.GetOrParse(out)
    if err != nil {
        return err
    }

    filter, _, err = th.convertFilter(schema, filter)
    if err != nil {
        return err
    }

    var mongoOpts []*options.FindOptions
    if len(opts) > 0 {
        opt := Merge(opts)
        mongoOpts, err = opt.makeFindOption(schema)
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

// 封装了一下mongo的查询方法
func (th *Collection) FindOne(ctx context.Context, filter interface{}, out interface{}, opts ...*FindOption) (ok bool, err error) {
    if filter == nil {
        filter = bson.M{}
    }

    // 获取schema
    _schema, err := entity.GetOrParse(out)
    if err != nil {
        return false, err
    }

    // 转化成mongo的配置选项
    var mongoOpts []*options.FindOneOptions
    if len(opts) > 0 {
        mongoOpts, err = Merge(opts).makeFindOneOptions(_schema)
        if err != nil {
            return false, err
        }
    }

    // 查找
    one := th.collection.FindOne(ctx, filter, mongoOpts...)
    err = one.Err()
    if err != nil {
        return false, err
    }

    // 解析
    err = one.Decode(out)
    if err != nil {
        if err == mongo.ErrNoDocuments {
            return false, nil
        }

        return false, err
    }

    return true, nil
}

func (th *Collection) mustConvertFilter(schema *entity.Entity, filter interface{}) (interface{}, error) {
    query, count, err := th.convertFilter(schema, filter)

    if err != nil {
        return NotIn{}, err
    }

    if count == 0 {
        return nil, errortype.ErrFilterNotContainAnyCondition
    }

    return query, nil
}

func (th *Collection) convertFilter(schema *entity.Entity, filter interface{}) (interface{}, int, error) {

    switch filter.(type) {
    // 原生M,直接返回
    case bson.M:
        return filter, len(filter.(bson.M)), nil
        // 原生D,直接返回
    case bson.D:
        return filter, len(filter.(bson.D)), nil
    }

    kind := reflect.Indirect(reflect.ValueOf(filter)).Kind()
    // 如果不是struct类型,直接当作id处理
    if kind != reflect.Struct {
        return bson.M{schema.PrimaryKeyDBName(): filter}, 1, nil
    }

    count := 0
    query := bson.M{}


    filterSchema, err := filterPkg.GetOrParse(filter)
    if err != nil {
        return nil, 0, err
    }

    err := th.iterStructNonNilColumn(schema, filter, func(column string, fieldValue reflect.Value, field reflect.StructField) {
        object := fieldValue.Interface()
        // handle by the field itself
        if o, ok := object.(FilterOperator); ok {
            o.handle(&FilterField{DBName: column, Field: field}, query)

        } else { // default handle
            query[column] = object
        }
        count++
    })

    return query, count, err
}

// begin iter all fields in filter
func (th *Collection) doConvertFilter(value reflect.Value, filterSchema *filterPkg.Filter, entitySchema *entity.Entity, query bson.M) error {
    for _, filterField := range filterSchema.Fields {
        fieldValue := filterField.ReflectValueOf(value)
        if filterField.Entity != nil {
            err := th.doConvertFilter(fieldValue, filterSchema, entitySchema, query)
            if err != nil {
                return err
            }
        } else {
            entityField, err := th.mustSchemaField(filterField.RelativeFieldName, entitySchema)
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
                query[column] = object
            }
            count++
        }
    }
}

func (th *Collection) iterStructNonNilColumn(schema *entity.Entity, model interface{}, consumer func(string, reflect.Value, reflect.StructField)) error {

    if model == nil {
        return nil
    }

    // 获取value
    modelValue := reflect.ValueOf(model)
    if modelValue.Kind() == reflect.Ptr {
        modelValue = modelValue.Elem()
    }

    // 获取type
    modelType := reflect.TypeOf(model)
    if modelType.Kind() == reflect.Ptr {
        modelType = modelType.Elem()
    }

    for i := 0; i < modelType.NumField(); i++ {
        field := modelType.Field(i)
        fieldValue := modelValue.Field(i)

        // 获取filter的字段对应的属性名字
        var fieldNameInModel string
        if fieldName, ok := field.Tag.Lookup("jfname"); ok {
            fieldNameInModel = fieldName
        } else {
            fieldNameInModel = field.Name
        }

        schemaField, err := th.mustSchemaField(fieldNameInModel, schema)
        if err != nil {
            return err
        }

        if !fieldValue.IsZero() {
            if fieldValue.Kind() == reflect.Struct {
                err = th.iterStructNonNilColumn(schema, fieldValue.Interface(), consumer)
                if err != nil {
                    return err
                }
            } else {
                column := schemaField.DBName
                consumer(column, fieldValue, field)
            }
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
func (th *Collection) mustSchemaField(fieldName string, schema *entity.Entity) (*entity.EntityField, error) {

    schemaField := schema.LookUpField(fieldName)

    if schemaField == nil {
        return nil, errors.WithStack(fmt.Errorf("fieldName name %s can not be found in %s", fieldName, schema.ModelType.Name()))
    }

    return schemaField, nil
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

// 创建单个元素
func (th *Collection) Create(ctx context.Context, model interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {

    if d, ok := model.(BeforeSave); ok {
        d.BeforeSave()
    }

    result, err := th.collection.InsertOne(ctx, model, opts...)
    if err != nil {
        return nil, err
    }

    if d, ok := model.(AfterSave); ok {
        d.AfterSave(result.InsertedID)
    }

    return result, nil
}

// 创建一组内容
func (th *Collection) CreateAll(ctx context.Context, models []interface{}, opts ...*options.InsertManyOptions) error {

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
func (th *Collection) Update(ctx context.Context, filter interface{}, document interface{}, opts ...*options.UpdateOptions) error {
    if d, ok := document.(BeforeUpdate); ok {
        d.BeforeUpdate()
    }

    update := mapToUpdateDocument(document)

    result, err := th.collection.UpdateOne(ctx, filter, update, opts...)

    if err != nil {
        return err
    }

    if result.MatchedCount == 0 {
        return fmt.Errorf("update fail")
    }

    if d, ok := document.(AfterUpdate); ok {
        d.AfterUpdate()
    }

    return nil
}

func (th *Collection) FindAndModify(ctx context.Context, filter interface{}, document interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
    return th.collection.FindOneAndUpdate(ctx, filter, document, opts...)
}

func (th *Collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
    return th.collection.InsertMany(ctx, documents, opts...)
}

func (th *Collection) EnsureIndex(model *mongo.IndexModel) (string, error) {
    return th.collection.Indexes().CreateOne(context.Background(), *model)
}

func mapToUpdateDocument(document interface{}) interface{} {

    m := bson.M{}

    t := reflect.TypeOf(document)
    v := reflect.ValueOf(document)

    // 字典类型先不处理，以后再加字典类型优化
    if t.Kind() == reflect.Map {
        return document
    }

    recursiveSet(t, v, m)
    return bson.M{
        "$set": m,
    }
}

var timeType = reflect.TypeOf(time.Time{})

func recursiveSet(t reflect.Type, v reflect.Value, result bson.M) {

    if t.Kind() == reflect.Ptr {
        t = t.Elem()
    }

    if v.Kind() == reflect.Ptr {
        v = v.Elem()
    }

    for i := 0; i < t.NumField(); i++ {
        // 获取属性
        field := t.Field(i)
        // 获取属性值
        fieldValue := v.Field(i)

        // 类型是指针,slice 和 map类型，发现是nil直接过滤
        if fieldValue.Kind() == reflect.Ptr || fieldValue.Kind() == reflect.Slice || fieldValue.Kind() == reflect.Map {
            if fieldValue.IsNil() {
                continue
            }

            if fieldValue.Kind() == reflect.Ptr {
                fieldValue = fieldValue.Elem()
            }
        }

        // slice 和 map类型需要检查是否为nil
        //if fieldValue.Kind() == reflect.Slice && fieldValue.Kind() == reflect.Map && fieldValue.IsNil() {
        //	continue
        //}

        if fieldValue.CanInterface() {
            bname := field.Tag.Get("bson")

            // bson协议中这个tag表示过滤
            if bname == "-" {
                continue
            }

            // 从bson中取，没有取到则用字段名字
            key := bname
            if key == "" {
                key = utils.ToLowerCamel(field.Name)
            }

            var finalValue interface{} = nil

            // inline模式的模型
            if bname == "inline" {
                recursiveSet(field.Type, fieldValue, result)
                continue
            } else if fieldValue.Kind() == reflect.Struct {

                // 检查类型
                //ft := field.Type
                //if ft.Kind() == reflect.Ptr {
                //    ft = ft.Elem()
                //}
                switch fieldValue.Type() {
                case timeType:
                    // 时间类型不做任何处理
                    finalValue = fieldValue.Interface()
                default:
                    // 当作一个属性设置
                    subM := bson.M{}
                    recursiveSet(field.Type, fieldValue, subM)
                    finalValue = subM
                }

            } else {
                finalValue = fieldValue.Interface()
            }

            if finalValue != nil {
                result[key] = finalValue
            }

        }
    }
}

// 更新的时候没有匹配上
type NotMatchError struct {
    msg string
}

//func NewNotMatchError(msg string) *NotMatchError {
//	return &NotMatchError{msg: msg}
//}
//
//func (n NotMatchError) Error() string {
//	return n.msg
//}
//
//func mustBeAddressableSlice(value reflect.Value) error {
//	if value.Kind() != reflect.Ptr {
//		return fmt.Errorf("results argument must be a pointer to a slice, but was a %s", value.Kind())
//	}
//
//	sliceVal := value.Elem()
//	if sliceVal.Kind() == reflect.Interface {
//		sliceVal = sliceVal.Elem()
//	}
//
//	if sliceVal.Kind() != reflect.Slice {
//		return fmt.Errorf("results argument must be a pointer to a slice, but was a pointer to %s", sliceVal.Kind())
//	}
//
//	return nil
//}
