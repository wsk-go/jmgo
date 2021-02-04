package jmongo

import (
    "context"
    "fmt"
    "github.com/pkg/errors"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "jmongo/utils"
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

// 查询和计算总数量
// 如果total不为空，返回总数量
func (th *Collection) FindAndCountTotal(ctx context.Context, filter interface{}, results interface{}, total *int64, opts ...*options.FindOptions) error {

    var query = true
    if total != nil {
        if filter == nil {
            filter = bson.M{}
        }

        totalNumber, err := th.collection.CountDocuments(ctx, filter)
        if err != nil {
            return errors.Wrap(err, "count documents occur error")
        }
        *total = totalNumber
        if totalNumber == 0 {
            query = false
        }
    }

    if query {
        // 查询
        err := th.Find(ctx, filter, results, opts...)
        if err != nil {
            return err
        }
    }

    return nil
}

type Page interface {
    // 获取分页偏移量
    GetOffset() int64

    // 获取分页长度
    GetLength() int64

    // 如果不为nil,会计算总数量
    GetWithTotal() *int64
}

// 查询和计算总数量
// 如果total不为空，返回总数量
func (th *Collection) Page(ctx context.Context, page Page, filter interface{}, results interface{}, opts ...*options.FindOptions) error {

    var query = true
    if page != nil && page.GetWithTotal() != nil {
        if filter == nil {
            filter = bson.M{}
        }

        totalNumber, err := th.collection.CountDocuments(ctx, filter)
        if err != nil {
            return errors.Wrap(err, "count documents occur error")
        }
        *page.GetWithTotal() = totalNumber
        if totalNumber == 0 {
            query = false
        }
    }

    if query {
        if page != nil && page.GetLength() >= 0 {
            opt := options.Find().SetSkip(page.GetOffset()).SetLimit(page.GetLength())
            opts = append(opts, opt)
        }

        // 查询
        err := th.Find(ctx, filter, results, opts...)
        if err != nil {
            return err
        }
    }

    return nil
}

// 封装了一下mongo的查询方法
func (th *Collection) Find(ctx context.Context, filter interface{}, results interface{}, opts ...*options.FindOptions) error {
    if filter == nil {
        filter = bson.M{}
    }
    // 查询
    cursor, err := th.collection.Find(ctx, filter, opts...)

    if err != nil {
        return err
    }

    defer func() {
        _ = cursor.Close(ctx)
    }()

    err = cursor.All(ctx, results)

    return err
}

// 封装了一下mongo的查询方法
func (th *Collection) FindOne(ctx context.Context, filter interface{}, results interface{}, opts ...*options.FindOneOptions) error {
    if filter == nil {
        filter = bson.M{}
    }
    // 查询
    one := th.collection.FindOne(ctx, filter, opts...)
    err := one.Decode(results)
    if err != nil {
        if err == mongo.ErrNoDocuments {
            return nil
        }
        return err
    }
    return err
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

//func (th *Collection) Count(ctx context.Context, filter interface{}, opts ...*options.AggregateOptions) (int64, error) {
//    type Count struct {
//        Count int64 `bson:"count"`
//    }
//    pipe := getPipe(filter)
//
//    cursor, err := th.collection.Aggregate(ctx, pipe, opts...)
//    if err != nil {
//        return 0, err
//    }
//
//    defer func() {
//        _ = cursor.Close(ctx)
//    }()
//
//    var results []*Count
//    err = cursor.All(ctx, &results)
//    if err != nil {
//        return 0, err
//    }
//
//    if len(results) != 0 {
//        return results[0].Count, err
//    }
//    return 0, nil
//}

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
        return newError("update fail")
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

//func (th *Collection) setUpdateDocument(document interface{})  {
//   v := reflect.ValueOf(document)
//
//   if v.Kind() == reflect.Ptr {
//       v = v.Elem()
//   }
//
//}

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

func NewNotMatchError(msg string) *NotMatchError {
    return &NotMatchError{msg: msg}
}

func (n NotMatchError) Error() string {
    return n.msg
}
