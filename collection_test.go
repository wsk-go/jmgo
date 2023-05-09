package jmongo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

const MongoUrl = "mongodb://47.94.142.208:27017/admin?connect=direct&maxPoolSize=50&minPoolSize=10&slaveOk=true"

type Base struct {
	Like string `bson:"like"`
}

func (b *Base) AfterUpdate() {
	fmt.Println("after update")
}

func (b *Base) AfterSave(id any) {
	fmt.Println("after save")
}

func (b *Base) BeforeUpdate() error {
	fmt.Println("before update")
	return nil
}

func (b *Base) BeforeSave() error {
	fmt.Println("before save")
	return nil
}

type Test struct {
	Base         `bson:",inline"`
	Id           SObjectId `bson:"_id,omitempty"`
	Name         string    `bson:"name"`
	Age          int       `bson:"happy"`
	HelloWorld   int       `bson:"helloWorld"`
	UserPassword int
	OrderId      SObjectId `bson:"orderId,omitempty"`
}

type TestFilter struct {
	Id SObjectId
}

func Test_Raw_Insert(t *testing.T) {
	c := setupMongoClient(MongoUrl)
	db := c.Database("test")
	col := NewCollection[*Test](&Test{}, db)

	err := col.InsertOne(context.Background(), &Test{
		Name:         "abc",
		Age:          8,
		HelloWorld:   123,
		UserPassword: 2,
		OrderId:      NewSObjectId(),
	})

	_, err = col.UpdateOne(context.Background(), bson.M{"_id": SObjectId("6425087c44ad0aff2c691cea")}, &Test{
		Name:         "abc",
		Age:          8,
		HelloWorld:   123,
		UserPassword: 2,
		OrderId:      NewSObjectId(),
	})

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
}

func Test_Bulk(t *testing.T) {
	c := setupMongoClient(MongoUrl)
	db := c.Database("test")
	col := NewCollection[*Test](&Test{}, db)

	r, err := col.BulkWrite(context.Background(), []mongo.WriteModel{
		col.NewUpdateManyModel(TestFilter{Id: "6425087c44ad0aff2c691cea"}, &Test{
			Name:         "abc11",
			Age:          8,
			HelloWorld:   123,
			UserPassword: 2,
			OrderId:      NewSObjectId(),
		}),
		col.NewInsertOneModel(&Test{
			Name:         "abc1122",
			Age:          8,
			HelloWorld:   123,
			UserPassword: 2,
			OrderId:      NewSObjectId(),
		}),
	})

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	fmt.Println(r.MatchedCount)
}

//	func Test_Raw_InsertTransaction(t *testing.T) {
//		c := setupMongoClient(MongoUrl)
//
//		ctx := context.Background()
//		db := c.Database("test")
//		db.WithTransaction(ctx, func(ctx context.Context) error {
//			col := db.Collection(&Test{})
//			err := col.InsertOne(ctx, &Test{
//				Name:         "abc",
//				Age:          8,
//				HelloWorld:   123,
//				UserPassword: 2,
//				OrderId:      types.NewSObjectId(),
//			})
//			if err != nil {
//				return err
//			}
//
//			col.InsertOne(ctx, &Test{
//				Name:         "abc",
//				Age:          8,
//				HelloWorld:   123,
//				UserPassword: 2,
//				OrderId:      types.NewSObjectId(),
//			})
//			return errors.New("test")
//		})
//
// }
func Test_Raw_Read(t *testing.T) {

	c := setupMongoClient(MongoUrl)
	db := c.Database("test")
	col := NewCollection[*Test](&Test{}, db)
	ctx := context.Background()

	models, err := col.FindOneByFilter(ctx, TestFilter{})

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	fmt.Println(models)
}

// func Test_FindOne(t *testing.T) {
//
//		type Base struct {
//			Age int
//		}
//
//		type Filter struct {
//			Base
//			Name string
//		}
//
//		c := setupMongoClient(MongoUrl)
//
//		db := c.Database("test")
//		col := db.Collection(&Test{})
//		ctx := context.Background()
//
//		var test Test
//		found, err := col.FindOne(ctx, &Filter{Base: Base{Age: 123}, Name: "abc"}, &test, Option().AddIncludes("Name"))
//
//		if err != nil {
//			fmt.Printf("%+v", err)
//			return
//		}
//
//		if !found {
//			fmt.Println("没有")
//			return
//		}
//
//		fmt.Println(test)
//	}
//
//	func Test_FindOneById(t *testing.T) {
//		c := setupMongoClient(MongoUrl)
//
//		db := c.Database("test")
//		col := db.Collection(&Test{})
//		ctx := context.Background()
//
//		var test Test
//		found, err := col.FindOne(ctx, primitive.NewObjectID().Hex(), &test, Option().AddIncludes("Name"))
//
//		if err != nil {
//			fmt.Printf("%+v", err)
//			return
//		}
//
//		if !found {
//			fmt.Println("没有")
//			return
//		}
//
//		fmt.Println(test)
//	}
//
// func Test_FindAll(t *testing.T) {
//
//		type Filter struct {
//			Name string
//		}
//
//		c := setupMongoClient(MongoUrl)
//
//		db := c.Database("test")
//		col := db.Collection(&Test{})
//		ctx := context.Background()
//
//		var test []Test
//		err := col.FindByFilter(ctx, &Filter{}, &test)
//
//		if err != nil {
//			fmt.Printf("%+v", err)
//			return
//		}
//
//		fmt.Println(test)
//	}
//
// func Test_Count(t *testing.T) {
//
//		type Filter struct {
//			Name  string
//			Names []string `jfield:"Name"`
//		}
//
//		c := setupMongoClient(MongoUrl)
//
//		db := c.Database("test")
//		col := db.Collection(&Test{})
//		ctx := context.Background()
//
//		count, err := col.Count(ctx, &Filter{
//			Names: []string{"123", "222"},
//		})
//
//		if err != nil {
//			fmt.Printf("%+v", err)
//			return
//		}
//
//		fmt.Println(count)
//	}
//
// //
// //func Test_Find(t *testing.T) {
// //
// //    type Filter struct {
// //        Name string
// //    }
// //
// //    c := NewClient(setupMongoClient(MongoUrl))
// //
// //    db := c.Database("test")
// //    col := db.Collection("test")
// //    ctx := context.Background()
// //
// //    var test []Test
// //    err := col.FindByFilter(ctx, &Filter{Name: "abc"}, &test, Option().Offset(0).Limit(2).AddSort("Age", true).AddIncludes("Name"))
// //
// //    if err != nil {
// //        fmt.Printf("%+v", err)
// //        return
// //    }
// //
// //    fmt.Println(test)
// //}
func setupMongoClient(mongoUrl string) *Client {

	monitorOptions := options.Client().SetMonitor(&event.CommandMonitor{
		Started: func(i context.Context, startedEvent *event.CommandStartedEvent) {
			fmt.Println("mongo command" + startedEvent.Command.String())
		},
	})

	client, err := NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		panic(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	return client
}

//
//type User struct {
//}

//func Test_Raw_Insert2(t *testing.T) {
//	t := reflect.TypeOf(User{})
//
//}
//
//func newMake[T any](t reflect.Type) T {
//	a := reflect.New(t)
//	return a.Interface()
//}
