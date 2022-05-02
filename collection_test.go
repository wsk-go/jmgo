package jmongo

import (
	"code.aliyun.com/jgo/jmongo/extype"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
	"time"
)

const MongoUrl = "mongodb://47.94.142.208:27017/?connect=direct&maxPoolSize=50&minPoolSize=10&slaveOk=true"

type Base struct {
	Like string `bson:"like"`
}

type Test struct {
	Base         `bson:",inline"`
	Id           extype.ObjectIdString `bson:"_id,omitempty"`
	Name         string                `bson:"name"`
	Age          int                   `bson:"happy"`
	HelloWorld   int                   `bson:"helloWorld"`
	UserPassword int
	OrderId      extype.ObjectIdString `bson:"orderId,omitempty"`
}

func Test_Raw_Insert(t *testing.T) {
	c := setupMongoClient(MongoUrl)

	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()
	err := col.InsertOne(ctx, &Test{
		Name:         "abc",
		Age:          8,
		HelloWorld:   123,
		UserPassword: 2,
		OrderId:      extype.NewObjectIdString(),
	})

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
}

func Test_Raw_InsertTransaction(t *testing.T) {
	c := setupMongoClient(MongoUrl)

	ctx := context.Background()
	db := c.Database("test")
	db.WithTransaction(ctx, func(ctx context.Context) error {
		col := db.Collection(&Test{})
		err := col.InsertOne(ctx, &Test{
			Name:         "abc",
			Age:          8,
			HelloWorld:   123,
			UserPassword: 2,
			OrderId:      extype.NewObjectIdString(),
		})
		if err != nil {
			return err
		}

		col.InsertOne(ctx, &Test{
			Name:         "abc",
			Age:          8,
			HelloWorld:   123,
			UserPassword: 2,
			OrderId:      extype.NewObjectIdString(),
		})
		return errors.New("test")
	})

}

func Test_Raw_Read(t *testing.T) {

	c := setupMongoClient(MongoUrl)
	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()

	var test Test
	ok, err := col.FindOne(ctx, extype.ObjectIdString("6088e5007f987f7fb64ab94d"), &test)

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	fmt.Println(ok)
	fmt.Println(test)
}

func Test_FindOne(t *testing.T) {

	type Base struct {
		Age int
	}

	type Filter struct {
		Base
		Name string
	}

	c := setupMongoClient(MongoUrl)

	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()

	var test Test
	found, err := col.FindOne(ctx, &Filter{Base: Base{Age: 123}, Name: "abc"}, &test, Option().AddIncludes("Name"))

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	if !found {
		fmt.Println("没有")
		return
	}

	fmt.Println(test)
}

func Test_FindOneById(t *testing.T) {
	c := setupMongoClient(MongoUrl)

	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()

	var test Test
	found, err := col.FindOne(ctx, primitive.NewObjectID().Hex(), &test, Option().AddIncludes("Name"))

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	if !found {
		fmt.Println("没有")
		return
	}

	fmt.Println(test)
}

func Test_FindAll(t *testing.T) {

	type Filter struct {
		Name string
	}

	c := setupMongoClient(MongoUrl)

	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()

	var test []Test
	err := col.Find(ctx, &Filter{}, &test)

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	fmt.Println(test)
}

func Test_Count(t *testing.T) {

	type Filter struct {
		Name  string
		Names []string `jfield:"Name"`
	}

	c := setupMongoClient(MongoUrl)

	db := c.Database("test")
	col := db.Collection(&Test{})
	ctx := context.Background()

	count, err := col.Count(ctx, &Filter{
		Names: []string{"123", "222"},
	})

	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	fmt.Println(count)
}

//
//func Test_Find(t *testing.T) {
//
//    type Filter struct {
//        Name string
//    }
//
//    c := NewClient(setupMongoClient(MongoUrl))
//
//    db := c.Database("test")
//    col := db.Collection("test")
//    ctx := context.Background()
//
//    var test []Test
//    err := col.Find(ctx, &Filter{Name: "abc"}, &test, Option().Offset(0).Limit(2).AddOrder("Age", true).AddIncludes("Name"))
//
//    if err != nil {
//        fmt.Printf("%+v", err)
//        return
//    }
//
//    fmt.Println(test)
//}

func setupMongoClient(mongoUrl string) *Client {

	monitorOptions := options.Client().SetMonitor(&event.CommandMonitor{
		Started: func(i context.Context, startedEvent *event.CommandStartedEvent) {
			fmt.Println("mongo command" + startedEvent.Command.String())
		},
	})

	//if conf.Profile != "dev" {
	//	monitorOptions.SetAuth(options.Credential{
	//		AuthSource: conf.MongoAuthSource,
	//		Username:   conf.MongoUserName,
	//		Password:   conf.MongoPassword,
	//	})
	//}

	//credentials := options.Client().SetAuth(options.Credential{
	//	AuthSource: "admin",
	//	Username:   "jcloudapp",
	//	Password:   "jcloudapp1231!",
	//})

	//client, err := mongo.NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions, credentials)
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions)
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

	return NewClient(client)
}
