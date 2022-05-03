package collection

import (
	"code.aliyun.com/jgo/jmongo/extype"
	"testing"
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

type MyFilter struct {
	Filter
	Id string `bson:"_id"`
}

func Test_Raw_Insert2(t *testing.T) {
	aaa()
}

//func Test_Raw_Insert2(t *testing.T) {
//
//	client := setupMongoClient(MongoUrl)
//	db := client.Database("abc")
//
//	test := &Test{}
//	cc := New[Test, *MyFilter](db, test)
//
//	f := &MyFilter{}
//	f.Id = "616e711dcb16ad0517dd8b12"
//	tttt, err := cc.FindOneByFilter(context.Background(), f)
//
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	print(tttt.Age)
//}
//
func aaa[T any]() T {
	return nil
}

//func setupMongoClient(mongoUrl string) *mongo.Client {
//
//	monitorOptions := options.Client().SetMonitor(&event.CommandMonitor{
//		Started: func(i context.Context, startedEvent *event.CommandStartedEvent) {
//			fmt.Println("mongo command" + startedEvent.Command.String())
//		},
//	})
//
//	//if conf.Profile != "dev" {
//	//	monitorOptions.SetAuth(options.Credential{
//	//		AuthSource: conf.MongoAuthSource,
//	//		Username:   conf.MongoUserName,
//	//		Password:   conf.MongoPassword,
//	//	})
//	//}
//
//	//credentials := options.Client().SetAuth(options.Credential{
//	//	AuthSource: "admin",
//	//	Username:   "jcloudapp",
//	//	Password:   "jcloudapp1231!",
//	//})
//
//	//client, err := mongo.NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions, credentials)
//	client, err := mongo.NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions)
//	if err != nil {
//		panic(err)
//	}
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//
//	err = client.Connect(ctx)
//	if err != nil {
//		panic(err)
//	}
//
//	err = client.Ping(context.TODO(), nil)
//	if err != nil {
//		panic(err)
//	}
//
//	return client
//}
