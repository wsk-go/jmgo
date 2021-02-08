package jmongo

import (
    "context"
    "fmt"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/event"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "jmongo/jtype"
    "testing"
    "time"
)

const MongoUrl = "mongodb://39.106.218.107:27017/?connect=direct&maxPoolSize=50&minPoolSize=10&slaveOk=true"

type Test struct {
    Name         jtype.ObjectIdString `bson:"name"`
    Age          int                  `bson:"happy"`
    HelloWorld   int                  `bson:"hello_world"`
    UserPassword int
    OrderId      jtype.ObjectIdString
}

func Test_Raw_Insert(t *testing.T) {

    c := setupMongoClient(MongoUrl)
    db := c.Database("test")
    col := db.Collection("test")
    ctx := context.Background()
    result, err := col.InsertOne(ctx, &Test{
        Name:         "abc",
        Age:          8,
        HelloWorld:   123,
        UserPassword: 2,
        OrderId:      "123",
    })

    if err != nil {
        fmt.Printf("%+v", err)
        return
    }

    fmt.Println(result.InsertedID)
}

func Test_Raw_Read(t *testing.T) {

    c := setupMongoClient(MongoUrl)
    db := c.Database("test")
    col := db.Collection("test")
    ctx := context.Background()

    var test Test
    err := col.FindOne(ctx, bson.M{}).Decode(&test)

    if err != nil {
        fmt.Printf("%+v", err)
        return
    }

    fmt.Println(test)
}

func Test_Read(t *testing.T) {

    c := NewClient(setupMongoClient(MongoUrl))

    db := c.Database("test")
    col := db.Collection("test")
    ctx := context.Background()

    var test Test
    err := col.FindOne()

    if err != nil {
        fmt.Printf("%+v", err)
        return
    }

    fmt.Println(test)
}

func setupMongoClient(mongoUrl string) *mongo.Client {

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

    credentials := options.Client().SetAuth(options.Credential{
        AuthSource: "admin",
        Username:   "jcloudapp",
        Password:   "jcloudapp1231!",
    })

    client, err := mongo.NewClient(options.Client().ApplyURI(mongoUrl), monitorOptions, credentials)
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
