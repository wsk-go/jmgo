package jmgo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type Database struct {
	db              *mongo.Database
	client          *Client
	lastResumeToken bson.Raw
	cache           sync.Map
}

func NewDatabase(db *mongo.Database, client *Client) *Database {
	return &Database{db: db, client: client}
}

func (th *Database) Database() *mongo.Database {
	return th.db
}

// Watch listen: 出错直接使用panic
func (th *Database) Watch(opts *options.ChangeStreamOptions, matchStage bson.D, listen func(stream *mongo.ChangeStream) error) {

	for {
		time.After(1 * time.Second)
		func() {
			defer func() {
				err := recover()
				if err != nil {
					fmt.Printf("同步出现异常: %+v \n", err)
				}
			}()

			// 设置恢复点
			if th.lastResumeToken != nil {
				opts.SetResumeAfter(th.lastResumeToken)
				opts.SetStartAtOperationTime(nil)
				opts.SetStartAfter(nil)
			}

			changeStream, err := th.db.Watch(context.TODO(), mongo.Pipeline{matchStage}, opts)
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
