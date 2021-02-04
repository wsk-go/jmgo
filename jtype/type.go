package jtype

import (
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/bsontype"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// 存储时string是ObjectId类型，则会转成ObjectId进行存储
// 如果是string不是ObjectId类型，抛出异常
type MustObjectIdString string

// bson转go对象
func (th *MustObjectIdString) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
    if data == nil {
        return nil
    }

    switch t {
    case bsontype.ObjectID:
        objectId, _, ok := bsoncore.ReadObjectID(data)
        if ok {
            *th = MustObjectIdString(objectId.Hex())
        }
    case bsontype.String:
        s, _, ok := bsoncore.ReadString(data)
        if ok {
            *th = MustObjectIdString(s)
        }
    }

    return nil
}

func NewMustObjectIdString() ObjectIdString {
    return ObjectIdString(primitive.NewObjectID().Hex())
}

// objectString 转bson
func (th MustObjectIdString) MarshalBSONValue() (bsontype.Type, []byte, error) {
    s := string(th)
    id, err := primitive.ObjectIDFromHex(s)
    if err != nil {
        return bsontype.Null, nil, err
    }

    return bson.MarshalValue(id)
}

// 如果是string是ObjectId类型，则会转成ObjectId进行存储
// 如果是string不是ObjectId类型，则会使用string进行存储
type ObjectIdString string

// bson转go对象
func (th *ObjectIdString) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
    if data == nil {
        return nil
    }

    switch t {
    case bsontype.ObjectID:
        objectId, _, ok := bsoncore.ReadObjectID(data)
        if ok {
            *th = ObjectIdString(objectId.Hex())
        }
    case bsontype.String:
        s, _, ok := bsoncore.ReadString(data)
        if ok {
            *th = ObjectIdString(s)
        }
    }

    return nil
}

// objectString 转bson
func (th ObjectIdString) MarshalBSONValue() (bsontype.Type, []byte, error) {
    s := string(th)
    id, err := primitive.ObjectIDFromHex(s)
    if err != nil {
        return bson.MarshalValue(s)
    }
    return bson.MarshalValue(id)
}

func NewObjectIdString() ObjectIdString {
    return ObjectIdString(primitive.NewObjectID().Hex())
}
