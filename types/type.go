package types

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// MustObjectIdString use this type to indicate objectId string,
// it throw errortype if value is not valid objectId string in marshaling
// value with the type will be marshaled into ObjectId and save into mongodb
// value fetched from mongodb which is objectId will be unmarshaled into ObjectIdString
type MustObjectIdString string

// UnmarshalBSONValue bson转go对象
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

func (th MustObjectIdString) MarshalBSONValue() (bsontype.Type, []byte, error) {
	s := string(th)
	id, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return bsontype.Null, nil, err
	}

	return bson.MarshalValue(id)
}

func NewMustObjectIdString() ObjectIdString {
	return ObjectIdString(primitive.NewObjectID().Hex())
}

// ObjectIdString use this type to indicate objectId string
// value with the type will be marshaled into ObjectId and save into mongodb
// value fetched from mongodb which is objectId will be unmarshaled into ObjectIdString
type ObjectIdString string

func (th ObjectIdString) ToString() string {
	return string(th)
}

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

func (th ObjectIdString) MarshalBSONValue() (bsontype.Type, []byte, error) {
	s := string(th)
	id, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return bson.MarshalValue(s)
	}
	t, v, err := bson.MarshalValue(id)
	fmt.Println(t)
	fmt.Printf("\n%+v\n", err)
	return t, v, err
}

func NewObjectIdString() ObjectIdString {
	return ObjectIdString(primitive.NewObjectID().Hex())
}
