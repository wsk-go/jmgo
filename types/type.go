package types

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type MustSObjectId string

// UnmarshalBSONValue bson转go对象
func (th *MustSObjectId) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	if data == nil {
		return nil
	}

	switch t {
	case bsontype.ObjectID:
		objectId, _, ok := bsoncore.ReadObjectID(data)
		if ok {
			*th = MustSObjectId(objectId.Hex())
		}
	case bsontype.String:
		s, _, ok := bsoncore.ReadString(data)
		if ok {
			*th = MustSObjectId(s)
		}
	}

	return nil
}

func (th MustSObjectId) MarshalBSONValue() (bsontype.Type, []byte, error) {
	s := string(th)
	id, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return bsontype.Null, nil, err
	}

	return bson.MarshalValue(id)
}

func NewMustObjectIdString() SObjectId {
	return SObjectId(primitive.NewObjectID().Hex())
}

// SObjectId use this type to indicate objectId string
// value with the type will be marshaled into ObjectId and save into mongodb
// value fetched from mongodb which is objectId will be unmarshaled into SObjectId
type SObjectId string

func (th SObjectId) ToString() string {
	return string(th)
}

func (th *SObjectId) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	if data == nil {
		return nil
	}

	switch t {
	case bsontype.ObjectID:
		objectId, _, ok := bsoncore.ReadObjectID(data)
		if ok {
			*th = SObjectId(objectId.Hex())
		}
	case bsontype.String:
		s, _, ok := bsoncore.ReadString(data)
		if ok {
			*th = SObjectId(s)
		}
	}

	return nil
}

func (th SObjectId) MarshalBSONValue() (bsontype.Type, []byte, error) {
	s := string(th)
	id, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return bson.MarshalValue(s)
	}
	t, v, err := bson.MarshalValue(id)
	return t, v, err
}

func NewObjectIdString() SObjectId {
	return SObjectId(primitive.NewObjectID().Hex())
}
