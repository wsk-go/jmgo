package jmgo

import (
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"strconv"
	"time"
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

func NewSObjectId() SObjectId {
	return SObjectId(primitive.NewObjectID().Hex())
}

type MilliTime time.Time

// UnmarshalBSONValue UnmarshalBSON bson转go对象
func (th *MilliTime) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	milli, _, ok := bsoncore.ReadDateTime(data)
	if ok {
		*th = MilliTime(time.UnixMilli(milli))
	}

	return nil
}

// MarshalBSONValue go对象转bson
func (th MilliTime) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bson.MarshalValue(time.Time(th))
}

func (th *MilliTime) UnmarshalJSON(data []byte) error {
	milli, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return err
	}
	*th = MilliTime(time.UnixMilli(milli))
	return nil
}

func (th MilliTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(th).UnixMilli())
}

func Now() MilliTime {
	return MilliTime(time.Now())
}

func FromTime(time time.Time) MilliTime {
	return MilliTime(time)
}

func NewFromTimePtr(time *time.Time) *MilliTime {
	return (*MilliTime)(time)
}

func NewFromTime(time time.Time) *MilliTime {
	return (*MilliTime)(&time)
}
