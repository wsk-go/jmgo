package jmongo

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"jmongo/utils"
	"reflect"
)

type FilterField struct {
	DBName string
	Field  reflect.StructField
}

type FilterOperator interface {
	handle(field *FilterField,  query bson.M)
}

// operator sign
type CompareSign uint8

const (
	// smaller than
	CompareSignLt CompareSign = 1
	// smaller than or equal
	CompareSignLte CompareSign = 2
	// greater than
	CompareSignGt CompareSign = 3
	// greater than or equal
	CompareSignGte CompareSign = 4
	// equal
	CompareSignE CompareSign = 5
)

// compare data type
type Compare struct {
	Value interface{}
	Sign  CompareSign
}

func (th Compare) handle(field *FilterField, query bson.M) {
	var sign = ""
	switch th.Sign {
	case CompareSignLt:
		sign = "$lt"
	case CompareSignLte:
		sign = "$lte"
	case CompareSignGt:
		sign = "$gt"
	case CompareSignGte:
		sign = "$gte"
	case CompareSignE:
		sign = "$eq"
	}

	query[field.DBName] = bson.M{sign: th.Value}
}

// range
// date type must use basic type
type Range struct {

	// start
	Start interface{}

	// end
	End interface{}

	// use xxx < end if the value is true or else use xxx <= end
	EndWithoutEqual bool
}

func (th Range) handle(field *FilterField, query bson.M) {
	startIsNil := utils.IsNil(th.Start)
	endIsNil := utils.IsNil(th.End)

	if startIsNil && endIsNil {
		panic(newError(fmt.Sprintf("start and end in %s at least one is not nil", field.Field.Name)))
	}

	m := bson.M{}
	if !startIsNil {
		m["$gte"] = th.Start
	}

	if !endIsNil {
		sign := "$lte"
		if th.EndWithoutEqual {
			sign = sign[0:3]
		}
		m[sign] = th.End
	}

	query[field.DBName] = m
}


// operator sign
type MatchType uint8

const (
	// smaller than
	MatchTypePrefix MatchType = 1
	// smaller than or equal
	MatchTypeSuffix MatchType = 2
	// greater than
	MatchTypeContains MatchType = 3
)

type Match struct {
	Value string
	Type MatchType
}

func (th Match) handle(field *FilterField, query bson.M) {
	var like = ""
	switch th.Type {
	case MatchTypePrefix:
		like = fmt.Sprintf("%s.*", th.Value)
	case MatchTypeSuffix:
		like = fmt.Sprintf("'.*%s'", th.Value)
	case MatchTypeContains:
		like = fmt.Sprintf(".*%s.*", th.Value)
	}

	query[field.DBName] = bson.M{"$regex": like}
}


// Not In Operator
type NotIn struct {
	Value interface{}
}

func (th NotIn) handle(field *FilterField, query bson.M) {
	query[field.DBName] = bson.M{"$nin": th.Value}
}
