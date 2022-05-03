package collection

import (
	"code.aliyun.com/jgo/jmongo/entity"
	"code.aliyun.com/jgo/jmongo/filter"
	"code.aliyun.com/jgo/jmongo/utils"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

type FilterOperator interface {
	handle(entityField *entity.EntityField, filterField *filter.FilterField, query bson.M) error
}

// CompareSign operator sign
type CompareSign uint8

const (
	// CompareSignLt smaller than
	CompareSignLt CompareSign = 1
	// CompareSignLte smaller than or equal
	CompareSignLte CompareSign = 2
	// CompareSignGt greater than
	CompareSignGt CompareSign = 3
	// CompareSignGte greater than or equal
	CompareSignGte CompareSign = 4
	// CompareSignE equal
	CompareSignE CompareSign = 5
)

// Compare compare data type
type Compare struct {
	Value any
	Sign  CompareSign
}

func (th Compare) handle(field *entity.EntityField, filterField *filter.FilterField, query bson.M) error {
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
	return nil
}

// Range
// date type must use basic type
type Range struct {

	// start
	Start any

	// end
	End any

	// use xxx < end if the value is true or else use xxx <= end
	EndWithoutEqual bool
}

func (th Range) handle(field *entity.EntityField, filterField *filter.FilterField, query bson.M) error {
	startIsNil := utils.IsNil(th.Start)
	endIsNil := utils.IsNil(th.End)

	if startIsNil && endIsNil {
		return errors.New(fmt.Sprintf("start and end in %s at least one is not nil", field.Name))
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
	return nil
}

// MatchType operator sign
type MatchType uint8

const (
	// MatchTypePrefix smaller than
	MatchTypePrefix MatchType = 1
	// MatchTypeSuffix smaller than or equal
	MatchTypeSuffix MatchType = 2
	// MatchTypeContains greater than
	MatchTypeContains MatchType = 3
)

type Match struct {
	Value string
	Type  MatchType
}

func (th Match) handle(field *entity.EntityField, filterField *filter.FilterField, query bson.M) error {
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
	return nil
}

// NotIn Not In Operator
type NotIn struct {
	Value any
}

func (th NotIn) handle(field *entity.EntityField, filterField *filter.FilterField, query bson.M) error {
	query[field.DBName] = bson.M{"$nin": th.Value}
	return nil
}
