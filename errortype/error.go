package errortype

import "errors"

var (

	// ErrUnsupportedDataType unsupported data type
	ErrUnsupportedDataType = errors.New("unsupported data type")

	// ErrFilterNotContainAnyCondition filter contain any condition
	ErrFilterNotContainAnyCondition = errors.New("filter not contain any condition, this behavior is not allow")

	// ErrFilterNotContainAnyCondition filter contain any condition
	ErrIdFieldDoesNotExists = errors.New("id field does not exits, please add tag bson:\"_id\" on any field you want")
)

