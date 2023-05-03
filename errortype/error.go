package errortype

import "errors"

var (
	ErrUnsupportedDataType = errors.New("unsupported data type")

	ErrFilterNotContainAnyCondition = errors.New("filter not contain any condition, this behavior is not allow")

	ErrIdFieldDoesNotExists = errors.New("id field does not exits, please add tag bson:\"_id\" on any field you want")

	ErrModelTypeNotMatchInCollection = errors.New("model type not match in operator")
)
