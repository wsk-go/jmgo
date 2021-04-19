package errortype

var (

	// ErrUnsupportedDataType unsupported data type
	ErrUnsupportedDataType = New("unsupported data type")

	// ErrFilterNotContainAnyCondition filter contain any condition
	ErrFilterNotContainAnyCondition = New("filter not contain any condition, this behavior is not allow")

	// ErrFilterNotContainAnyCondition filter contain any condition
	ErrIdFieldNotFound = New("id field does not exits, please add tag bson:\"_id\" on any field you want")
)

func New(message string) error {
	return nil
}
