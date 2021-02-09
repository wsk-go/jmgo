package errors

import "github.com/pkg/errors"

func NewError(message string) error  {
    return errors.New(message)
}

func WithStack(err error) error  {
    return errors.WithStack(err)
}
