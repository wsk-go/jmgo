package jmongo

import "github.com/pkg/errors"

func newError(message string) error  {
    return errors.New(message)
}
