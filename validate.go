package jmgo

import "github.com/go-playground/validator/v10"

var validate = validator.New()

var Validate = func(obj any) error {
	return validate.Struct(obj)
}
