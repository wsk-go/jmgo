package jmgo

import "github.com/go-playground/validator/v10"

var validate = validator.New()

type ValidateFunc func(obj any) error

var defaultValidate = func(obj any) error {
	return validate.Struct(obj)
}
