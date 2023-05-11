package jmgo

import "github.com/go-playground/validator/v10"

type Validator interface {
	Struct(obj any) error
}

var Validate Validator = validator.New()
