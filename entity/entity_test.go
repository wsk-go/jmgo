package entity

import (
	"fmt"
	"reflect"
	"testing"
)

type Order struct {
	Id          string `bson:"_id"`
}

type User struct {
	Order `bson:"_,inline"`
	Name  *string `bson:"name"`
	Name2 string  `bson:"name333,inline"`
}

func Test_Entity(t *testing.T) {

	e, err := GetOrParse(&User{})

	if err != nil {
		fmt.Printf("%+v", err)
		//panic(e)
		return
	}

	//fmt.Println(e)

	//name := "123123"
	u := User{
		Order: Order{
			Id: "123",
		},
		//Name:  &name,
		//Name2: "222",
	}

	uv := reflect.ValueOf(u)

	for _, field := range e.Fields {
		v, _ := field.ValueOf(uv)
		fmt.Println(field.Name, v)
	}

}

func Benchmark(b *testing.B) {

	e, err := GetOrParse(&User{})

	if err != nil {
		panic(e)
	}

	fmt.Println(e)

	//name := "123123"
	u := User{
		Order: Order{
			Id: "123",
		},
		//Name:  &name,
		//Name2: "222",
	}

	uv := reflect.ValueOf(u)

	b.Run("one", func(b *testing.B) {
	   for i := 0; i < b.N; i++ {
           for _, field := range e.Fields {
               if field.Entity != nil {
                   uvv := field.ReflectValueOf(uv)
                   for _, f2 := range field.Entity.Fields {
                       f2.ValueOf(uvv)
                   }
               }
           }
	   }
	})

	b.Run("two", func(b *testing.B) {

		for i := 0; i < b.N; i++ {
            uv := uv.Field(0)
			for range e.Fields {
				uv.Field(0)
				//fmt.Println(field.Name, v)
			}
		}
	})

    b.Run("three", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            for _, field := range e.AllFields {
                field.InlineValueOf(uv)
            }
        }
    })
}
