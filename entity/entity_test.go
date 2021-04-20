package entity

import (
    "fmt"
    "reflect"
    "testing"
)

type Order struct {
    OrderField string
    OrderField2 string
}

type User struct {
    Order
    //Name  *string `bson:"name"`
    //Name2 string  `bson:"name333,inline"`
}

func Test_Entity(t *testing.T) {

    e, err := GetOrParse(&User{})

    if err != nil {
        panic(e)
    }

    fmt.Println(e)

    //name := "123123"
    u := User{
        Order: Order{
            OrderField: "123",
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
            OrderField: "123",
        },
        //Name:  &name,
        //Name2: "222",
    }

    uv := reflect.ValueOf(u)


    b.Run("jstream", func(b *testing.B) {
        uv := uv.Field(0)
        for i := 0; i < b.N; i++ {

            for _, field := range e.Fields {
                field.ValueOf(uv)
                //uv.Field(0)
                //fmt.Println(field.Name, v)
            }
        }
    })
}