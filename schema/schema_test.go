package schema

import (
    "fmt"
    "jmongo/jtype"
    "testing"
)

type User struct {
    Name    string
    Age     string `bson:"age"`
    OrderId jtype.ObjectIdString
}

func (*User) CollectionName() string {
    return "t_user"
}

func Test_ParseSchema(t *testing.T) {
    s, err := getOrParse(&User{})
    if err != nil {
        fmt.Println(s)
    }
    fmt.Println(s)

}
