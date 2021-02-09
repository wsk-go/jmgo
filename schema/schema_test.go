package schema

import (
    "fmt"
    "jmongo/extype"
    "testing"
)

type User struct {
    Name    string
    Age     string `bson:"age"`
    OrderId extype.ObjectIdString
}

func (*User) CollectionName() string {
    return "t_user"
}

func Test_ParseSchema(t *testing.T) {
    s, err := GetOrParse(&User{})
    if err != nil {
        fmt.Println(s)
    }
    fmt.Println(s)

}
