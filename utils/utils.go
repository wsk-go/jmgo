package utils

import (
    "reflect"
    "strconv"
    "strings"
)

func ToLowerCamel(s string) string {
    return s
}

func ToString(value interface{}) string {
    switch v := value.(type) {
    case string:
        return v
    case int:
        return strconv.FormatInt(int64(v), 10)
    case int8:
        return strconv.FormatInt(int64(v), 10)
    case int16:
        return strconv.FormatInt(int64(v), 10)
    case int32:
        return strconv.FormatInt(int64(v), 10)
    case int64:
        return strconv.FormatInt(v, 10)
    case uint:
        return strconv.FormatUint(uint64(v), 10)
    case uint8:
        return strconv.FormatUint(uint64(v), 10)
    case uint16:
        return strconv.FormatUint(uint64(v), 10)
    case uint32:
        return strconv.FormatUint(uint64(v), 10)
    case uint64:
        return strconv.FormatUint(v, 10)
    }
    return ""
}


// 通过反射来确定是否是nil
// 因为interface的nil必须是Type和Value都是nil才算
// 所以以下这种情况不会是nil
// var a []string = nil
// var b interface{} = a
// b == nil (false) 因为b的类型是[]string类型，无法判定为nil
// 我们一般判定是否是nil都是通过Value来确定
func IsNil(i interface{}) bool {
    if i == nil {
        return true
    }

    value := reflect.ValueOf(i)
    switch value.Kind() {
    case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer:
        fallthrough
    case reflect.Interface, reflect.Slice:
        return value.IsNil()
    }
    return false
}

// 通过反射来确定是否是zero
func IsZero(i interface{}) bool {
    value := reflect.ValueOf(i)
    return value.IsZero()
}

func LowerFirst(s string) string {
    if len(s) > 1 {
        return strings.ToLower(s[0:1]) + s[1:]
    }

    return ""
}

// 解析TagSettings
func ParseTagSetting(str string, sep string) map[string]string {
    settings := map[string]string{}
    names := strings.Split(str, sep)

    for i := 0; i < len(names); i++ {
        j := i
        if len(names[j]) > 0 {
            for {
                if names[j][len(names[j])-1] == '\\' {
                    i++
                    names[j] = names[j][0:len(names[j])-1] + sep + names[i]
                    names[i] = ""
                } else {
                    break
                }
            }
        }

        values := strings.Split(names[j], ":")
        k := strings.TrimSpace(strings.ToUpper(values[0]))

        if len(values) >= 2 {
            settings[k] = strings.Join(values[1:], ":")
        } else if k != "" {
            settings[k] = k
        }
    }

    return settings
}