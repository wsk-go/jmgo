package filter

type StructTags struct {
    Name      string
    Skip      bool
}

func parseTags(key string, tag string) (StructTags, error) {
    var st StructTags
    if tag == "-" {
        st.Skip = true
        return st, nil
    }

    //for idx, str := range strings.Split(tag, ",") {
    //    if idx == 0 && str != "" {
    //        key = str
    //    }
    //    switch str {
    //    case "omitempty":
    //        st.OmitEmpty = true
    //    case "minsize":
    //        st.MinSize = true
    //    case "truncate":
    //        st.Truncate = true
    //    case "inline":
    //        st.Inline = true
    //    }
    //}

    st.Name = key

    return st, nil
}

