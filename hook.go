package jmongo

type BeforeSave interface {
    BeforeSave()
}

type AfterSave interface {
    AfterSave(id interface{})
}

type BeforeUpdate interface {
    BeforeUpdate()
}

type AfterUpdate interface {
    AfterUpdate()
}
