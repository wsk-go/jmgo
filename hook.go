package jmongo

type BeforeSave interface {
	BeforeSave()
}

type AfterSave interface {
	AfterSave(id any)
}

type BeforeUpdate interface {
	BeforeUpdate()
}

type AfterUpdate interface {
	AfterUpdate()
}
