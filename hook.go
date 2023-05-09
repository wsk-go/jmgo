package jmongo

type BeforeSave interface {
	BeforeSave() error
}

type AfterSave interface {
	AfterSave(id any)
}

type BeforeUpdate interface {
	BeforeUpdate() error
}

type AfterUpdate interface {
	AfterUpdate()
}
