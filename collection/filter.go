package collection

type Order struct {
	Field string
	Asc   bool
}

type IFilter interface {
	Skip() int
	SetSkip(skip int)

	Limit() int
	SetLimit(limit int)

	// WithTotal get total if you want
	WithTotal(total *int64)

	// AddIncludes 要选择的属性，注意用模型定义的属性名字，而不是
	AddIncludes(includes ...string)

	// AddExcludes 不选择的属性
	AddExcludes(excludes ...string)

	// AddOrder 排序
	// - fieldName: 属性名字
	// - asc: 是否从小到大排序
	AddOrder(fieldName string, asc bool)
}

// Filter 过滤实现
type Filter struct {
	skip     int
	limit    int
	total    *int64
	includes []string
	excludes []string
	orders   []*Order
}

func (th *Filter) Skip() int {
	return th.skip
}

func (th *Filter) SetSkip(skip int) {
	th.skip = skip
}

func (th *Filter) Limit() int {
	return th.limit
}

func (th *Filter) SetLimit(limit int) {
	th.limit = limit
}

func (th *Filter) WithTotal(total *int64) {
	th.total = total
}

func (th *Filter) Includes() []string {
	return th.includes
}

func (th *Filter) SetIncludes(includes []string) {
	th.includes = includes
}

func (th *Filter) Excludes() []string {
	return th.excludes
}

func (th *Filter) SetExcludes(excludes []string) {
	th.excludes = excludes
}

// AddIncludes 要选择的属性，注意用模型定义的属性名字，而不是
func (th *Filter) AddIncludes(includes ...string) {
	th.includes = append(th.includes, includes...)
}

// AddExcludes 不选择的属性
func (th *Filter) AddExcludes(excludes ...string) {
	th.excludes = append(th.excludes, excludes...)
}

// AddOrder 排序
// - fieldName: 属性名字
// - asc: 是否从小到大排序
func (th *Filter) AddOrder(fieldName string, asc bool) {
	th.orders = append(th.orders, &Order{
		Field: fieldName,
		Asc:   asc,
	})
}
