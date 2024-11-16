package querystore

type ConditionType int

const (
	ConditionEquals ConditionType = iota
	ConditionNotEquals
	ConditionLessThan
	ConditionGreaterThan
)

type AggregatorType int

const (
	AggregatorCount AggregatorType = iota
	AggregatorSum
)

type Filter struct {
	Attribute string
	Condition ConditionType
	Value     any
}

type Query struct {
	Aggregator          AggregatorType
	AggregatorAttribute string
	Filters             []Filter
	GroupBy             string
}

type ConditionalFunc func(a, b any) bool

func anyEquals[T comparable]() ConditionalFunc {
	return func(a, b any) bool {
		return a.(T) == b.(T)
	}
}

func anyNotEquals[T comparable]() ConditionalFunc {
	return func(a, b any) bool {
		return a.(T) != b.(T)
	}
}

var conditionals = map[ConditionType]map[ColumnType]ConditionalFunc{
	ConditionEquals: {
		ColumnTypeBool:    anyEquals[bool](),
		ColumnTypeInt64:   anyEquals[int64](),
		ColumnTypeFloat64: anyEquals[float64](),
		ColumnTypeString:  anyEquals[string](),
	},
	ConditionNotEquals: {
		ColumnTypeBool:    anyNotEquals[bool](),
		ColumnTypeInt64:   anyNotEquals[int64](),
		ColumnTypeFloat64: anyNotEquals[float64](),
		ColumnTypeString:  anyNotEquals[string](),
	},
	ConditionLessThan: {
		ColumnTypeInt64:   func(a, b any) bool { return a.(int64) < b.(int64) },
		ColumnTypeFloat64: func(a, b any) bool { return a.(float64) < b.(float64) },
	},
	ConditionGreaterThan: {
		ColumnTypeInt64:   anyNotEquals[int64](),
		ColumnTypeFloat64: anyNotEquals[float64](),
		ColumnTypeString:  anyNotEquals[string](),
	},
}
