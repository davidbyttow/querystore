package querystore

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// toUint64 converts an integer type value to uint64
func toUint64(val any) uint64 {
	switch v := val.(type) {
	case int:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	default:
		panic(fmt.Sprintf("unsupported type: %T", val))
	}
}

// toFloat64 converts an float type value to float64
func toFloat64(val any) float64 {
	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", val))
	}
}

func biMap[K comparable, V comparable](m map[K]V) map[V]K {
	res := make(map[V]K, len(m))
	for k, v := range m {
		if _, ok := res[v]; ok {
			panic(fmt.Sprintf("duplicate value: %v", v))
		}
		res[v] = k
	}
	return res
}

func castValueToColumnType(v any, typ ColumnType) any {
	switch typ {
	case ColumnTypeBool:
		return valueToBool(v)
	case ColumnTypeString:
		return valueToString(v)
	case ColumnTypeInt64:
		return valueToInt64(v)
	case ColumnTypeFloat64:
		return valueToFloat64(v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func valueColumnType(v any) ColumnType {
	switch v.(type) {
	case bool:
		return ColumnTypeBool
	case string:
		return ColumnTypeString
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return ColumnTypeInt64
	case float32, float64:
		return ColumnTypeFloat64
	default:
		return ColumnTypeString
	}
}

func valueToBool(v any) bool {
	switch v := v.(type) {
	case bool:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v != 0
	case float32, float64:
		return v != 0
	case string:
		if strings.EqualFold(v, "true") {
			return true
		}
		return false
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func valueToInt64(v any) int64 {
	switch v := v.(type) {
	case bool:
		if v {
			return 1
		}
		return 0
	case int64:
		return v
	case int, int8, int16, int32, uint, uint8, uint16, uint32, uint64:
		return int64(toUint64(v))
	case float32, float64:
		return int64(math.Round(toFloat64(v)))
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0
		}
		return i
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func valueToFloat64(v any) float64 {
	switch v := v.(type) {
	case bool:
		if v {
			return 1
		}
		return 0
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return float64(toUint64(v))
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func valueToString(v any) string {
	switch v := v.(type) {
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case string:
		return v
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func makeColumnFileName(name string, typ ColumnType) string {
	return name + "." + columnTypeToSuffix[typ] + "." + extension
}
