package querystore

import (
	"fmt"
	"os"
)

func FileExists(path string) (bool, error) {
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
