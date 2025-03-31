package unstructured

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/json"
)

func SetNestedField[T any](obj map[string]interface{}, value any, fields ...string) {
	var result T
	json.MustUnmarshal(json.MustMarshal(value), &result)
	err := unstructured.SetNestedField(obj, result, fields...)
	if err != nil {
		panic(err)
	}
}

func SetNestedFieldObject(obj map[string]interface{}, value any, fields ...string) {
	SetNestedField[map[string]interface{}](obj, value, fields...)
}

func SetNestedFieldSlice(obj map[string]interface{}, value any, fields ...string) {
	SetNestedField[[]interface{}](obj, value, fields...)
}
