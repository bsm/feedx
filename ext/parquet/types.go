package parquet

import (
	"reflect"
	"strings"
	"sync"
)

type structFields map[string]int

var fieldCache sync.Map // map[reflect.Type]structFields

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
//
// "Inspired" by https://golang.org/src/encoding/json/encode.go
// Copyright 2010 The Go Authors. All rights reserved.
func cachedTypeFields(t reflect.Type) structFields {
	if f, ok := fieldCache.Load(t); ok {
		return f.(structFields)
	}

	f, _ := fieldCache.LoadOrStore(t, typeFields(t))
	return f.(structFields)
}

func tagName(tag string) string {
	if pos := strings.Index(tag, ","); pos != -1 {
		return tag[:pos]
	}
	return tag
}

// "Inspired" by https://golang.org/src/encoding/json/encode.go
// Copyright 2010 The Go Authors. All rights reserved.
func typeFields(t reflect.Type) structFields {
	index := make(map[string]int, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("parquet")
		if tag == "-" {
			continue
		}

		name := field.Name
		if s := tagName(tag); s != "" {
			name = s
		}
		index[name] = i
	}
	return index
}
