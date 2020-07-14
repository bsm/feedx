package parquet

import (
	"fmt"
	"io"
	"reflect"

	kpq "github.com/kostya-sh/parquet-go/parquet"
)

type decoder struct {
	cols    []*columnReader
	closers []io.Closer
}

func newDecoder(rs io.ReadSeeker, names []string, batchSize int) (*decoder, error) {
	file, err := kpq.FileFromReader(rs)
	if err != nil {
		return nil, err
	}

	// normalise column names
	if len(names) == 0 {
		for _, c := range file.Schema.Columns() {
			names = append(names, c.String())
		}
	}

	// normalise batch size
	if batchSize < 1 {
		batchSize = 1000
	}

	// initialise column buffers
	cols := make([]*columnReader, 0, len(names))
	for _, name := range names {
		col, ok := file.Schema.ColumnByName(name)
		if !ok {
			_ = file.Close()
			return nil, fmt.Errorf("column %q does not exist", name)
		}
		cols = append(cols, newColumnReader(file, col, batchSize))
	}

	return &decoder{cols: cols, closers: []io.Closer{file}}, nil
}

func (w *decoder) Decode(v interface{}) error {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("cannot decode non-pointer %s type", rt.String())
	}

	// field index by name
	fidx := cachedTypeFields(rt.Elem())
	elem := rv.Elem()

	for _, r := range w.cols {
		// next column value
		val, err := r.Next()
		if err != nil {
			return err
		}

		// skip if value is NULL
		if val == nil {
			continue
		}

		// set field if exists
		if fi, ok := fidx[r.Name()]; ok {
			if err := setValue(elem.Field(fi), val); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *decoder) Close() (err error) {
	for _, c := range w.closers {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return
}

// --------------------------------------------------------------------

func setValue(rv reflect.Value, v interface{}) error {
	switch vv := v.(type) {
	case bool:
		switch rv.Kind() {
		case reflect.Bool:
			rv.SetBool(vv)
			return nil
		}
	case []byte:
		switch rv.Kind() {
		case reflect.String:
			rv.SetString(string(vv))
			return nil
		case reflect.Slice:
			if rv.Type() == byteSliceType {
				rv.SetBytes(vv)
				return nil
			}
		}
	case int, int8, int16, int32, int64:
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rv.SetInt(reflect.ValueOf(v).Int())
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rv.SetUint(uint64(reflect.ValueOf(v).Int()))
			return nil
		}
	case uint, uint8, uint16, uint32, uint64:
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rv.SetInt(int64(reflect.ValueOf(v).Uint()))
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rv.SetUint(reflect.ValueOf(v).Uint())
			return nil
		}
	case float32, float64:
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			rv.SetFloat(reflect.ValueOf(v).Float())
			return nil
		}
	case kpq.Int96:
		if rv.Type() == int96Type {
			rv.Set(reflect.ValueOf(v))
			return nil
		}
	}
	return fmt.Errorf("cannot assign value of type %T to %s", v, rv.Type())
}

var (
	byteSliceType = reflect.TypeOf(([]byte)(nil))
	int96Type     = reflect.TypeOf(kpq.Int96{})
)
