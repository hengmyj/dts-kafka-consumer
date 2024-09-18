// Code generated by github.com/actgardner/gogen-avro/v7. DO NOT EDIT.
/*
 * SOURCE:
 *     Record.avsc
 */
package dtsavro

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/actgardner/gogen-avro/v7/vm"
	"github.com/actgardner/gogen-avro/v7/vm/types"
)

type UnionNullArrayLongTypeEnum int

const (
	UnionNullArrayLongTypeEnumArrayLong UnionNullArrayLongTypeEnum = 1
)

type UnionNullArrayLong struct {
	Null      *types.NullVal
	ArrayLong []int64
	UnionType UnionNullArrayLongTypeEnum
}

func writeUnionNullArrayLong(r *UnionNullArrayLong, w io.Writer) error {

	if r == nil {
		err := vm.WriteLong(0, w)
		return err
	}

	err := vm.WriteLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullArrayLongTypeEnumArrayLong:
		return writeArrayLong(r.ArrayLong, w)
	}
	return fmt.Errorf("invalid value for *UnionNullArrayLong")
}

func NewUnionNullArrayLong() *UnionNullArrayLong {
	return &UnionNullArrayLong{}
}

func (_ *UnionNullArrayLong) SetBoolean(v bool)   { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetInt(v int32)      { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetFloat(v float32)  { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetDouble(v float64) { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetBytes(v []byte)   { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetString(v string)  { panic("Unsupported operation") }
func (r *UnionNullArrayLong) SetLong(v int64) {
	r.UnionType = (UnionNullArrayLongTypeEnum)(v)
}
func (r *UnionNullArrayLong) Get(i int) types.Field {
	switch i {
	case 0:
		return r.Null
	case 1:
		r.ArrayLong = make([]int64, 0)
		return &ArrayLongWrapper{Target: (&r.ArrayLong)}
	}
	panic("Unknown field index")
}
func (_ *UnionNullArrayLong) NullField(i int)                  { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) SetDefault(i int)                 { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) AppendArray() types.Field         { panic("Unsupported operation") }
func (_ *UnionNullArrayLong) Finalize()                        {}

func (r *UnionNullArrayLong) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	switch r.UnionType {
	case UnionNullArrayLongTypeEnumArrayLong:
		return json.Marshal(map[string]interface{}{"array": r.ArrayLong})
	}
	return nil, fmt.Errorf("invalid value for *UnionNullArrayLong")
}

func (r *UnionNullArrayLong) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if value, ok := fields["array"]; ok {
		r.UnionType = 1
		return json.Unmarshal([]byte(value), &r.ArrayLong)
	}
	return fmt.Errorf("invalid value for *UnionNullArrayLong")
}
