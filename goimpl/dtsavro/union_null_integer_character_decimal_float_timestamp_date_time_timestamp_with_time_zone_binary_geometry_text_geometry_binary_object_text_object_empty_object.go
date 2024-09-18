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

type UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum int

const (
	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumInteger UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 1

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumCharacter UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 2

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDecimal UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 3

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumFloat UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 4

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestamp UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 5

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDateTime UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 6

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestampWithTimeZone UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 7

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryGeometry UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 8

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextGeometry UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 9

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryObject UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 10

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextObject UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 11

	UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumEmptyObject UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum = 12
)

type UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject struct {
	Null                  *types.NullVal
	Integer               *Integer
	Character             *Character
	Decimal               *Decimal
	Float                 *Float
	Timestamp             *Timestamp
	DateTime              *DateTime
	TimestampWithTimeZone *TimestampWithTimeZone
	BinaryGeometry        *BinaryGeometry
	TextGeometry          *TextGeometry
	BinaryObject          *BinaryObject
	TextObject            *TextObject
	EmptyObject           EmptyObject
	UnionType             UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum
}

func writeUnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject(r *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject, w io.Writer) error {

	if r == nil {
		err := vm.WriteLong(0, w)
		return err
	}

	err := vm.WriteLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumInteger:
		return writeInteger(r.Integer, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumCharacter:
		return writeCharacter(r.Character, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDecimal:
		return writeDecimal(r.Decimal, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumFloat:
		return writeFloat(r.Float, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestamp:
		return writeTimestamp(r.Timestamp, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDateTime:
		return writeDateTime(r.DateTime, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestampWithTimeZone:
		return writeTimestampWithTimeZone(r.TimestampWithTimeZone, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryGeometry:
		return writeBinaryGeometry(r.BinaryGeometry, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextGeometry:
		return writeTextGeometry(r.TextGeometry, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryObject:
		return writeBinaryObject(r.BinaryObject, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextObject:
		return writeTextObject(r.TextObject, w)
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumEmptyObject:
		return writeEmptyObject(r.EmptyObject, w)
	}
	return fmt.Errorf("invalid value for *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject")
}

func NewUnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject() *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject {
	return &UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject{}
}

func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetBoolean(v bool) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetInt(v int32) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetFloat(v float32) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetDouble(v float64) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetBytes(v []byte) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetString(v string) {
	panic("Unsupported operation")
}
func (r *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetLong(v int64) {
	r.UnionType = (UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnum)(v)
}
func (r *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) Get(i int) types.Field {
	switch i {
	case 0:
		return r.Null
	case 1:
		r.Integer = NewInteger()
		return r.Integer
	case 2:
		r.Character = NewCharacter()
		return r.Character
	case 3:
		r.Decimal = NewDecimal()
		return r.Decimal
	case 4:
		r.Float = NewFloat()
		return r.Float
	case 5:
		r.Timestamp = NewTimestamp()
		return r.Timestamp
	case 6:
		r.DateTime = NewDateTime()
		return r.DateTime
	case 7:
		r.TimestampWithTimeZone = NewTimestampWithTimeZone()
		return r.TimestampWithTimeZone
	case 8:
		r.BinaryGeometry = NewBinaryGeometry()
		return r.BinaryGeometry
	case 9:
		r.TextGeometry = NewTextGeometry()
		return r.TextGeometry
	case 10:
		r.BinaryObject = NewBinaryObject()
		return r.BinaryObject
	case 11:
		r.TextObject = NewTextObject()
		return r.TextObject
	case 12:
		return &EmptyObjectWrapper{Target: (&r.EmptyObject)}
	}
	panic("Unknown field index")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) NullField(i int) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) SetDefault(i int) {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) AppendMap(key string) types.Field {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) AppendArray() types.Field {
	panic("Unsupported operation")
}
func (_ *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) Finalize() {
}

func (r *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	switch r.UnionType {
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumInteger:
		return json.Marshal(map[string]interface{}{"Integer": r.Integer})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumCharacter:
		return json.Marshal(map[string]interface{}{"Character": r.Character})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDecimal:
		return json.Marshal(map[string]interface{}{"Decimal": r.Decimal})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumFloat:
		return json.Marshal(map[string]interface{}{"Float": r.Float})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestamp:
		return json.Marshal(map[string]interface{}{"Timestamp": r.Timestamp})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumDateTime:
		return json.Marshal(map[string]interface{}{"DateTime": r.DateTime})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTimestampWithTimeZone:
		return json.Marshal(map[string]interface{}{"TimestampWithTimeZone": r.TimestampWithTimeZone})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryGeometry:
		return json.Marshal(map[string]interface{}{"BinaryGeometry": r.BinaryGeometry})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextGeometry:
		return json.Marshal(map[string]interface{}{"TextGeometry": r.TextGeometry})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumBinaryObject:
		return json.Marshal(map[string]interface{}{"BinaryObject": r.BinaryObject})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumTextObject:
		return json.Marshal(map[string]interface{}{"TextObject": r.TextObject})
	case UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObjectTypeEnumEmptyObject:
		return json.Marshal(map[string]interface{}{"EmptyObject": r.EmptyObject})
	}
	return nil, fmt.Errorf("invalid value for *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject")
}

func (r *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if value, ok := fields["Integer"]; ok {
		r.UnionType = 1
		return json.Unmarshal([]byte(value), &r.Integer)
	}
	if value, ok := fields["Character"]; ok {
		r.UnionType = 2
		return json.Unmarshal([]byte(value), &r.Character)
	}
	if value, ok := fields["Decimal"]; ok {
		r.UnionType = 3
		return json.Unmarshal([]byte(value), &r.Decimal)
	}
	if value, ok := fields["Float"]; ok {
		r.UnionType = 4
		return json.Unmarshal([]byte(value), &r.Float)
	}
	if value, ok := fields["Timestamp"]; ok {
		r.UnionType = 5
		return json.Unmarshal([]byte(value), &r.Timestamp)
	}
	if value, ok := fields["DateTime"]; ok {
		r.UnionType = 6
		return json.Unmarshal([]byte(value), &r.DateTime)
	}
	if value, ok := fields["TimestampWithTimeZone"]; ok {
		r.UnionType = 7
		return json.Unmarshal([]byte(value), &r.TimestampWithTimeZone)
	}
	if value, ok := fields["BinaryGeometry"]; ok {
		r.UnionType = 8
		return json.Unmarshal([]byte(value), &r.BinaryGeometry)
	}
	if value, ok := fields["TextGeometry"]; ok {
		r.UnionType = 9
		return json.Unmarshal([]byte(value), &r.TextGeometry)
	}
	if value, ok := fields["BinaryObject"]; ok {
		r.UnionType = 10
		return json.Unmarshal([]byte(value), &r.BinaryObject)
	}
	if value, ok := fields["TextObject"]; ok {
		r.UnionType = 11
		return json.Unmarshal([]byte(value), &r.TextObject)
	}
	if value, ok := fields["EmptyObject"]; ok {
		r.UnionType = 12
		return json.Unmarshal([]byte(value), &r.EmptyObject)
	}
	return fmt.Errorf("invalid value for *UnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject")
}