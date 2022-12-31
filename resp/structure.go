package resp

import "strconv"

var (
	CRLF = "\r\n"
)

type RedisData interface {
	ToBytes() []byte
	ByteData() []byte
}

type StringData struct {
	data string
}
type IntData struct {
	data int64
}
type ErrorData struct {
	data string
}
type BulkData struct {
	data []byte
}
type ArrayData struct {
	data []RedisData
}
type PlainData struct {
	data string
}

/*-------string data------------*/

func MakeStringData(data string) *StringData {
	return &StringData{
		data: data,
	}
}

func (r *StringData) ToBytes() []byte {
	return []byte("+" + r.data + CRLF)
}

func (r *StringData) Data() string {
	return r.data
}
func (r *StringData) ByteData() []byte {
	return []byte(r.data)
}

/*-----------------------bulk data----------------*/

func MakeBulkData(data []byte) *BulkData {
	return &BulkData{
		data: data,
	}
}

func (r *BulkData) ToBytes() []byte {
	return []byte("$" + strconv.Itoa(len(r.data)) + CRLF + string(r.data) + CRLF)
}
func (r *BulkData) Data() []byte {
	return r.data
}
func (r *BulkData) ByteData() []byte {
	return r.data
}

/*------------------Int Data-----------------*/

func MakeIntData(data int64) *IntData {
	return &IntData{
		data: data,
	}
}

func (r *IntData) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.data, 10) + CRLF)
}
func (r *IntData) Data() int64 {
	return r.data
}

func (r *IntData) ByteData() []byte {
	return []byte(strconv.FormatInt(r.data, 10))
}

/*----------------------Error Data----------------------*/

func MakeErrorData(data string) *ErrorData {
	return &ErrorData{
		data: data,
	}
}

func (r *ErrorData) ToBytes() []byte {
	return []byte("-" + r.data + CRLF)
}

func (r *ErrorData) Data() string {
	return r.data
}

func (r *ErrorData) ByteData() []byte {
	return []byte(r.data)
}

/*--------------------Array Data--------------------*/

func MakeArrayData(data []RedisData) *ArrayData {
	return &ArrayData{
		data: data,
	}
}

func MakeEmptyArrayData() *ArrayData {
	return &ArrayData{
		data: nil,
	}
}

func (r *ArrayData) ToBytes() []byte {
	if r.data == nil {
		return []byte("*-1" + CRLF)
	}
	result := []byte("*" + strconv.Itoa(len(r.data)) + CRLF)
	for _, ele := range r.data {
		result = append(result, ele.ToBytes()...)
	}
	return result
}

func (r *ArrayData) Data() []RedisData {
	return r.data
}

func (r *ArrayData) ToCommand() [][]byte {
	res := make([][]byte, 0)
	for _, v := range r.data {
		res = append(res, v.ByteData())
	}
	return res
}

/*-----------------Plain Data----------------*/

func MakePlainData(data string) *PlainData {
	return &PlainData{
		data: data,
	}
}
func (r *PlainData) ToBytes() []byte {
	return []byte(r.data + CRLF)
}
func (r *PlainData) Data() string {
	return r.data
}
func (r *PlainData) ByteData() []byte {
	return []byte(r.data)
}
