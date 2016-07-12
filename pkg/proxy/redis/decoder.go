// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

var (
	ErrBadRespCRLFEnd  = errors.New("bad resp CRLF end")
	ErrBadRespBytesLen = errors.New("bad resp bytes len")
	ErrBadRespArrayLen = errors.New("bad resp array len")
)

func btoi(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errors.Trace(err)
	} else {
		return n, nil
	}
}

type Decoder struct {
	*bufio.Reader

	Err error
}

func NewDecoder(br *bufio.Reader) *Decoder {
	return &Decoder{Reader: br}
}

func NewDecoderSize(r io.Reader, size int) *Decoder {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, size)
	}
	return &Decoder{Reader: br}
}

func (d *Decoder) Decode() (*Resp, error) {
	if d.Err != nil {
		return nil, d.Err
	}
	r, err := d.decodeResp(0)
	if err != nil {
		d.Err = err
	}
	return r, err
}

func Decode(br *bufio.Reader) (*Resp, error) {
	return NewDecoder(br).Decode()
}

func DecodeFromBytes(p []byte) (*Resp, error) {
	return Decode(bufio.NewReader(bytes.NewReader(p)))
}

func (d *Decoder) decodeResp(depth int) (*Resp, error) {
	b, err := d.ReadByte() // 读1个字节
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch t := RespType(b); t {
	case TypeString, TypeError, TypeInt: // SIMPLE STRING, ERROR, INT
		r := &Resp{Type: t}
		r.Value, err = d.decodeTextBytes()
		return r, err
	case TypeBulkBytes: // BULK STRINGS
		r := &Resp{Type: t}
		r.Value, err = d.decodeBulkBytes()
		return r, err
	case TypeArray: // ARRAY
		r := &Resp{Type: t}
		r.Array, err = d.decodeArray(depth)
		return r, err
	default: // UNKNOWN
		if depth != 0 {
			return nil, errors.Errorf("bad resp type %s", t)
		}
		if err := d.UnreadByte(); err != nil {
			return nil, errors.Trace(err)
		}
		r := &Resp{Type: TypeArray}
		r.Array, err = d.decodeSingleLineBulkBytesArray()
		return r, err
	}
}

/******************************************************************************
 **函数名称: decodeTextBytes
 **功    能: 接收RESP SIMPLE-STRING/ERROR/INT响应
 **输入参数: NONE
 **输出参数: NONE
 **返    回:
 **     []byte: 应答结果
 **     error: 错误描述
 **实现描述:
 **注意事项:
 **     + 表示是一个Simple Strings
 **     - 表示是错误信息Errors
 **     : 表示是整型数据
 **作    者: # Codis # XXXX.XX.XX #
 ******************************************************************************/
func (d *Decoder) decodeTextBytes() ([]byte, error) {
	b, err := d.ReadBytes('\n') // 读取多个字节, 知道'\n'结尾
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' { // 校验合法性
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	} else {
		return b[:n], nil // 返回结果
	}
}

func (d *Decoder) decodeTextString() (string, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *Decoder) decodeInt() (int64, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return 0, err
	}
	return btoi(b)
}

/******************************************************************************
 **函数名称: decodeBulkBytes
 **功    能: 接收RESP BULK STRINGS响应
 **输入参数: NONE
 **输出参数: NONE
 **返    回:
 **     resp: 应答结果
 **     err: 错误描述
 **实现描述:
 **注意事项:
 **     BULK STRINGS的使用是为了表示一个二进制安全的字符串.这个字符串的长度可以
 **     达到512MB.
 **     BULK STRINGS使用如下的形式进行编码:
 **      -> 用$符号后面跟着一个整数的形式组成一个字符串(长度前缀), 以CRLF结尾.
 **      -> 实际的字符串.
 **      -> 整个字符串后面使用CRLF结束.
 **     举例说明:
 **      -> 字符串“onmpw”的编码形式如下： “$5\r\nonmpw\r\n”
 **      -> 空字符串的形式如下：“$0\r\n\r\n”
 **      -> 除此之外, Bulk Strings还可以使用一个特殊的标识来表示一个不存在的值.
 **         这个值就是上面我们提到过的NULL值.其表示形式如下：“$-1\r\n”
 **         这被称为Null Bulk String.
 **         当请求的对象不存在的时候, 客户端的API不要返回一个空字符串, 应该返回
 **         一个nil对象.就好像一个Ruby库应该返回一个nil, C库应该返回一个NULL等等.
 **作    者: # Codis # XXXX.XX.XX #
 ******************************************************************************/
func (d *Decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt() // 读取1个整数
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespBytesLen)
	} else if n == -1 {
		return nil, nil
	}
	b := make([]byte, n+2)
	if _, err := io.ReadFull(d.Reader, b); err != nil { // 读取实际字符+"\r\n"
		return nil, errors.Trace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' { // 校验合法性
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	}
	return b[:n], nil
}

/******************************************************************************
 **函数名称: decodeArray
 **功    能: 接收RESP ARRAY响应
 **输入参数: NONE
 **输出参数: NONE
 **返    回:
 **     resp: 应答结果
 **     err: 错误描述
 **实现描述:
 **注意事项:
 **     客户端想Redis服务端发送命令的时候, 使用的是RESP 数组的形式. 并且还有一些
 **     特定的命令的返回结果也是一个元素的集合. 这些返回的结果也是用RESP数组的形
 **     式编码的. 比如说命令LRANGE就返回一些列的元素.
 **
 **     RESP Arrays使用如下的形式发送信息：
 **      -> 星号(*)作为第一个字节, 后面跟着一个十进制的整数, 接着是CRLF(\r\n).
 **      -> 对每一个数组的元素都有一个额外的RESP类型.
 **     举例说明:
 **      -> 空数组的形式如下:
 **         “*0\r\n”
 **      -> 对于有两个Bulk Strings元素foo和bar的Array的表示形式如下:
 **         “*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n”
 **      -> Arrays除了包含有Bulk Strings元素外还可以包含整型元素:
 **         “*3\r\n:1\r\n:2\r\n:3\r\n”
 **作    者: # Codis # XXXX.XX.XX #
 ******************************************************************************/
func (d *Decoder) decodeArray(depth int) ([]*Resp, error) {
	n, err := d.decodeInt() // 读取数组长度
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespArrayLen)
	} else if n == -1 {
		return nil, nil
	}
	a := make([]*Resp, n)
	for i := 0; i < len(a); i++ { // 依次读取数组各成员
		if a[i], err = d.decodeResp(depth + 1); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// 单行BULK STRINGS数组的处理
func (d *Decoder) decodeSingleLineBulkBytesArray() ([]*Resp, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return nil, err
	}
	a := make([]*Resp, 0, 4)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				a = append(a, &Resp{
					Type:  TypeBulkBytes,
					Value: b[l:r],
				})
			}
			l = r + 1
		}
	}
	return a, nil
}
