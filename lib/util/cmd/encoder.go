// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	_pool = buffer.NewPool()
)

type tidbEncoder struct {
	line *buffer.Buffer
	zapcore.EncoderConfig
	openNamespaces int
}

func NewTiDBEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	if cfg.ConsoleSeparator == "" {
		cfg.ConsoleSeparator = "\t"
	}
	if cfg.LineEnding == "" {
		cfg.LineEnding = zapcore.DefaultLineEnding
	}
	return &tidbEncoder{_pool.Get(), cfg, 0}
}

func (c tidbEncoder) clone() *tidbEncoder {
	return &tidbEncoder{_pool.Get(), c.EncoderConfig, 0}
}

func (c tidbEncoder) Clone() zapcore.Encoder {
	return c.clone()
}

func (c *tidbEncoder) beginQuoteFiled() {
	if c.line.Len() > 0 {
		c.line.AppendByte(' ')
	}
	c.line.AppendByte('[')
}
func (c *tidbEncoder) endQuoteFiled() {
	c.line.AppendByte(']')
}
func (c *tidbEncoder) encodeError(f zapcore.Field) {
	err := f.Interface.(error)
	basic := err.Error()
	c.AddString(f.Key, basic)
	if e, isFormatter := err.(fmt.Formatter); isFormatter {
		verbose := fmt.Sprintf("%+v", e)
		if verbose != basic {
			// This is a rich error type, like those produced by github.com/pkg/errors.
			c.AddString(f.Key+"Verbose", verbose)
		}
	}
}
func (e *tidbEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	c := e.clone()
	if c.TimeKey != "" {
		c.beginQuoteFiled()
		if c.EncodeTime != nil {
			c.EncodeTime(ent.Time, c)
		} else {
			c.AppendString(ent.Time.Format("2006/01/02 15:04:05.000 -07:00"))
		}
		c.endQuoteFiled()
	}
	if c.LevelKey != "" && c.EncodeLevel != nil {
		c.beginQuoteFiled()
		c.EncodeLevel(ent.Level, c)
		c.endQuoteFiled()
	}
	if ent.LoggerName != "" && c.NameKey != "" {
		c.beginQuoteFiled()
		nameEncoder := c.EncodeName
		if nameEncoder == nil {
			nameEncoder = zapcore.FullNameEncoder
		}
		nameEncoder(ent.LoggerName, c)
		c.endQuoteFiled()
	}
	if ent.Caller.Defined {
		c.beginQuoteFiled()
		if c.CallerKey != "" && c.EncodeCaller != nil {
			c.EncodeCaller(ent.Caller, c)
		}
		if c.FunctionKey != "" {
			c.AppendString(ent.Caller.Function)
		}
		c.endQuoteFiled()
	}

	// Add the message itself.
	if c.MessageKey != "" {
		c.beginQuoteFiled()
		c.line.AppendString(ent.Message)
		c.endQuoteFiled()
	}

	if c.line.Len() > 0 {
		c.line.AppendByte(' ')
	}

	// append the old fields
	c.line.WriteString(e.line.String())

	for _, f := range fields {
		if f.Type == zapcore.ErrorType {
			// handle ErrorType in pingcap/log to fix "[key=?,keyVerbose=?]" problem.
			// see more detail at https://github.com/pingcap/log/pull/5
			c.encodeError(f)
			continue
		}
		f.AddTo(c)
	}

	c.closeOpenNamespaces()

	if ent.Stack != "" && c.StacktraceKey != "" {
		c.beginQuoteFiled()
		c.line.AppendString(ent.Stack)
		c.endQuoteFiled()
	}

	c.line.AppendString(c.LineEnding)

	return c.line, nil
}

/* map encoder part */
func (f *tidbEncoder) safeAddString(s string) {
	needQuotes := false
outerloop:
	for _, b := range s {
		if b <= 0x20 {
			needQuotes = true
			break outerloop
		}
		switch b {
		case '\\', '"', '[', ']', '=':
			needQuotes = true
			break outerloop
		}
	}

	if needQuotes {
		f.line.AppendByte('"')
	}

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError {
			f.line.AppendString(`\ufffd`)
		} else if size == 1 {
			switch r {
			case '\\', '"', '\n', '\r', '\t':
				f.line.AppendByte('\\')
				f.line.AppendByte(s[i])
			default:
				if r >= 0x20 {
					f.line.AppendByte(s[i])
				} else {
					f.line.AppendString(`\u`)
					fmt.Fprintf(f.line, "%4x", r)
				}
			}
		} else {
			f.line.AppendString(s[i : i+size])
		}
		i += size
	}

	if needQuotes {
		f.line.AppendByte('"')
	}
}
func (s *tidbEncoder) addKey(key string) {
	s.addElementSeparator()
	s.safeAddString(key)
	s.line.AppendByte('=')
}
func (s *tidbEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	s.beginQuoteFiled()
	s.addKey(key)
	err := s.AppendArray(arr)
	s.endQuoteFiled()
	return err
}
func (s *tidbEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	s.beginQuoteFiled()
	s.addKey(key)
	err := s.AppendObject(obj)
	s.endQuoteFiled()
	return err
}
func (s *tidbEncoder) AddBinary(key string, val []byte) {
	s.AddString(key, base64.StdEncoding.EncodeToString(val))
}
func (s *tidbEncoder) AddByteString(key string, val []byte) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendByteString(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddBool(key string, val bool) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendBool(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddComplex128(key string, val complex128) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendComplex128(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddComplex64(key string, val complex64) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendComplex64(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddDuration(key string, val time.Duration) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendDuration(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddFloat64(key string, val float64) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendFloat64(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddFloat32(key string, val float32) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendFloat32(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddInt(key string, val int) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendInt(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddInt8(key string, val int8) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendInt8(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddInt16(key string, val int16) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendInt16(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddInt32(key string, val int32) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendInt32(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddInt64(key string, val int64) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendInt64(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddString(key string, val string) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendString(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddTime(key string, val time.Time) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendTime(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUint(key string, val uint) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUint(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUint8(key string, val uint8) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUint8(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUint16(key string, val uint16) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUint16(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUint32(key string, val uint32) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUint32(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUint64(key string, val uint64) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUint64(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddUintptr(key string, val uintptr) {
	s.beginQuoteFiled()
	s.addKey(key)
	s.AppendUintptr(val)
	s.endQuoteFiled()
}
func (s *tidbEncoder) AddReflected(key string, obj interface{}) error {
	s.beginQuoteFiled()
	s.addKey(key)
	enc := json.NewEncoder(s.line)
	if err := enc.Encode(obj); err != nil {
		return err
	}
	s.line.TrimNewline()
	s.endQuoteFiled()
	return nil
}
func (s *tidbEncoder) OpenNamespace(key string) {
	s.addKey(key)
	s.line.AppendByte('{')
	s.openNamespaces++
}
func (s *tidbEncoder) closeOpenNamespaces() {
	for i := 0; i < s.openNamespaces; i++ {
		s.line.AppendByte('}')
	}
}

/* array encoder part */
func (s *tidbEncoder) addElementSeparator() {
	length := s.line.Len()
	if length == 0 {
		return
	}
	switch s.line.Bytes()[length-1] {
	case '{', '[', ':', ',', ' ', '=':
	default:
		s.line.AppendByte(',')
	}
}
func (s *tidbEncoder) AppendArray(v zapcore.ArrayMarshaler) error {
	s.addElementSeparator()
	s.line.AppendByte('[')
	if err := v.MarshalLogArray(s); err != nil {
		return err
	}
	s.line.AppendByte(']')
	return nil
}
func (s *tidbEncoder) AppendObject(v zapcore.ObjectMarshaler) error {
	s.addElementSeparator()
	s.line.AppendByte('{')
	if err := v.MarshalLogObject(s); err != nil {
		return err
	}
	s.line.WriteByte('}')
	return nil
}
func (s *tidbEncoder) AppendReflected(v interface{}) error {
	s.addElementSeparator()
	_, err := fmt.Fprint(s.line, v)
	return err
}
func (s *tidbEncoder) AppendBool(v bool) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendByteString(v []byte) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendComplex128(v complex128) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendComplex64(v complex64) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendDuration(v time.Duration) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendFloat64(v float64) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendFloat32(v float32) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendInt(v int) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendInt64(v int64) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendInt32(v int32) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendInt16(v int16) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendInt8(v int8) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendString(v string) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendTime(v time.Time) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUint(v uint) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUint64(v uint64) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUint32(v uint32) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUint16(v uint16) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUint8(v uint8) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
func (s *tidbEncoder) AppendUintptr(v uintptr) {
	s.addElementSeparator()
	fmt.Fprint(s.line, v)
}
