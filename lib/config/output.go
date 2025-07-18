// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

type output struct {
	Component  string
	Version    string
	Parameters []field
}

type field struct {
	Name          string `json:"key"`
	Type          string `json:"type"`
	DefaultValue  any    `json:"default_value"`
	HotReloadable bool   `json:"hot_reloadable"`
}

// ConfigInfo outputs the config info used for loading to the operation system.
func ConfigInfo(format string) (string, error) {
	if !strings.EqualFold(format, "json") {
		return "", errors.New("only support json format")
	}
	fields := make([]field, 0, 256)
	cfg := NewConfig()
	res, err := formatValue(reflect.ValueOf(*cfg), "", true)
	if err != nil {
		return "", err
	}
	fields = append(fields, res...)

	op := output{
		Component:  "TiProxy Server",
		Version:    "",
		Parameters: fields,
	}
	bytes, err := json.MarshalIndent(op, "", "    ")
	return string(bytes), err
}

func formatValue(v reflect.Value, name string, reloadable bool) ([]field, error) {
	if !v.IsValid() {
		return nil, errors.New("invalid value")
	}

	if v.Kind() == reflect.Interface {
		return formatValue(v.Elem(), name, reloadable)
	}

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		return formatValue(v.Elem(), name, reloadable)
	}

	switch v.Kind() {
	case reflect.Bool:
		return []field{{Name: name, Type: "bool", DefaultValue: v.Bool(), HotReloadable: reloadable}}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []field{{Name: name, Type: "int", DefaultValue: v.Int(), HotReloadable: reloadable}}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []field{{Name: name, Type: "int", DefaultValue: v.Uint(), HotReloadable: reloadable}}, nil
	case reflect.Float32, reflect.Float64:
		return []field{{Name: name, Type: "float", DefaultValue: v.Float(), HotReloadable: reloadable}}, nil
	case reflect.String:
		return []field{{Name: name, Type: "string", DefaultValue: v.String(), HotReloadable: reloadable}}, nil
	case reflect.Slice, reflect.Array:
		return formatSliceOrArray(v, name, reloadable)
	case reflect.Struct:
		return formatStruct(v, name)
	case reflect.Map:
		return formatMap(v, name, reloadable)
	default:
		return nil, errors.Errorf("unsupported type %s", v.Kind().String())
	}
}

func formatSliceOrArray(v reflect.Value, name string, reloadable bool) ([]field, error) {
	length := v.Len()
	elements := make([]string, 0, length)
	for i := 0; i < length; i++ {
		str, err := valueToString(v.Index(i))
		if err != nil {
			return nil, err
		}
		elements = append(elements, str)
	}
	return []field{{Name: name, Type: "json", DefaultValue: elements, HotReloadable: reloadable}}, nil
}

func formatStruct(v reflect.Value, name string) ([]field, error) {
	typ := v.Type()
	fields := make([]field, 0, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		jsonName := strings.Split(f.Tag.Get("json"), ",")[0]
		switch jsonName {
		case "-":
			continue
		case "":
			jsonName = name
		default:
			if name != "" {
				jsonName = name + "." + jsonName
			}
		}
		reloadable := f.Tag.Get("reloadable") == "true"
		res, err := formatValue(v.Field(i), jsonName, reloadable)
		if err != nil {
			return nil, err
		}
		fields = append(fields, res...)
	}
	return fields, nil
}

func formatMap(v reflect.Value, name string, reloadable bool) ([]field, error) {
	entries := make(map[string]string)
	if v.IsNil() || v.Len() == 0 {
		return []field{{Name: name, Type: "json", DefaultValue: entries, HotReloadable: reloadable}}, nil
	}

	for _, key := range v.MapKeys() {
		val := v.MapIndex(key)
		keyStr, err := valueToString(key)
		if err != nil {
			return nil, err
		}
		valStr, err := valueToString(val)
		if err != nil {
			return nil, err
		}
		entries[keyStr] = valStr
	}
	return []field{{Name: name, Type: "json", DefaultValue: entries, HotReloadable: reloadable}}, nil
}

func valueToString(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.String:
		return strconv.Quote(v.String()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64), nil
	case reflect.Bool:
		return strconv.FormatBool(v.Bool()), nil
	default:
		return "", errors.Errorf("unsupported type %s", v.Kind().String())
	}
}
