package network

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Decoder consumes one complete response representation. It does not close r.
type Decoder func(r io.Reader, target any) error
type decoderContextKey struct{}

func defaultDecoders() map[string]Decoder {
	return map[string]Decoder{ContentTypeJSON: decodeJSON, ContentTypeAppXML: decodeXML, ContentTypeXML: decodeXML}
}
func decodeJSON(r io.Reader, target any) error {
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
func decodeXML(r io.Reader, target any) error {
	decoder := xml.NewDecoder(r)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch v := token.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(v)) != "" {
				return fmt.Errorf("unexpected XML trailing text")
			}
		case xml.Comment, xml.ProcInst:
		default:
			return fmt.Errorf("multiple XML values")
		}
	}
}

// StringOrBytes permits decoding strings and byte slices, including named types.
type StringOrBytes interface{ ~string | ~[]byte }

func ParseJSON[T any, I StringOrBytes](raw I) (*T, error) {
	target := new(T)
	if err := decodeJSON(strings.NewReader(string(raw)), target); err != nil {
		return nil, err
	}
	return target, nil
}
func ParseXML[T any, I StringOrBytes](raw I) (*T, error) {
	target := new(T)
	if err := decodeXML(strings.NewReader(string(raw)), target); err != nil {
		return nil, err
	}
	return target, nil
}
