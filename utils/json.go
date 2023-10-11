package utils

import "encoding/json"

// MarshalToString marshals v into a string.
func MarshalToString(v any) (string, error) {
	data, err := Marshal(v)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// Marshal marshals v into json bytes.
func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
