package store

import (
	"fmt"

	"github.com/mr-tron/base58"
)

func MustBase58Decode(s string) []byte {
	data, err := base58.Decode(s)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode base58 %q: %s", s, err))
	}
	return data
}
