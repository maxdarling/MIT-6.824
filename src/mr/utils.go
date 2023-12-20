package mr

import (
	"strings"
)

// helper for decoding things like "mr-out-5-12" and "map-1503"
func GetLastToken(s string, delim string) string {
	tokens := strings.Split(s, delim)
	return tokens[len(tokens)-1]
}
