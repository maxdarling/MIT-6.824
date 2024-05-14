package mr

import (
	"os/exec"
	"strconv"
	"strings"
)

// helper for decoding things like "mr-out-5-12" and "map-1503"
func GetLastToken(s string, delim string) string {
	tokens := strings.Split(s, delim)
	return tokens[len(tokens)-1]
}

// split a file, keeping lines in tact, and return the constituent filenames.
// file prefix must be unique.
func SplitFile(file string, file_prefix string, chunkSizeKB int) []string {
	size_bytes := strconv.Itoa(chunkSizeKB * 1000)
	cmd := exec.Command(
		"gsplit", "-C", size_bytes, "--numeric-suffixes", "--suffix-length=3", file, file_prefix,
	)
	_, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	// cmd = exec.Command("ls", file_prefix+"*") // bugged for some reason...
	cmd = exec.Command("find", ".", "-name", file_prefix+"*")
	stdout, err2 := cmd.Output()
	if err2 != nil {
		panic(err2)
	}

	splitFn := func(c rune) bool {
		return c == '\n'
	}
	return strings.FieldsFunc(string(stdout), splitFn)
}
