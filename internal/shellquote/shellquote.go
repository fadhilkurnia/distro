package shellquote

import "strings"

// Wraps a string in single quotes and converting all ' in that string into \'
func Quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
