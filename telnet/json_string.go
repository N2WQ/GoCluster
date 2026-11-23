// File role: JSON v1 string encoding for the connection writer. Oversize
// records can span socket writes while retaining a fixed amount of scratch.
package telnet

import (
	"encoding/json/jsontext"
	"io"
	"unicode/utf8"
)

const (
	jsonQuoteChunkBytes = 1024
	// The schema 7 renderer produces UTF-8 YAML. Its only raw ASCII controls
	// are tab and line endings. Each of those, a quote, or a backslash takes
	// two JSON bytes; other rendered bytes need no expansion.
	maxJSONYAMLRecordBytes = 2*maxV7YAMLBytes + len(`["yaml",""]`+"\n")
)

// One worst-case chunk of C0 controls encodes as six bytes per input byte.
// Scratch belongs to one writer and is reusable after each Write returns.
type jsonQuoteScratch [6*jsonQuoteChunkBytes + 2]byte

// appendJSONString appends one complete quoted value for an ordinary record.
// jsontext replaces each invalid input byte. Count only on that exceptional
// path so valid values need no second scan.
func appendJSONString[S ~[]byte | ~string](dst []byte, src S) ([]byte, int) {
	quoted, err := jsontext.AppendQuote(dst, src)
	if err == nil {
		return quoted, 0
	}
	invalid := 0
	for len(src) > 0 {
		r, size := utf8.DecodeRuneInString(string(src[:min(len(src), utf8.UTFMax)]))
		if r == utf8.RuneError && size == 1 {
			invalid++
		}
		src = src[size:]
	}
	return quoted, invalid
}

// A valid rune crossing the cut starts at most three bytes before it. Keep
// it intact even beside malformed continuation bytes, which must still count
// individually and cannot force the scratch to grow.
func jsonQuoteChunkLen[S ~[]byte | ~string](s S) int {
	if len(s) <= jsonQuoteChunkBytes {
		return len(s)
	}
	if !utf8.RuneStart(s[jsonQuoteChunkBytes]) {
		for i := jsonQuoteChunkBytes - 1; i > jsonQuoteChunkBytes-utf8.UTFMax; i-- {
			if utf8.RuneStart(s[i]) {
				_, size := utf8.DecodeRuneInString(string(s[i:min(len(s), i+utf8.UTFMax)]))
				if i+size > jsonQuoteChunkBytes {
					return i
				}
				break
			}
		}
	}
	return jsonQuoteChunkBytes
}

// writeJSONString emits one quoted string, including both quotes. The caller
// owns record ordering and must abandon the connection on any write failure.
func writeJSONString[S ~[]byte | ~string](w io.Writer, scratch *jsonQuoteScratch, src S) (int, error) {
	invalid := 0
	first := true
	for {
		n := jsonQuoteChunkLen(src)
		part, replacements := appendJSONString(scratch[:0], src[:n])
		invalid += replacements
		src = src[n:]
		if !first {
			part = part[1:]
		}
		if len(src) > 0 {
			part = part[:len(part)-1]
		}
		written, err := w.Write(part)
		if err != nil {
			return invalid, err
		}
		if written != len(part) {
			return invalid, io.ErrShortWrite
		}
		if len(src) == 0 {
			return invalid, nil
		}
		first = false
	}
}

// writeJSONYAML wraps the complete CRLF document from the selected schema renderer.
// Validate the original frame before emitting anything. Its reservation and
// the final flush belong to the connection writer, including on discard.
func writeJSONYAML(w io.Writer, scratch *jsonQuoteScratch, raw []byte) (int, error) {
	if len(raw) > maxV7YAMLBytes {
		return 0, errReadbackTooLarge
	}
	const prefix = `["yaml",`
	if n, err := io.WriteString(w, prefix); err != nil {
		return 0, err
	} else if n != len(prefix) {
		return 0, io.ErrShortWrite
	}
	invalid, err := writeJSONString(w, scratch, raw)
	if err != nil {
		return 0, err
	}
	const suffix = "]\n"
	if n, err := io.WriteString(w, suffix); err != nil {
		return 0, err
	} else if n != len(suffix) {
		return 0, io.ErrShortWrite
	}
	return invalid, nil
}
