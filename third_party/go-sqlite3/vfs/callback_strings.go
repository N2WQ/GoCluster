package vfs

import "github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"

// Borrow only for the current serialized callback; no returned span is retained
// or used across an engine entry. SQL expressions can create values much larger
// than the input SQL, so copying a string before identifying it is not bounded
// by the configured DSN's length. Preserve ReadString's pointer/NUL checks.
func callbackString(mem *sqlite3_wrap.Memory, ptr ptr_t, maxlen int64) []byte {
	return mem.BorrowStringBytes(ptr, maxlen)
}

func callbackParameter(mem *sqlite3_wrap.Memory, ptr ptr_t, key string) []byte {
	_, value := callbackParameterPosition(mem, ptr, key)
	return value
}

func callbackParameterPosition(mem *sqlite3_wrap.Memory, ptr ptr_t, key string) (ptr_t, []byte) {
	for {
		// URI expressions can exceed the public API's one-million-byte limit.
		// Borrowing is bounded by the already-owned fixed engine extent.
		k := callbackString(mem, ptr, int64(len(mem.Buf)))
		if len(k) == 0 {
			return 0, nil
		}
		keyPtr := ptr
		ptr += ptr_t(len(k)) + 1
		v := callbackString(mem, ptr, int64(len(mem.Buf)))
		if string(k) == key {
			return keyPtr, v
		}
		ptr += ptr_t(len(v)) + 1
	}
}

// This is a representation for the existing ParseBool result, not truncation
// of a stored SQL value. Numeric prefixes alone determine their result; no
// accepted nonnumeric word can exceed20UTF8 bytes before simple lowercase.
func callbackBoolString(value []byte) string {
	if len(value) != 0 && value[0] >= '0' && value[0] <= '9' {
		return string(value[:1])
	}
	if len(value) > 4*len("false") {
		return ""
	}
	return string(value)
}
