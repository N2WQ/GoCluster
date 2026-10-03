package sqlite3

import (
	sqlite3_wasm "github.com/ncruces/go-sqlite3-wasm/v6"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

func TestV15SQLiteHostInventory(t *testing.T) {
	fileType := reflect.TypeOf(os.File{})
	fileImpl := fileType.Field(0).Type.Elem()
	t.Logf("Conn=%d Stmt=%d Wrapper=%d Memory=%d Arena=%d Module=%d os.File=%d os.file=%d", unsafe.Sizeof(Conn{}), unsafe.Sizeof(Stmt{}), unsafe.Sizeof(sqlite3_wrap.Wrapper{}), unsafe.Sizeof(sqlite3_wrap.Memory{}), unsafe.Sizeof(sqlite3_wrap.Arena{}), unsafe.Sizeof(sqlite3_wasm.Module{}), fileType.Size(), fileImpl.Size())
	if fileType.Size()+fileImpl.Size() > 1024 {
		t.Fatal("OS file metadata exceeded inventory allowance")
	}
	if unsafe.Sizeof(Conn{})+unsafe.Sizeof(sqlite3_wrap.Wrapper{})+unsafe.Sizeof(sqlite3_wrap.Memory{}) > 64<<10 {
		t.Fatal("fixed owner structures exceeded inventory allowance")
	}
}
