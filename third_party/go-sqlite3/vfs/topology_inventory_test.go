//go:build windows || linux

package vfs

import (
	"testing"
	"unsafe"
)

func TestV15SQLiteVFSHostInventory(t *testing.T) {
	t.Logf("vfsFile=%d vfsShm=%d cksmFile=%d Filename=%d", unsafe.Sizeof(vfsFile{}), unsafe.Sizeof(vfsShm{}), unsafe.Sizeof(cksmFile{}), unsafe.Sizeof(Filename{}))
	if unsafe.Sizeof(vfsFile{})+unsafe.Sizeof(vfsShm{})+unsafe.Sizeof(cksmFile{}) > 2048 {
		t.Fatal("fixed VFS metadata exceeded inventory allowance")
	}
}
