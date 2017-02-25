// +build windows

package files

import (
	"path/filepath"
	"strings"
	"syscall"
)

func IsHidden(f File) bool {

	return IsHiddenPath(f.FullPath())
}

func IsHiddenPath(fullPath string) bool {
	fName := filepath.Base(fullPath)

	if strings.HasPrefix(fName, ".") && len(fName) > 1 {
		return true
	}

	p, e := syscall.UTF16PtrFromString(fullPath)
	if e != nil {
		return false
	}

	attrs, e := syscall.GetFileAttributes(p)
	if e != nil {
		return false
	}
	return attrs&syscall.FILE_ATTRIBUTE_HIDDEN != 0
}
