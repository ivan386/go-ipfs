// +build !windows

package files

import (
	"path/filepath"
	"strings"
)

func IsHidden(f File) bool {
	return IsHiddenPath(f.FullPath())
}

func IsHiddenPath(fullPath string) bool {

	fName := filepath.Base(fullPath)

	if strings.HasPrefix(fName, ".") && len(fName) > 1 {
		return true
	}

	return false
}
