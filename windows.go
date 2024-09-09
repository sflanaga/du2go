//go:build windows
// +build windows

package main

import (
	"fmt"
	"io/fs"
	"syscall"
)

func getUserId(fileInfo *fs.FileInfo) uint32 {
	var uid uint32
	fileBasicInfo, err := syscall.GetFileInformationByHandle(fileInfo.Sys().(*syscall.Handle))
	if err != nil {
		return 0, fmt.Errorf("failed to get file information: %w", err)
	}
	uid = fileBasicInfo.Owner.LowPart
	return uid
}
