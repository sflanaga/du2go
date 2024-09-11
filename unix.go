//go:build darwin || freebsd || netbsd || openbsd || linux
// +build darwin freebsd netbsd openbsd linux

package main

import (
	"io/fs"
	"syscall"
)

func getUserId(fileInfo *fs.FileInfo) uint32 {
	uid := (*fileInfo).Sys().(*syscall.Stat_t).Uid
	return uid
}
