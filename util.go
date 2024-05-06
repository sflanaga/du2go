package main

import (
	"fmt"
	"io/fs"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func formatDuration(d time.Duration) string {
	units := []struct {
		value time.Duration
		label string
	}{
		{24 * time.Hour, "d"}, // Days
		{time.Hour, "h"},      // Hours
		{time.Minute, "m"},    // Minutes
		{time.Second, "s"},    // Seconds
	}

	var result string

	for _, unit := range units {
		value := d / unit.value
		if value > 0 {
			result += fmt.Sprintf("%d%s", value, unit.label)
			d -= value * unit.value
		}
	}

	return result
}

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2fGB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2fMB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2fKB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

func modeToString(mode fs.FileMode) string {
	var str = ""
	if mode.IsDir() {
		str += "D"
	} else if mode.IsRegular() {
		str += "F"
	} else if mode&fs.ModeSymlink != 0 {
		str += "L"
	} else if mode&fs.ModeNamedPipe != 0 {
		str += "P"
	} else if mode&fs.ModeSocket != 0 {
		str += "S"
	} else if mode&fs.ModeDevice != 0 {
		str += "D"
	} else if mode&fs.ModeCharDevice != 0 {
		str += "C"
	} else if mode&fs.ModeIrregular != 0 {
		str += "I"
	}
	return str
}

var totalSize uint64 = 0
var countFiles uint64 = 0
var countDirs uint64 = 0

func create_start_ticker(msg *string) *time.Ticker {
	ticker := time.NewTicker(time.Second)
	go func() {
		p := message.NewPrinter(language.English)
		last_totalSize := uint64(0)
		last_countFiles := uint64(0)
		last_countDirs := uint64(0)

		startTime := time.Now()
		var m runtime.MemStats
		for range ticker.C {
			runtime.ReadMemStats(&m)
			this_totalSize := atomic.LoadUint64(&totalSize)
			this_countFiles := atomic.LoadUint64(&countFiles)
			this_countDirs := atomic.LoadUint64(&countDirs)

			delta_totalSize := this_totalSize - last_totalSize
			delta_countFiles := this_countFiles - last_countFiles
			delta_countDirs := this_countDirs - last_countDirs

			elapsedTime := time.Since(startTime)
			x := float64(elapsedTime.Milliseconds()) / 1000.0
			p.Printf("   %s %0.3f bytes %s/%s files: %d/%d  dirs: %d/%d\n",
				*msg, x, formatBytes(delta_totalSize), formatBytes(this_totalSize), delta_countFiles, this_countFiles, delta_countDirs, this_countDirs)

			last_totalSize = this_totalSize
			last_countFiles = this_countFiles
			last_countDirs = this_countDirs
		}
	}()
	return ticker
}

var tabs = string("                                                                                                    ")

func treeWalk(dir *DirInfo, depth int) {
	fmt.Printf("%9s %s %s %d\n", formatBytes(dir.imm_size), tabs[0:depth], dir.name, depth)
	for _, child := range dir.children {
		treeWalk(child, depth+1)
	}
}
