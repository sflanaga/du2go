package main

import (
	"os"
	"fmt"
	"io/fs"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func maxInt64(nums ...int64) int64 {
	if len(nums) == 0 {
		panic("No numbers provided") // Or handle this as needed
	}
	currentMax := nums[0]
	for _, num := range nums[1:] {
		if num > currentMax {
			currentMax = num
		}
	}
	return currentMax
}
func minInt64(nums ...int64) int64 {
	if len(nums) == 0 {
		panic("No numbers provided") // Or handle this as needed
	}
	currentMin := nums[0]
	for _, num := range nums[1:] {
		if num < currentMin {
			currentMin = num
		}
	}
	return currentMin
}

func formatDuration(d time.Duration) string {
	if d == math.MaxInt64 {
		return "NA"
	}

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
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2fTB", float64(bytes)/TB)
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
func modeToStringLong(mode fs.FileMode) string {
	var str = ""
	if mode.IsDir() {
		str += "dir"
	} else if mode.IsRegular() {
		str += "file"
	} else if mode&fs.ModeSymlink != 0 {
		str += "symlink"
	} else if mode&fs.ModeNamedPipe != 0 {
		str += "pipe"
	} else if mode&fs.ModeSocket != 0 {
		str += "socket"
	} else if mode&fs.ModeDevice != 0 {
		str += "device"
	} else if mode&fs.ModeCharDevice != 0 {
		str += "char-device"
	} else if mode&fs.ModeIrregular != 0 {
		str += "irregular"
	}
	return str
}

var totalSize uint64 = 0
var countFiles uint64 = 0
var countDirs uint64 = 0
var goroutines int32 = 0

func create_start_ticker(msg *string, interval *time.Duration) *time.Ticker {
	ticker := time.NewTicker(*interval)
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

			this_go_routines := atomic.LoadInt32(&goroutines)

			delta_totalSize := this_totalSize - last_totalSize
			delta_countFiles := this_countFiles - last_countFiles
			delta_countDirs := this_countDirs - last_countDirs

			elapsedTime := time.Since(startTime)
			x := float64(elapsedTime.Milliseconds()) / 1000.0
			p.Fprintf(os.Stderr, "   %s %0.3f bytes %s/%s files: %d/%d  dirs: %d/%d  go-threads: %d\n",
				*msg, x, formatBytes(delta_totalSize), formatBytes(this_totalSize), delta_countFiles, this_countFiles, delta_countDirs, this_countDirs, this_go_routines)

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

func time2duration(filemod int64, now *time.Time) time.Duration {
	if filemod == math.MaxInt64 || filemod == math.MinInt64 {
		return time.Duration(math.MaxInt64)
	} else {
		now_t := now.Unix()
		if filemod > now_t {
			return time.Duration(math.MaxInt64)
		}
		delta_t := now_t - filemod
		return time.Duration(delta_t) * time.Second
	}
}

func mod2str(filemod int64, now *time.Time) string {
	return formatDuration(time2duration(filemod, now))
}

func mod2TimestampStr(filemod int64, now *time.Time) string {
	if filemod == math.MaxInt64 || filemod == math.MinInt64 {
		return "NA"
	} else {
		hours := time2duration(filemod, now).Hours()/24.0
		return fmt.Sprintf("%.3f", hours)
		// return time.Unix(filemod, 0).Format("2006/01/02T15:04:05")
	}
}

func treeWalkDetails(dir *DirInfo, depth int, start *time.Time, flatUnits bool) {
	if depth == 0 {
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", "path", "imm_size", "imm_files", "imm_dirs",
			"rec_size", "rec_files", "rec_dirs",
			"imm_oldest", "imm_newest", "rec_oldest", "imm_oldest", "depth")
	}
	if !flatUnits {
		fmt.Printf("%s,%s,%d,%d,%s,%d,%d,%s,%s,%s,%s,%d\n", dir.name, formatBytes(dir.imm_size), dir.imm_files, dir.imm_dirs,
			formatBytes(dir.rec_size), dir.rec_files, dir.rec_dirs,
			mod2str(dir.imm_old_file, start), mod2str(dir.imm_new_file, start), mod2str(dir.rec_old_file, start), mod2str(dir.rec_new_file, start),
			depth)
	} else {
		fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%d\n", dir.name,  dir.imm_size,  dir.imm_files,  dir.imm_dirs, 
			dir.rec_size,  dir.rec_files,  dir.rec_dirs, 
			mod2TimestampStr(dir.imm_old_file, start),  mod2TimestampStr(dir.imm_new_file, start),
			mod2TimestampStr(dir.rec_old_file, start),  mod2TimestampStr(dir.rec_new_file, start), 
			depth)
	}
	for _, child := range dir.children {
		treeWalkDetails(child, depth+1, start, flatUnits)
	}
}
