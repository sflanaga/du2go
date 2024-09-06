package main

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
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

func formatDuration(d time.Duration, precision int) string {
	if d == math.MaxInt64 {
		return "NA"
	}

	units := []struct {
		value time.Duration
		label string
	}{
		{24 * 365 * time.Hour, "y"}, // Years
		{24 * 7 * time.Hour, "w"}, // Days
		{24 * time.Hour, "d"}, // Days
		{time.Hour, "h"},      // Hours
		{time.Minute, "m"},    // Minutes
		{time.Second, "s"},    // Seconds
	}

	var result string

	for _, unit := range units {
		if precision <= 0 {
			break
		}
		value := d / unit.value
		if value > 0 {
			precision -= 1
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

func modeToChar(mode fs.FileMode) string {
	if mode.IsDir() {
		return "D"
	} else if mode.IsRegular() {
		return "F"
	} else if mode&fs.ModeSymlink != 0 {
		return "L"
	} else if mode&fs.ModeNamedPipe != 0 {
		return "P"
	} else if mode&fs.ModeSocket != 0 {
		return "S"
	} else if mode&fs.ModeCharDevice != 0 {
		return "C"
	} else if mode&fs.ModeDevice != 0 {
		return "D"
	} else if mode&fs.ModeIrregular != 0 {
		return "I"
	}
	return "?"
}
func modeToStringLong(mode fs.FileMode) string {
	if mode.IsDir() {
		return "dir"
	} else if mode.IsRegular() {
		return "file"
	} else if mode&fs.ModeSymlink != 0 {
		return "symlink"
	} else if mode&fs.ModeNamedPipe != 0 {
		return "pipe"
	} else if mode&fs.ModeSocket != 0 {
		return "socket"
	} else if mode&fs.ModeCharDevice != 0 {
		return "char-device"
	} else if mode&fs.ModeDevice != 0 {
		return "device"
	} else if mode&fs.ModeIrregular != 0 {
		return "irregular"
	}
	return "unknown"
}

var totalSize uint64 = 0
var countFiles uint64 = 0
var countDirs uint64 = 0
var goroutines int32 = 0

var filestatErrors uint64 = 0
var notDirOrFile uint64 = 0
var filterDirs uint64 = 0
var dirListErrors uint64 = 0

var countFileTypes = xsync.NewMapOf[fs.FileMode, int]()

func printFilteredStringTypes(fileTypeMap *xsync.MapOf[fs.FileMode, int]) bool {
	var list []string
	var atLeastOne = false
	fileTypeMap.Range(func(key fs.FileMode, value int) bool {
		if value > 0 {
			var s = fmt.Sprintf("%s=%d", modeToStringLong(key), value)
			list = append(list, s)
			atLeastOne = true
		}
		return true // Continue iterating
	})
	if atLeastOne {
		fmt.Printf("count of \"off\" filetypes: %s\n", strings.Join(list, ", "))
	}
	return atLeastOne
}

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
	return formatDuration(time2duration(filemod, now),3)
}

func mod2TimestampStr(filemod int64, now *time.Time) string {
	if filemod == math.MaxInt64 || filemod == math.MinInt64 {
		return "NA"
	} else {
		hours := time2duration(filemod, now).Hours() / 24.0
		return fmt.Sprintf("%.3f", hours)
		// return time.Unix(filemod, 0).Format("2006/01/02T15:04:05")
	}
}

func treeWalkDetails(dir *DirInfo, depth int, start *time.Time, flatUnits bool) {
	if depth == 0 {
		if flatUnits {
			fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", "path", "imm_size", "imm_files", "imm_dirs",
				"rec_size", "rec_files", "rec_dirs",
				"imm_oldest_days", "imm_newest_days", "rec_oldest_days", "imm_oldest_days", "depth")
		} else {
			fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", "path", "imm_size", "imm_files", "imm_dirs",
				"rec_size", "rec_files", "rec_dirs",
				"imm_oldest", "imm_newest", "rec_oldest", "imm_oldest", "depth")
		}
	}
	if !flatUnits {
		fmt.Printf("%s,%s,%d,%d,%s,%d,%d,%s,%s,%s,%s,%d\n", dir.name, formatBytes(dir.imm_size), dir.imm_files, dir.imm_dirs,
			formatBytes(dir.rec_size), dir.rec_files, dir.rec_dirs,
			mod2str(dir.imm_old_file, start), mod2str(dir.imm_new_file, start), mod2str(dir.rec_old_file, start), mod2str(dir.rec_new_file, start),
			depth)
	} else {
		fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%d\n", dir.name, dir.imm_size, dir.imm_files, dir.imm_dirs,
			dir.rec_size, dir.rec_files, dir.rec_dirs,
			mod2TimestampStr(dir.imm_old_file, start), mod2TimestampStr(dir.imm_new_file, start),
			mod2TimestampStr(dir.rec_old_file, start), mod2TimestampStr(dir.rec_new_file, start),
			depth)
	}
	for _, child := range dir.children {
		treeWalkDetails(child, depth+1, start, flatUnits)
	}
}
