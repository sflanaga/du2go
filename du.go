package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/sflanaga/statticker"
	"golang.org/x/sync/semaphore"
)

var isWindows = runtime.GOOS == "windows"

var totalSize = statticker.NewStat("bytes", statticker.Bytes)
var countFiles = statticker.NewStat("files", statticker.Count)
var countDirs = statticker.NewStat("dir", statticker.Count)
var goroutines = statticker.NewStat("goroutines", statticker.Gauge)

type DirInfo struct {
	name         string
	imm_size     uint64
	imm_files    uint64
	imm_dirs     uint64
	imm_old_file int64
	imm_new_file int64
	rec_size     uint64
	rec_files    uint64
	rec_dirs     uint64
	rec_old_file int64
	rec_new_file int64
	children     []*DirInfo
}

func NewDirInfo(name string) *DirInfo {
	return &DirInfo{
		name:         name,
		imm_size:     0,
		imm_files:    0,
		imm_dirs:     0,
		imm_old_file: math.MaxInt64,
		imm_new_file: math.MinInt64,
		rec_size:     0,
		rec_files:    0,
		rec_dirs:     0,
		rec_old_file: math.MaxInt64,
		rec_new_file: math.MinInt64,
		children:     make([]*DirInfo, 0),
	}
}

type PathSize struct {
	size int64
	path string
}

func pathSizeLess(a, b PathSize) bool {
	return a.size < b.size
}

type maxGlobalFile struct {
	limits  int
	mtx     sync.Mutex
	minFile int64
	mapMax  *btree.BTreeG[PathSize]
}

func NewMaxGlobalFile(limit int) *maxGlobalFile {
	return &maxGlobalFile{
		limits:  limit,
		mtx:     sync.Mutex{},
		minFile: 0,
		mapMax:  btree.NewG[PathSize](16, pathSizeLess),
	}
}

var maxFiles *maxGlobalFile = nil

func (m *maxGlobalFile) setMaxFile(size int64, path *string) {
	// we do the quick check to avoid the mutex lock
	currMin := atomic.LoadInt64(&m.minFile)
	if size > currMin {
		m.mtx.Lock()
		defer m.mtx.Unlock()

		currMin := atomic.LoadInt64(&m.minFile)
		if size > currMin {
			m.mapMax.ReplaceOrInsert(PathSize{size: size, path: *path})
			if m.mapMax.Len() > m.limits {
				m.mapMax.DeleteMin()
			}
			if currMin > size {
				atomic.StoreInt64(&m.minFile, size)
			}
		}
	}
}

var fsFilter = map[string]bool{
	"/proc": true,
	"/dev":  true,
	"/sys":  true,
}

// func getUserIDFromFileInfo(fileInfo os.FileInfo) (uint32, error) {
//     var uid uint32
//     switch runtime.GOOS {
//     case "windows":
//         // On Windows, use the FileBasicInfo structure
//         fileBasicInfo, err := syscall.GetFileInformationByHandle(fileInfo.Sys().(*syscall.Handle))
//         if err != nil {
//             return 0, fmt.Errorf("failed to get file information: %w", err)
//         }
//         uid = fileBasicInfo.Owner.LowPart
//     default:
//         // On Unix-like systems, use the Stat_t structure
//         uid = fileInfo.Sys().(*syscall.Stat_t).Uid
//     }
//     return uid, nil
// }

func walkGo(debug bool, dir *DirInfo, limitworkers *semaphore.Weighted, goroutine bool, depth int) {
	if goroutine {
		// we need to release the allocated thread/goroutine if we stop early
		// we only need to do this when we did NOT steal the next directory/task
		// also note that defer DOES work conditionally here because it works at
		// the end of the current function and NOT the current scope
		goroutines.Add(1)
		defer goroutines.Add(-1)
		defer limitworkers.Release(1)
	}

	user := UserStats{NULL_USER_ID, 0, 0, 0}
	defer func() {
		loadUserInfo(user)
		// println("loading user info")
	}()

	// goofy special filters
	if depth <= 1 {
		if _, ok := fsFilter[dir.name]; ok {
			atomic.AddUint64(&filterDirs, 1)
			if debug {
				fmt.Fprintf(os.Stderr, "skipping path %s as special\n", dir.name)
			}
			return
		}
	}

	files, err := os.ReadDir(dir.name)
	if err != nil {
		atomic.AddUint64(&dirListErrors, 1)
		if debug {
			fmt.Fprintln(os.Stderr, "Error reading directory:", err)
		}
		return
	}
	newest := int64(math.MinInt64)
	oldest := int64(math.MaxInt64)

	for _, file := range files {
		var cleanPath = filepath.Join(dir.name, file.Name())
		if file.IsDir() {
			subdir := NewDirInfo(cleanPath)
			dir.children = append(dir.children, subdir)
			// atomic.AddUint64(&countDirs, 1)
			countDirs.Add(1)
			dir.imm_dirs++
			dir.rec_dirs++
			// fmt.Println(cleanPath, file.IsDir())
			// cheesey simple work-stealing

			stats, err_st := file.Info()
			if err_st == nil {
				uid := getUserId(&stats)
				user.addDir(uid)
			} else {
				println("error on ", cleanPath, " of ", err_st)
			}

			if limitworkers.TryAcquire(1) {
				go walkGo(debug, subdir, limitworkers, true, depth+1)
			} else {
				walkGo(debug, subdir, limitworkers, false, depth+1)
			}
		} else if file.Type().IsRegular() || (fs.ModeIrregular&file.Type() != 0) {
			stats, err_st := file.Info()
			if err_st != nil {
				atomic.AddUint64(&filestatErrors, 1)
				if debug {
					fmt.Fprintln(os.Stderr, "... Error reading file info:", err_st)
				}
				continue
			}
			sz := stats.Size()
			// uid := getUserId(&stats)
			if stats.ModTime().Unix() > newest {
				newest = stats.ModTime().Unix()
			}
			if stats.ModTime().Unix() < oldest {
				oldest = stats.ModTime().Unix()
			}
			countFiles.Add(1)
			totalSize.Add(int64(sz))
			// atomic.AddUint64(&countFiles, 1)
			// atomic.AddUint64(&totalSize, uint64(sz))

			dir.imm_size += uint64(sz)
			dir.rec_size += uint64(sz)

			dir.imm_files++
			dir.rec_files++

			dir.imm_new_file = newest
			dir.imm_old_file = oldest

			dir.rec_new_file = maxInt64(dir.rec_new_file, newest)
			dir.rec_old_file = minInt64(dir.rec_old_file, oldest)

			maxFiles.setMaxFile(sz, &cleanPath)

			uid := getUserId(&stats)
			user.addFile(uid, uint64(sz))
		} else {
			atomic.AddUint64(&notDirOrFile, 1)
			countFileTypes.Compute(file.Type(), func(oldValue int, loaded bool) (newValue int, delete bool) {
				newValue = oldValue + 1
				return
			})

			// _, _ = countFileTypes.LoadOrStore(key, func(value interface{}) interface{} {
			// 	if value == nil {
			// 		return 1
			// 	}
			// 	return value.(V) + 1
			// })
			if debug {
				fmt.Fprintln(os.Stderr, "... skipping file:", cleanPath, " type: ", modeToStringLong(file.Type()))
			}
		}
	}
	// loadUserInfo(user)

}

func treePerk(dir *DirInfo, depth int) {
	// fmt.Printf("%9s %s %s %d\n", formatBytes(dir.imm_size), tabs[0:depth], dir.name, depth)
	for _, child := range dir.children {
		treePerk(child, depth+1)

		dir.rec_size += child.rec_size
		dir.rec_files += child.rec_files
		dir.rec_dirs += child.rec_dirs
		dir.rec_new_file = maxInt64(dir.rec_new_file, child.rec_new_file)
		dir.rec_old_file = minInt64(dir.rec_old_file, child.rec_old_file)
	}
}

// var maxDirByImmSize = btree.NewMap[int64, string](16)
func trySetNewMaxPath(tree *btree.BTreeG[PathSize], size int64, path *string, limit int) {
	tree.ReplaceOrInsert(PathSize{size, *path})
	if tree.Len() > limit {
		tree.DeleteMin()
	}
}

var maxDirByImmSize = btree.NewG[PathSize](16, pathSizeLess)
var maxDirByImmCount = btree.NewG[PathSize](16, pathSizeLess)
var maxDirByImmDirCount = btree.NewG[PathSize](16, pathSizeLess)
var maxDirByRecSize = btree.NewG[PathSize](16, pathSizeLess)

func walkTreeSummary(dir *DirInfo, sumLimit int, depth int) {
	trySetNewMaxPath(maxDirByImmSize, int64(dir.imm_size), &dir.name, sumLimit)
	trySetNewMaxPath(maxDirByImmCount, int64(dir.imm_files), &dir.name, sumLimit)
	trySetNewMaxPath(maxDirByImmDirCount, int64(dir.imm_dirs), &dir.name, sumLimit)
	trySetNewMaxPath(maxDirByRecSize, int64(dir.rec_size), &dir.name, sumLimit)
	for _, child := range dir.children {
		walkTreeSummary(child, sumLimit, depth+1)
	}
}

func printSummary(tree *btree.BTreeG[PathSize], bytes bool, title string, flatUnits bool) {
	fmt.Println(title)
	tree.Descend(func(value PathSize) bool {
		if bytes {
			if flatUnits {
				fmt.Printf("%12d %s\n", uint64(value.size), value.path)
			} else {
				fmt.Printf("%8s %s\n", statticker.FormatBytes(uint64(value.size)), value.path)
			}
		} else {
			fmt.Printf("%8d %s\n", value.size, value.path)
		}
		return true
	})
}

func reportAnyScanErrors() {
	if filestatErrors > 0 {
		fmt.Printf("%8d file stat errors\n", filestatErrors)
	}
	printFilteredStringTypes(countFileTypes)
	if filterDirs > 0 {
		fmt.Printf("%8d special directories filtered\n", filterDirs)
	}
	if dirListErrors > 0 {
		fmt.Printf("%8d directories that cannot be listed\n", dirListErrors)
	}
}

func duStatPrinter(t *statticker.Ticker, samplePeriod time.Duration, finalOutput bool) {
	timeStr := float64(time.Since(t.StartTime).Milliseconds()) / 1000.0
	if finalOutput {
		t.Buf = fmt.Appendf(t.Buf, "OVERALL[%s] %0.3f ", t.Msg, timeStr)
	} else {
		t.Buf = fmt.Appendf(t.Buf, "%s %0.3f ", t.Msg, timeStr)
	}
	for _, sample := range t.Samples {
		var ratePerSec float64
		if !finalOutput {
			ratePerSec = float64(sample.Delta) / float64(samplePeriod.Seconds())
		} else {
			ratePerSec = float64(sample.Delta) / float64(samplePeriod.Seconds())
		}
		// fmt.Printf("%f  %f\n", float64(sample.delta), float64(samplePeriod.Seconds()))
		switch sample.Stype {
		case statticker.Bytes:
			t.Buf = fmt.Appendf(t.Buf, " %s: %s/s, %s", *sample.Name, statticker.FormatBytes(uint64(ratePerSec)), statticker.FormatBytes(uint64(sample.Value)))
		case statticker.Count:
			t.Buf = fmt.Appendf(t.Buf, " %s: %s/s, %s", *sample.Name, statticker.AddCommas(uint64(ratePerSec)), statticker.AddCommas(uint64(sample.Value)))
		case statticker.Gauge:
			t.Buf = fmt.Appendf(t.Buf, " %s: %s", *sample.Name, statticker.AddCommas(uint64(sample.Value)))
		}
	}
	fmt.Fprintln(os.Stderr, string(t.Buf))

}

func main() {

	start := time.Now()

	rootDir := flag.String("d", ".", "root directory to scan")
	ticker_duration := flag.Duration("i", 1*time.Second, "ticker duration")
	dumpFullDetails := flag.Bool("D", false, "dump full details")
	flatUnits := flag.Bool("F", false, "use basic units for size and age - useful for simpler post processing")
	reports := flag.String("R", "lifdru", "Top stats reports: \n l - largest file\n i - directories by total file size immediately in it\n f - directories by file count immediately in it\n d - directories by directory count immediately in it\n r - directories by total file size recursively in it\n u - total file usage by user id\n")
	cpuNum := runtime.NumCPU()
	threadLimit := flag.Int("t", cpuNum, "limit number of threads")
	summaryLimit := flag.Int("l", 10, "limit stat reports to the top N")
	debug := flag.Bool("v", false, "write per file/directory errors during scan")

	for _, x := range *reports {
		if !strings.ContainsRune("lifdru", x) {
			fmt.Printf("Unknown -R sub option: '%c'\n", x)
			os.Exit(1)
		}
	}

	var workerSema = semaphore.NewWeighted(int64(*threadLimit))

	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS]\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "Options error - Extra/orphaned arguments - most likely not using -d option")
		for i, arg := range flag.Args() {
			fmt.Fprintf(os.Stderr, "\t[%d]: %s\n", i+1, arg)
		}
		os.Exit(2)
	}

	maxFiles = NewMaxGlobalFile(*summaryLimit)
	absPath, err := filepath.Abs(*rootDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error getting absolute path:", err)
		os.Exit(3)
	}

	var statList []*statticker.Stat
	statList = append(statList, countFiles)
	statList = append(statList, countDirs)
	statList = append(statList, totalSize)

	var ticker *statticker.Ticker
	if ticker_duration.Seconds() != 0 {
		ticker = statticker.NewTicker("stats monitor", *ticker_duration, statList)
		ticker.WithPrinter(duStatPrinter)
		ticker.Start()
	}

	root := NewDirInfo(absPath)
	var ctx = context.Background()

	workerSema.Acquire(ctx, 1)
	walkGo(*debug, root, workerSema, true, 0)

	workerSema.Acquire(ctx, int64(*threadLimit))

	elapse := time.Since(start)
	if ticker != nil {
		ticker.Stop()
	}

	treePerk(root, 0)

	fmt.Printf("Scanned directory path: %s\n", *rootDir)

	_startTime := time.Now()
	walkTreeSummary(root, *summaryLimit, 0)
	fmt.Printf("post scan report computer time: %v\n", time.Since(_startTime))

	if *dumpFullDetails {
		treeWalkDetails(root, 0, &start, *flatUnits)
		fmt.Println()
		reportAnyScanErrors()
	} else {
		for _, x := range *reports {
			switch x {
			case 'l':
				fmt.Println("Largest files (globally)")
				maxFiles.mapMax.Descend(func(value PathSize) bool {
					if *flatUnits {
						fmt.Printf("%12d %s\n", uint64(value.size), value.path)
					} else {
						fmt.Printf("%8s %s\n", statticker.FormatBytes(uint64(value.size)), value.path)
					}
					return true
				})
			case 'i':
				fmt.Println()
				printSummary(maxDirByImmSize, true, "directories by total file size immediately in it", *flatUnits)
			case 'f':
				fmt.Println()
				printSummary(maxDirByImmCount, false, "directories by file count immediately in it", *flatUnits)
			case 'd':
				fmt.Println()
				printSummary(maxDirByRecSize, true, "directories by total file size recursively in it", *flatUnits)
			case 'r':
				fmt.Println()
				printSummary(maxDirByImmDirCount, false, "directories by directory count immediately in it", *flatUnits)
			case 'u':
				fmt.Println()
				if !isWindows {
					printUserInfo(*summaryLimit)
				} else {
					fmt.Println("user id not supported on windows")
				}
			default:
				fmt.Printf("Unknown -R sub option: '%c' skipped\n", x)
			}

		}
		fmt.Println("Total size:", statticker.FormatBytes(totalSize.Get()), "in",
			statticker.AddCommas(countFiles.Get()), "files and", countDirs.Get(), "directories", "done in", elapse)
	}

}
