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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"golang.org/x/sync/semaphore"
)

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

var fsFilter = map[string]bool{
	"/proc": true,
	"/dev":  true,
	"/sys":  true,
}

func walkGo(debug bool, dir *DirInfo, limitworkers *semaphore.Weighted, workers *int32, goroutine bool, depth int) {
	if goroutine {
		// we need to release the allocated thread/goroutine if we stop early
		// we only need to do this when we did NOT steal the next directory/task
		// also note that defer DOES work conditionally here because it works at
		// the end of the current function and NOT the current scope
		defer limitworkers.Release(1)
	}

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
			atomic.AddUint64(&countDirs, 1)
			dir.imm_dirs++
			dir.rec_dirs++
			// fmt.Println(cleanPath, file.IsDir())
			// cheesey simple work-stealing

			if limitworkers.TryAcquire(1) {
				// if atomic.LoadInt32(workers) < limitworkers {
				go walkGo(debug, subdir, limitworkers, workers, true, depth+1)
			} else {
				walkGo(debug, subdir, limitworkers, workers, false, depth+1)
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
			if stats.ModTime().Unix() > newest {
				newest = stats.ModTime().Unix()
			}
			if stats.ModTime().Unix() < oldest {
				oldest = stats.ModTime().Unix()
			}
			atomic.AddUint64(&countFiles, 1)
			atomic.AddUint64(&totalSize, uint64(sz))

			dir.imm_size += uint64(sz)
			dir.rec_size += uint64(sz)

			dir.imm_files++
			dir.rec_files++

			dir.imm_new_file = newest
			dir.imm_old_file = oldest

			dir.rec_new_file = maxInt64(dir.rec_new_file, newest)
			dir.rec_old_file = minInt64(dir.rec_old_file, oldest)

			maxFiles.setMaxFile(sz, &cleanPath)

		} else {
			atomic.AddUint64(&notDirOrFile, 1)
			if debug {
				fmt.Fprintln(os.Stderr, "... skipping file:", cleanPath, " type: ", modeToStringLong(file.Type()))
			}
		}
	}

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
				fmt.Printf("%8s %s\n", formatBytes(uint64(value.size)), value.path)
			}
		} else {
			fmt.Printf("%8d %s\n", value.size, value.path)
		}
		return true
	})
}

func printSkipAndError() {
	if filestatErrors > 0 {
		fmt.Printf("%8d file stat errors\n", filestatErrors)
	}
	if notDirOrFile > 0 {
		fmt.Printf("%8d nodes not a file or directory\n", notDirOrFile)
	}
	if filterDirs > 0 {
		fmt.Printf("%8d special directories filtered\n", filterDirs)
	}
	if dirListErrors > 0 {
		fmt.Printf("%8d directories that cannot be listed\n", dirListErrors)
	}
}

func main() {
	start := time.Now()

	rootDir := flag.String("d", ".", "root directory to scan")
	ticker_duration := flag.Duration("i", 1*time.Second, "ticker duration")
	dumpFullDetails := flag.Bool("D", false, "dump full details")
	flatUnits := flag.Bool("F", false, "use basic units for size and age - useful for simpler post processing")
	cpuNum := runtime.NumCPU()
	threadLimit := flag.Int("t", cpuNum, "limit number of threads")
	summaryLimit := flag.Int("l", 10, "limit summaries to N number")
	debug := flag.Bool("v", false, "keep intermediate error messages quiet")

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
		return
	}
	// wg := &sync.WaitGroup{}
	var msg = "tree scan"
	var ticker *time.Ticker = nil

	if ticker_duration.Seconds() != 0 {
		ticker = create_start_ticker(&msg, ticker_duration)
	}
	root := NewDirInfo(absPath)
	goroutines = int32(0)
	var ctx = context.Background()

	workerSema.Acquire(ctx, 1)
	walkGo(*debug, root, workerSema, &goroutines, true, 0)
	// wg.Wait()
	workerSema.Acquire(ctx, int64(*threadLimit))

	// for !workerSema.TryAcquire(int64(*threadLimit)) {
	// 	fmt.Println("cannot get semaphore")
	// 	time.Sleep(time.Duration(1 * 1_000_000_000))
	// }

	elapse := time.Since(start)
	if ticker_duration.Seconds() != 0 {
		ticker.Stop()
	}

	treePerk(root, 0)

	// if *dumpFullDetails {
	// 	treeWalkDetails(root, 0, &start)
	// }

	// global max files size
	// max dirs imm size
	// max dirs imm file count
	// max dirs file size recursive
	// max dir by rec count

	// mm := btree.NewMap[int64, string](16)

	// mm.Scan(func(key int64, value string) bool {
	// 	fmt.Printf("%s %s\n", formatBytes(uint64(key)), value)
	// 	return true
	// })

	fmt.Printf("Scanned directory path: %s\n", *rootDir)

	walkTreeSummary(root, *summaryLimit, 0)

	if *dumpFullDetails {
		treeWalkDetails(root, 0, &start, *flatUnits)
		fmt.Println()
		printSkipAndError()
	} else {
		fmt.Println("Largest files (globally)")
		maxFiles.mapMax.Descend(func(value PathSize) bool {
			if *flatUnits {
				fmt.Printf("%12d %s\n", uint64(value.size), value.path)
			} else {
				fmt.Printf("%8s %s\n", formatBytes(uint64(value.size)), value.path)
			}
			return true
		})
		fmt.Println()
		printSummary(maxDirByImmSize, true, "directories by total file size immediately in it", *flatUnits)
		fmt.Println()
		printSummary(maxDirByImmCount, false, "directories by file count immediately in it", *flatUnits)
		fmt.Println()
		printSummary(maxDirByImmDirCount, false, "directories by directory count immediately in it", *flatUnits)
		fmt.Println()
		printSummary(maxDirByRecSize, true, "directories by total file size recursively in it", *flatUnits)
		fmt.Println()
		printSkipAndError()
		fmt.Println()
		fmt.Println("Total size:", formatBytes(uint64(totalSize)), "in", countFiles, "files and", countDirs, "directories", "done in", elapse)
	}

}
