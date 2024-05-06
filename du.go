package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
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
	if size > atomic.LoadInt64(&m.minFile) {
		m.mtx.Lock()
		defer m.mtx.Unlock()

		if size > atomic.LoadInt64(&m.minFile) {
			m.mapMax.ReplaceOrInsert(PathSize{size: size, path: *path})
			if m.mapMax.Len() > m.limits {
				m.mapMax.DeleteMin()
				atomic.StoreInt64(&m.minFile, size)
			}
		} else {
			fmt.Println("FLIP FLOP ---------------------")
		}
	}
}

func walkGo(dir *DirInfo, wg *sync.WaitGroup, limitworkers int32, workers *int32, goroutine bool) {

	if goroutine {
		// we need to release the allocated thread/goroutine if we stop early
		// we only need to do this when we did NOT steal the next directory/task
		// also note that defer DOES work conditionally here because it works at
		// the end of the current function and NOT the current scope
		defer atomic.AddInt32(workers, -1)
		wg.Add(1)
		defer wg.Done()
	}

	files, err := os.ReadDir(dir.name)
	if err != nil {
		fmt.Println("Error reading directory:", err)
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

			if atomic.LoadInt32(workers) < limitworkers {
				atomic.AddInt32(workers, 1)
				go walkGo(subdir, wg, limitworkers, workers, true)
			} else {
				walkGo(subdir, wg, limitworkers, workers, false)
			}
		} else {
			stats, err_st := file.Info()
			if err_st != nil {
				fmt.Println("... Error reading file info:", err_st)
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

func printSummary(tree *btree.BTreeG[PathSize], bytes bool, title string) {
	fmt.Println(title)
	tree.Descend(func(value PathSize) bool {
		if bytes {
			fmt.Printf("%s %s\n", formatBytes(uint64(value.size)), value.path)
		} else {
			fmt.Printf("%d %s\n", value.size, value.path)
		}
		return true
	})
}

func main() {

	rootDir := flag.String("d", ".", "root directory to scan")
	ticker_duration := flag.Duration("i", 1*time.Second, "ticker duration")
	dumpFullDetails := flag.Bool("D", false, "dump full details")
	cpuNum := runtime.NumCPU()
	threadLimit := flag.Int("t", cpuNum, "limit number of threads")
	summaryLimit := flag.Int("l", 10, "limit summaries to N number")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS]\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	maxFiles = NewMaxGlobalFile(*summaryLimit)
	start := time.Now()
	absPath, err := filepath.Abs(*rootDir)
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	wg := &sync.WaitGroup{}
	var msg = "tree scan"
	var ticker *time.Ticker = nil

	if ticker_duration.Seconds() != 0 {
		ticker = create_start_ticker(&msg, ticker_duration)
	}
	root := NewDirInfo(absPath)
	goroutines = int32(0)
	walkGo(root, wg, int32(*threadLimit), &goroutines, true)

	wg.Wait()
	elapse := time.Since(start)
	if ticker_duration.Seconds() != 0 {
		ticker.Stop()
	}

	treePerk(root, 0)

	if *dumpFullDetails {
		treeWalkDetails(root, 0, &start)
	}

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

	fmt.Println("Largest files (globally)")
	maxFiles.mapMax.Descend(func(value PathSize) bool {
		fmt.Printf("%s %s\n", formatBytes(uint64(value.size)), value.path)
		return true
	})

	walkTreeSummary(root, *summaryLimit, 0)

	fmt.Println()
	printSummary(maxDirByImmSize, true, "direstories by total file size immediately in it")
	fmt.Println()
	printSummary(maxDirByImmCount, false, "direstories by file count immediately in it")
	fmt.Println()
	printSummary(maxDirByImmDirCount, false, "direstories by directory count immediately in it")
	fmt.Println()
	printSummary(maxDirByRecSize, true, "direstories by total file size recursively in it")
	fmt.Println()
	fmt.Println("Total size:", formatBytes(uint64(totalSize)), " in ", countFiles, " files and ", countDirs, " directories", " done in ", elapse)

}
