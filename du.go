package main

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
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

func walkGo(dir *DirInfo, wg *sync.WaitGroup, limitworkers uint32, workers *uint32) {
	wg.Add(1)
	defer wg.Done()
	files, err := os.ReadDir(dir.name)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}
	newest := int64(math.MinInt64)
	oldest := int64(math.MaxInt64)

	for _, file := range files {
		var cleanPath = path.Join(dir.name, file.Name())
		if file.IsDir() {
			subdir := new(DirInfo)
			subdir.name = cleanPath
			dir.children = append(dir.children, subdir)
			atomic.AddUint64(&countDirs, 1)
			dir.imm_dirs++
			// fmt.Println(cleanPath, file.IsDir())
			// cheesey simple work-stealing
			if atomic.LoadUint32(workers) < limitworkers {
				atomic.AddUint32(workers, 1)
				go walkGo(subdir, wg, limitworkers, workers)
			} else {
				walkGo(subdir, wg, limitworkers, workers)
			}
		} else {
			stats, err_st := file.Info()
			if err_st != nil {
				fmt.Println("... Error reading file info:", err_st)
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
			dir.imm_files++
			dir.imm_new_file = newest
			dir.imm_old_file = oldest

		}
	}
}

func main() {
	start := time.Now()
	absPath, err := filepath.Abs("../..")
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	wg := &sync.WaitGroup{}
	var msg = "tree scan"
	ticker := create_start_ticker(&msg)
	root := DirInfo{name: absPath}
	walkGo(&root, wg, 10, new(uint32))

	wg.Wait()
	elapse := time.Since(start)
	ticker.Stop()
	fmt.Println("Total size:", formatBytes(uint64(totalSize)), " in ", countFiles, " files and ", countDirs, " directories", " done in ", elapse)

	treeWalk(&root, 0)

}
