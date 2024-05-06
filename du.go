package main

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

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

func walkDirectory(dir string) {
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	// Print or process the contents
	for _, file := range files {
		var cleanPath = path.Join(dir, file.Name())

		stats, err_st := file.Info()
		if err_st != nil {
			fmt.Println("... Error reading file info:", err_st)
		}
		fmt.Println(modeToString(stats.Mode()), " ", cleanPath, formatBytes(uint64(stats.Size())))

		if file.IsDir() {
			walkDirectory(cleanPath)
		}

	}
}

func walk(dir string, queue chan string) {

	select {
	case dir = <-queue:
		files, err := os.ReadDir(dir)
		if err != nil {
			fmt.Println("Error reading directory:", err)
			return
		}
		for _, file := range files {
			var cleanPath = path.Join(dir, file.Name())
			fmt.Print(cleanPath, file.IsDir())
			if file.IsDir() {
				select {
				case queue <- cleanPath:
					continue
				default:
					walk(cleanPath, queue)
				}
			} else {
				stats, err_st := file.Info()
				if err_st != nil {
					fmt.Println("... Error reading file info:", err_st)
				}
				fmt.Println(modeToString(stats.Mode()), " ", cleanPath, formatBytes(uint64(stats.Size())))
			}
		}
	default:
		// need to check completion?

	}

}

type FileInfo struct {
	Name  string
	IsDir bool
	Size  int64
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

func walkGo(dir string, wg *sync.WaitGroup, limitworkers uint32, workers *uint32) {
	wg.Add(1)
	defer wg.Done()
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}
	for _, file := range files {
		var cleanPath = path.Join(dir, file.Name())
		if file.IsDir() {
			atomic.AddUint64(&countDirs, 1)
			// fmt.Println(cleanPath, file.IsDir())
			// cheesey simple work-stealing
			if atomic.LoadUint32(workers) < limitworkers {
				atomic.AddUint32(workers, 1)
				go walkGo(cleanPath, wg, limitworkers, workers)
			} else {
				walkGo(cleanPath, wg, limitworkers, workers)
			}
		} else {
			stats, err_st := file.Info()
			if err_st != nil {
				fmt.Println("... Error reading file info:", err_st)
			}
			sz := stats.Size()
			atomic.AddUint64(&countFiles, 1)

			atomic.AddUint64(&totalSize, uint64(sz))
			// fmt.Println(modeToString(stats.Mode()), " ", cleanPath, formatBytes(sz))
		}
	}
}

func main() {
	start := time.Now()
	absPath, err := filepath.Abs("/")
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	wg := &sync.WaitGroup{}
	var msg = "tree scan"
	ticker := create_start_ticker(&msg)
	walkGo(absPath, wg, 10, new(uint32))

	wg.Wait()
	elapse := time.Since(start)
	ticker.Stop()
	fmt.Println("Total size:", formatBytes(uint64(totalSize)), " in ", countFiles, " files and ", countDirs, " directories", " done in ", elapse)

}

func _main() {
	absPath, err := filepath.Abs("..")
	if err != nil {
		fmt.Println("Error getting absolute path:", err)
		return
	}
	walkDirectory(absPath)
}
