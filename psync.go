// export GOMEMLIMIT=512MiB GOGC=off CRAWL_WORKERS=128 # sudo -E ./psync
package main

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"runtime/debug"
	"slices"
	"syscall"
)

const MAX_WORKERS = 5000

type Stat struct{ size, files int64 }

type Stats map[uint32]Stat

func (stats *Stats) Add(other Stats) {
	for u, s := range other {
		stat := (*stats)[u]
		stat.files += s.files
		stat.size += s.size
		(*stats)[u] = stat
	}
}

func (stats *Stats) FPrint(w io.Writer) {

	ids := make([]uint32, 0, len(*stats))
	for uID := range *stats {
		ids = append(ids, uID)
	}
	slices.SortFunc(ids, func(a, b uint32) int { return int((*stats)[b].size - (*stats)[a].size) })

	for _, uID := range ids {
		stat := (*stats)[uID]
		userName := fmt.Sprint(uID)
		if u, err := user.LookupId(fmt.Sprint(uID)); err == nil {
			userName = u.Username
			if u.Name != "" {
				userName += " (" + u.Name + ")"
			}
		}
		fmt.Fprintf(w, "% 50s => % 5s\n", userName, formatBigNum(uint64(stat.size)))
	}
}

type Task struct {
	dir   string
	stats chan Stats
}

func crawlDir(dir string, tasks chan<- Task) (stats Stats) {
	stats = make(Stats)
	openedDir, err := os.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING - could not open directory %s: %s\n", dir, err)
		return
	}
	defer openedDir.Close()

	async := make(chan Stats)
	asyncCount := 0

	for {
		entries, err := openedDir.ReadDir(1000)
		if err != nil || len(entries) == 0 {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "WARNING - failed to read directory %s: %s\n", dir, err)
			}
			break
		}
		for _, entry := range entries {
			f, err := entry.Info()
			if err != nil {
				fmt.Fprintf(os.Stderr, "WARNING - failed to get info of dir entry %s: %s\n", entry.Name(), err)
				continue
			}

			if f.IsDir() {
				select {
				case tasks <- Task{dir: dir + "/" + f.Name(), stats: async}:
					asyncCount++
				default:
					stats.Add(crawlDir(dir+"/"+f.Name(), tasks))
				}
				continue
			}

			if sysStat, ok := f.Sys().(*syscall.Stat_t); ok {
				stat := stats[sysStat.Uid]
				stat.size += f.Size()
				stat.files += 1
				stats[sysStat.Uid] = stat
			}
		}
	}

	for i := 0; i < asyncCount; i++ {
		stats.Add(<-async)
	}
	close(async)

	return
}

func main() {

	debug.SetMemoryLimit(1_000_000_000)
	debug.SetGCPercent(-1)

	tasks := make(chan Task)
	workers := MAX_WORKERS
	if w := os.Getenv("CRAWL_WORKERS"); w != "" {
		fmt.Sscanf(w, "%d", &workers)
		if workers > MAX_WORKERS || workers < 1 {
			panic(fmt.Errorf("worker pool size must be between 1 and %d", MAX_WORKERS))
		}
	}

	fmt.Fprintf(os.Stderr, "intiating %d workers\n", workers)
	for i := 0; i < workers; i++ {
		go func() {
			for task := range tasks {
				task.stats <- crawlDir(task.dir, tasks)
			}
		}()
	}

	async := make(chan Stats)
	for _, d := range os.Args[1:] {
		tasks <- Task{dir: d, stats: async}
	}

	stats := Stats{}
	for range os.Args[1:] {
		stats.Add(<-async)
	}

	stats.FPrint(os.Stdout)
}

func formatBigNum(n uint64) string {
	if n < 1_000 {
		return fmt.Sprint(n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("% 3.1fk", float32(n)/1_000)
	}
	if n < 1_000_000_000 {
		return fmt.Sprintf("% 3.1fM", float32(n)/1_000_000)
	}
	if n < 1_000_000_000_000 {
		return fmt.Sprintf("% 3.1fG", float32(n)/1_000_000_000)
	}
	return fmt.Sprintf("% 3.1fT", float32(n)/1_000_000_000_000)
}
