// Copyright 2018 by Harald Weidner <hweidner@gmx.net>. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// Version 3 that can be found in the LICENSE.txt file.

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// BUFSIZE defines the size of the buffer used for copying. It is currently 64kB.
const BUFSIZE = 1024 * 1024

type Counter struct {
	files, bytes uint64
}

type File struct {
	name string
	info os.FileInfo
}

// Buffer, Channels and Synchronization
var (
	buffer    [][BUFSIZE]byte
	counters  []Counter
	dch       = make(chan string, 1000) // dispatcher channel - get work into work queue
	wch       = make(chan string, 1000) // worker channel - get work from work queue to copy thread
	fch       = make(chan File, 1000)   // file channel
	wg        sync.WaitGroup            // waitgroup for work queue length
	wgf       sync.WaitGroup            // waitgroup for work queue length
	inflights atomic.Int32
)

// Commandline Flags
var (
	threads        uint   // number of threads
	src, dest      string // source and destination directory
	verbose, quiet bool   // verbose and quiet flags
	times, owner   bool   // preserve timestamps and owner flag
	create         bool   // create destination directory flag
)

func main() {
	// parse commandline flags
	flags()

	// check or create the destination directory
	prepareDestDir()

	// clear umask, so that it does not interfere with explicite permissions
	// used in os.FileOpen()
	syscall.Umask(0000)

	// initialize buffers
	buffer = make([][BUFSIZE]byte, threads)
	counters = make([]Counter, threads)

	// Start dispatcher and copy threads
	go dispatcher()
	for i := uint(0); i < threads; i++ {
		go copyDir(i)
		go copyFile(i)
	}

	// start copying top level directory
	wgf.Add(int(threads))
	wg.Add(1)
	dch <- ""

	go func() {
		var lastF, lastB uint64
		//statLen := 10
		intervalSecondCoefs := 1.0
		start := time.Now()
		//bars := 50
		for {
			time.Sleep(time.Second / time.Duration(intervalSecondCoefs))
			var f, b uint64
			for _, c := range counters {
				f += c.files
				b += c.bytes
			}
			sinceSec := time.Since(start).Seconds()
			fmt.Printf("instant: %03.3fk files/s\t%02.3f GB/s\t% 4d inflights\t\tavg: %03.3fk files/s\t%02.3f GB/s\n",
				float64(f-lastF)*intervalSecondCoefs/1000,
				float64(b-lastB)*intervalSecondCoefs/1000000000,
				inflights.Load(),
				float64(f)/sinceSec/1000,
				float64(b)/sinceSec/1000000000,
			)
			lastB, lastF = b, f
		}
	}()

	// wait for work queue to get empty
	wg.Wait()
	close(fch)
	wgf.Wait()
}

// Function flags parses the command line flags and checks them for sanity.
func flags() {
	flag.UintVar(&threads, "threads", 16, "Number of threads to run in parallel")
	flag.BoolVar(&verbose, "verbose", false, "Verbose mode")
	flag.BoolVar(&quiet, "quiet", false, "Quiet mode")
	flag.BoolVar(&times, "times", false, "Preserve time stamps")
	flag.BoolVar(&owner, "owner", false, "Preserve user/group ownership (root only)")
	flag.BoolVar(&create, "create", false, "Create destination directory, if needed (with standard permissions)")
	flag.Parse()

	if flag.NArg() != 2 || flag.Arg(0) == "" || flag.Arg(1) == "" || threads > 2048 {
		usage()
	}

	if threads == 0 {
		threads = 16
	}
	src = flag.Arg(0)
	dest = flag.Arg(1)
}

// Function usage prints a message about how to use psync, and exits.
func usage() {
	fmt.Println("Usage: psync [options] source destination")
	flag.Usage()
	os.Exit(1)
}

// Function prepareDestDir checks for the existence of the destination,
// or creates it if the flag '-create' is set.
func prepareDestDir() {
	if create {
		// create destination directory
		err := os.MkdirAll(dest, os.FileMode(0777))
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR - unable to create destination dir %s: %s\n", dest, err)
			os.Exit(1)
		}
	} else {
		// test the existence of destination directory prior to syncing
		stat, err := os.Stat(dest)
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "ERROR - destination directory %s does not exist: %s.\nUse '-create' to create it.\n", dest, err)
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR - cannot stat() destination directory %s: %s.\n", dest, err)
			os.Exit(1)
		}
		if !stat.IsDir() {
			fmt.Fprintf(os.Stderr, "ERROR - destination %s exists, but is not a directory\n", dest)
			os.Exit(1)
		}
	}
}

// Function dispatcher maintains a work list of potentially arbitrary size.
// Incoming directories (over the dispather channel) will be forwarded to a
// copy thread through the worker channel, or stored in the work list if no
// copy thread is available. For easier memory handling, the work list is
// treated last-in-first-out.
func dispatcher() {
	worklist := make([]string, 0, 1000)
	var dir string
	for {
		if len(worklist) == 0 {
			dir = <-dch
			worklist = append(worklist, dir)
		} else {
			select {
			case dir = <-dch:
				worklist = append(worklist, dir)
			case wch <- worklist[len(worklist)-1]:
				worklist = worklist[:len(worklist)-1]
			}
		}
	}
}

// Function copyDir receives a directory on the worker channel and copies its
// content from src to dest. Files are copied sequentially. If a subdirectory
// is discovered, it is created on the destination side, and then inserted into
// the work queue through the dispatcher channel.
func copyDir(id uint) {

	printCurrent := func(dir string) {}

	{
		if id == 0 {
			printer := time.NewTicker(5 * time.Second)
			under := printCurrent
			printCurrent = func(dir string) {
				select {
				case <-printer.C:
					fmt.Printf("copying: %v\n", dir)
				default:
				}

				under(dir)
			}
		}
	}

	for {
		// read next directory to handle
		dir := <-wch
		printCurrent(dir)
		if verbose {
			fmt.Printf("[%d] Handling directory %s%s\n", id, src, dir)
		}

		// read directory content
		files, err := ioutil.ReadDir(src + dir)
		if err != nil {
			if !quiet {
				fmt.Fprintf(os.Stderr, "WARNING - could not read directory %s: %s\n", src+dir, err)
			}
			wg.Done()
			continue
		}

		for _, f := range files {
			fname := f.Name()
			if fname == "." || fname == ".." {
				continue
			}

			if f.IsDir() {
				// create directory on destination side
				perm := f.Mode().Perm()
				err := os.Mkdir(dest+dir+"/"+fname, perm)
				if err != nil {
					if !quiet {
						fmt.Fprintf(os.Stderr, "WARNING - could not create directory %s: %s\n",
							dest+dir+"/"+fname, err)
					}
					continue
				}

				// submit directory to work queue
				wg.Add(1)
				dch <- dir + "/" + fname
			} else {
				// copy file sequentially
				if verbose {
					fmt.Printf("[%d] Copying %s%s/%s to %s%s/%s\n",
						id, src, dir, fname, dest, dir, fname)
				}
				fch <- File{name: dir + "/" + fname, info: f}
			}
		}
		finfo, err := os.Stat(src + dir)
		if err != nil {
			if !quiet {
				fmt.Fprintf(os.Stderr, "WARNING - could not read fileinfo of directory %s: %s\n",
					dest+dir, err)
			}
		} else {
			// preserve user and group of the destination directory
			if owner {
				preserveOwner(dest+dir, finfo, "directory")
			}
			// setting the timestamps of the destination directory
			if times {
				preserveTimes(dest+dir, finfo, "directory")
			}
		}
		if verbose {
			fmt.Printf("[%d] Finished directory %s%s\n", id, src, dir)
		}
		wg.Done()
	}
}

// Function copyFile copies a file from the source to the destination directory.
func copyFile(id uint) {

	countF := func(written int) {
		counters[id].bytes += uint64(written)
	}

	cpf := func(file string, f os.FileInfo) {
		mode := f.Mode()
		switch {
		case mode&os.ModeSymlink != 0: // symbolic link
			// read link
			link, err := os.Readlink(src + file)
			if err != nil {
				if !quiet {
					fmt.Fprintf(os.Stderr, "WARNING - link %s disappeared while copying %s\n", src+file, err)
				}
				return
			}

			// write link to destination
			err = os.Symlink(link, dest+file)
			if err != nil {
				if !quiet {
					fmt.Fprintf(os.Stderr, "WARNING - link %s could not be created: %s\n", dest+file, err)
				}
				return
			}

			// preserve owner of symbolic link
			if owner {
				preserveOwner(dest+file, f, "link")
			}
			// preserving the timestamps of links seems not be supported in Go
			// TODO: it should be possible by using the futimesat system call,
			// see https://github.com/golang/go/issues/3951
			//if times {
			//	preserveTimes(dest+file, f, "link")
			//}

		case mode&(os.ModeDevice|os.ModeNamedPipe|os.ModeSocket) != 0: // special files
		// TODO: not yet implemented

		default:
			// copy regular file
			// open source file for reading
			rd, err := os.Open(src + file)
			if err != nil {
				if !quiet {
					fmt.Fprintf(os.Stderr, "WARNING - file %s disappeared while copying: %s\n", src+file, err)
				}
				return
			}
			defer rd.Close()

			// open destination file for writing
			perm := mode.Perm()
			wr, err := os.OpenFile(dest+file, os.O_WRONLY|os.O_CREATE|syscall.O_DIRECT, perm)
			if err != nil {
				if !quiet {
					fmt.Fprintf(os.Stderr, "WARNING - file %s could not be created: %s\n", dest+file, err)
				}
				return
			}
			defer wr.Close()

			// copy data
			inflights.Add(1)
			_, err = io.CopyBuffer(&CounterWriter{w: wr, count: countF}, rd, buffer[id][:])
			inflights.Add(-1)
			if err != nil {
				if !quiet {
					fmt.Fprintf(os.Stderr, "WARNING - file %s could not be created: %s\n", dest+file, err)
				}
				return
			}

			counters[id].files++

			if owner {
				preserveOwner(dest+file, f, "file")
			}
			if times {
				preserveTimes(dest+file, f, "file")
			}
		}
	}

	for fcp := range fch {
		cpf(fcp.name, fcp.info)
	}
	wgf.Done()
}

type CounterWriter struct {
	w     io.Writer
	count func(written int)
}

func (cw *CounterWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.count(n)
	return
}

// Function preserveOwner transfers the ownership information from the source to
// the destination file/directory.
func preserveOwner(name string, f os.FileInfo, ftype string) {
	if stat, ok := f.Sys().(*syscall.Stat_t); ok {
		uid := int(stat.Uid)
		gid := int(stat.Gid)

		var err error
		if ftype == "link" {
			err = syscall.Lchown(name, uid, gid)
		} else {
			err = os.Chown(name, uid, gid)
		}

		if err != nil && !quiet {
			fmt.Fprintf(os.Stderr, "WARNING - could not change ownership of %s %s: %s\n",
				ftype, name, err)
		}
	}
}

// Function preserveTimes transfers the access and modification timestamp from
// the source to the destination file/directory.
func preserveTimes(name string, f os.FileInfo, ftype string) {
	mtime := f.ModTime()
	atime := mtime
	if stat, ok := f.Sys().(*syscall.Stat_t); ok {
		atime = time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
	}
	err := os.Chtimes(name, atime, mtime)
	if err != nil && !quiet {
		fmt.Fprintf(os.Stderr, "WARNING - could not change timestamps for %s %s: %s\n",
			ftype, name, err)
	}
}
