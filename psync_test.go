// Copyright 2018 by Harald Weidner <hweidner@gmx.net>. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// Version 3 that can be found in the LICENSE.txt file.

package main

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestOffsetLimitReader(t *testing.T) {
	base := "mathieu"
	test := func(offset, max int) {
		read, _ := io.ReadAll(&OffsetLimitReader{r: strings.NewReader(base), offset: int64(offset), max: max})
		fmt.Printf("%s :: offet %d max %d read: %#v\n", base, offset, max, string(read))
	}

	test(0, 100)
	test(1, 100)
	test(2, 100)
	test(3, 100)
	test(3, 1)
	test(3, 2)
	test(3, 3)
	test(3, 4)
	test(3, 5)
	test(3, 6)
	test(300, 6)

	println("--------")

	reader := &OffsetLimitReader{r: strings.NewReader(base), offset: 1, max: 5}
	var buf [2]byte
	for i := 0; i < 4; i++ {
		n, err := reader.Read(buf[:])
		fmt.Printf("n: %v, err %v\n", n, err)
		fmt.Printf("string(buf[:]): %v\n", string(buf[:]))
	}
}

func TestFileParts(t *testing.T) {
	test := func(totalSize, minPartSize, maxParts int) {
		fmt.Printf("totalSize: %v\n", totalSize)
		fmt.Printf("minPartSize: %v\n", minPartSize)
		fmt.Printf("maxParts: %v\n", maxParts)
		offsets := FileParts(totalSize, minPartSize, maxParts)
		fmt.Printf("offsets: %v\n", offsets)
		println(" - - - - - -")
	}

	test(5, 1, 4)
	test(10, 2, 100)
	test(10, 2, 3)
	test(11, 2, 3)
	test(12, 2, 3)
	test(101, 10, 4)
	test(20, 13, 3)
	test(100, 44, 1)
	test(100, 44, 2)
	test(100, 50, 1)
	test(100, 51, 2)
	test(100, 50, 2)
	test(199, 50, 2)
	test(99, 12, 20)
	test(5, 10, 20)
}

func TestFilePartsStartEnd(t *testing.T) {
	partsOffsets := FileParts(100, 22, 16)
	fmt.Printf("partsOffsets: %v\n", partsOffsets)
	for i, end := range partsOffsets[1:] {
		start := partsOffsets[i]
		fmt.Printf("%v -> %v\n", start, end)
	}
}
