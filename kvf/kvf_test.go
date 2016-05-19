package kvf

import (
	"bytes"
	"math/rand"
	"testing"
)

func randomSliceFix(length int) []byte {
	res := make([]byte, length)
	for k := range res {
		res[k] = byte(rand.Intn(0xFF))
	}
	return res
}

// [l, r)
func randomSlice(l, r int) []byte {
	return randomSliceFix(rand.Intn(r-l) + l)
}

func TestGetSet(t *testing.T) {
	kvfm := NewKVFBlockManager("st_test")
	const Blocks = 100
	blockIDs := make([]uint64, 0)
	for i := 0; i < Blocks; i++ {
		id, err := kvfm.AllocBlock()
		if err != nil {
			t.Fatal(err)
		}
		blockIDs = append(blockIDs, id)
	}

	contents := make([][]byte, 0)
	for i := 0; i < Blocks; i++ {
		contents = append(contents, randomSlice(20, 200000))
	}
	const Rewrite = 6
	for k, id := range blockIDs {
		for i := 0; i < Rewrite; i++ {
			blk := randomSlice(100000, 200000)
			err := kvfm.SetBlock(id, blk)
			if err != nil {
				t.Fatal(err)
			}
			blkRead, err := kvfm.GetBlock(id)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(blk, blkRead) {
				t.Errorf("Block %d (%d-th) got wrong data at cycle %d", id, k, i)
			}
		}
		err := kvfm.SetBlock(id, contents[k])
		if err != nil {
			t.Fatal(err)
		}
	}

	for k, id := range blockIDs {
		blkRead, err := kvfm.GetBlock(id)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents[k], blkRead) {
			t.Errorf("Block %d got wrong data at final check", id)
		}
	}
}

func BenchmarkRawSet262144(b *testing.B) {
	kvfm := NewKVFBlockManager("st_test")
	sl := randomSliceFix(262144)
	id, err := kvfm.AllocBlock()
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		err := kvfm.SetBlock(id, sl)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRawGet262144(b *testing.B) {
	kvfm := NewKVFBlockManager("st_test")
	sl := randomSliceFix(262144)
	id, err := kvfm.AllocBlock()
	if err != nil {
		b.Fatal(err)
	}
	kvfm.SetBlock(id, sl)
	for n := 0; n < b.N; n++ {
		_, err := kvfm.GetBlock(id)
		if err != nil {
			b.Fatal(err)
		}
	}
}
