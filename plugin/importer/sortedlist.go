package importer

import (
	"sort"

	"github.com/algorand/conduit/conduit/data"
)

type sortedList struct {
	values []*data.BlockData
}

func (sl *sortedList) Push(blk *data.BlockData) {
	sl.values = append(sl.values, blk)
	sort.Slice(sl.values, func(i, j int) bool {
		return sl.values[i].Round() > sl.values[j].Round()
	})
}

func (sl *sortedList) Len() int {
	return len(sl.values)
}

func (sl *sortedList) Min() uint64 {
	l := len(sl.values)
	return sl.values[l-1].Round()
}

func (sl *sortedList) PopMin() *data.BlockData {
	l := len(sl.values)
	tmp := sl.values[l-1]
	sl.values = sl.values[0 : l-1]
	return tmp
}
