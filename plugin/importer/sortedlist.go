package importer

import "sort"

type sortedList struct {
	values []uint64
}

func (sl *sortedList) Push(round uint64) {
	sl.values = append(sl.values, round)
	sort.Slice(sl.values, func(i, j int) bool {
		return sl.values[i] > sl.values[j]
	})
}

func (sl *sortedList) Len() int {
	return len(sl.values)
}

func (sl *sortedList) Min() uint64 {
	l := len(sl.values)
	return sl.values[l-1]
}

func (sl *sortedList) PopMin() uint64 {
	l := len(sl.values)
	tmp := sl.values[l-1]
	sl.values = sl.values[0 : l-1]
	return tmp
}
