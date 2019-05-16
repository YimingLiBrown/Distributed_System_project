/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: contains all interval related logic.
 */

package liteminer

// Represents [Lower, Upper)
type Interval struct {
	Lower uint64 // Inclusive
	Upper uint64 // Exclusive
}

// GenerateIntervals divides the range [0, upperBound] into numIntervals
// intervals.
func GenerateIntervals(upperBound uint64, numIntervals int) (intervals []Interval) {
	intervals = make([]Interval, numIntervals)
	div := upperBound / uint64(numIntervals)
	for i := 0; i < numIntervals; i++ {
		intervals[i].Lower = div * uint64(i)
		intervals[i].Upper = div * uint64(i+1)
	}
	if intervals[numIntervals-1].Upper != upperBound { // 需要平均分嘛？
		intervals[numIntervals-1].Upper = upperBound
	}

	return intervals
}
