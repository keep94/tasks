// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package recurring_test

import (
	"fmt"
	"github.com/keep94/tasks/recurring"
	"time"
)

func ExampleFilter() {
	// Weekdays at 7:00
	r := recurring.Filter(
		recurring.AtTime(7, 0),
		recurring.OnDays(recurring.Weekdays))
	stream := r.ForTime(time.Date(2013, 10, 1, 7, 0, 0, 0, time.Local))
	layout := "Mon Jan 2 15:04:05"
	var current time.Time
	for i := 0; i < 5; i++ {
		stream.Next(&current)
		fmt.Println(current.Format(layout))
	}
	// Output:
	// Wed Oct 2 07:00:00
	// Thu Oct 3 07:00:00
	// Fri Oct 4 07:00:00
	// Mon Oct 7 07:00:00
	// Tue Oct 8 07:00:00
}

func ExampleCombine() {
	// Daily at 10:09, 13:05, and 17:13
	r := recurring.Combine(
		recurring.AtTime(10, 9),
		recurring.AtTime(13, 5),
		recurring.AtTime(17, 13))
	stream := r.ForTime(time.Date(2013, 10, 2, 12, 0, 0, 0, time.Local))
	layout := "Jan 2 15:04:05"
	var current time.Time
	for i := 0; i < 8; i++ {
		stream.Next(&current)
		fmt.Println(current.Format(layout))
	}
	// Output:
	// Oct 2 13:05:00
	// Oct 2 17:13:00
	// Oct 3 10:09:00
	// Oct 3 13:05:00
	// Oct 3 17:13:00
	// Oct 4 10:09:00
	// Oct 4 13:05:00
	// Oct 4 17:13:00
}
