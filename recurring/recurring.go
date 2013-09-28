// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package recurring contains implementations of the the Recurring interface
// in the tasks package.
package recurring

import (
  "container/heap"
  "github.com/keep94/gofunctional3/functional"
  "time"
)

const (
  Sunday = 1<<iota
  Saturday
  Friday
  Thursday
  Wednesday
  Tuesday
  Monday
)

const (
  Weekdays = Monday | Tuesday | Wednesday | Thursday | Friday
  Weedend = Saturday | Sunday
)

var (
  kOnTheHour = RFunc(func(t time.Time) functional.Stream {
    t = time.Date(
        t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
    return &intervalStream{t: t.Add(time.Hour), d: time.Hour}
  })
)

// R represents a recurring time such as each Monday at 7:00.
type R interface {

  // ForTime returns a Stream of time.Time starting at t. The times that
  // returned Stream emits shall be after t and be in ascending order.
  ForTime(t time.Time) functional.Stream
}

// RFunc converts an ordinary function to an R instance
type RFunc func(t time.Time) functional.Stream

func (f RFunc) ForTime(t time.Time) functional.Stream {
  return f(t)
}

// Combine combines multiple R instances together and returns them
// as a single one.
func Combine(rs ...R) R {
  return RFunc(func(t time.Time) functional.Stream {
    streams := make([]functional.Stream, len(rs))
    for i := range rs {
      streams[i] = rs[i].ForTime(t)
    }
    return combineStreams(streams)
  })
}

// Modify returns a new R instance that uses f to modify the time.Time
// Streams that r creates.
func Modify(r R, f func(s functional.Stream) functional.Stream) R {
  return RFunc(func(t time.Time) functional.Stream {
    return f(r.ForTime(t))
  })
}

// Filter returns a new R instance that filters the time.Time Streams
// that r creates
func Filter(r R, f functional.Filterer) R {
  return Modify(
      r,
      func(s functional.Stream) functional.Stream {
        return functional.Filter(f, s)
      })
}

// After returns a new R instance that represents duration d after every time
// in r
func After(r R, d time.Duration) R {
  return RFunc(func(t time.Time) functional.Stream {
    return functional.Filter(
        functional.NewFilterer(func(ptr interface{}) error {
          p := ptr.(*time.Time)
          *p = (*p).Add(d)
          return nil
        }),
        r.ForTime(t.Add(-1 * d)))
  })
}

// FirstN returns a new R instance that generates only the first N
// times that r generates.
func FirstN(r R, n int) R {
  return Modify(
      r,
      func(s functional.Stream) functional.Stream {
        return functional.Slice(s, 0, n)
      })
}

// AtInterval returns a new R instance that represents repeating
// at d intervals.
func AtInterval(d time.Duration) R {
  return RFunc(func(t time.Time) functional.Stream {
    return &intervalStream{t: t.Add(d), d: d}
  })
}

// OnTheHour returns an R instance that represents repeating at the start of
// each hour.
func OnTheHour() R {
  return kOnTheHour
}
  
// AtTime returns a new R instance that represents repeating at a
// certain time of day.
func AtTime(hour24, minute int) R {
  return RFunc(func(t time.Time) functional.Stream {
    firstT := time.Date(t.Year(), t.Month(), t.Day(), hour24, minute, 0, 0, t.Location())
    if !firstT.After(t) {
      firstT = firstT.AddDate(0, 0, 1)
    }
    return &dateStream{t: firstT}
  })
}

// OnDate returns a new R instance that represents happening once on a
// particular date and time.
func OnDate(targetTime time.Time) R {
  return RFunc(func(t time.Time) functional.Stream {
    if targetTime.After(t) {
      return functional.NewStreamFromValues([]time.Time{targetTime}, nil)
    }
    return functional.NilStream()
  })
}

// OnDays filters times by day of week. dayMask is the desired days of the
// week ored together e.g functional.Monday | functional.Tuesday
func OnDays(dayMask int) functional.Filterer {
  return functional.NewFilterer(func(ptr interface{}) error {
    p := ptr.(*time.Time)
    ourWeekday := uint((7 - p.Weekday()) % 7)
    if dayMask & (1 << ourWeekday) != 0 {
      return nil
    }
    return functional.Skipped
  })
}

type dateStream struct {
  t time.Time
  closeDoesNothing
}

func (s *dateStream) Next(ptr interface{}) error {
  p := ptr.(*time.Time)
  *p = s.t
  s.t = s.t.AddDate(0, 0, 1)
  return nil
}

type intervalStream struct {
  t time.Time
  d time.Duration
  closeDoesNothing
}

func (s *intervalStream) Next(ptr interface{}) error {
  p := ptr.(*time.Time)
  *p = s.t
  s.t = s.t.Add(s.d)
  return nil
}

type closeDoesNothing struct {
}

func (n closeDoesNothing) Close() error {
  return nil
}

func combineStreams(streams []functional.Stream) functional.Stream {
  h := make(streamHeap, len(streams))
  for i := range streams {
    h[i] = &item{stream: streams[i]}
    h[i].pop()
  }
  heap.Init(&h)
  return &mergeStream{orig: streams, sh: &h}
}

type item struct {
  stream functional.Stream
  t time.Time
  e error
}

func (i *item) pop() {
  i.e = i.stream.Next(&i.t)
}

type streamHeap []*item

func (sh streamHeap) Len() int {
  return len(sh)
}

func (sh streamHeap) Less(i, j int) bool {
  if sh[i].e != nil {
    return sh[j].e == nil
  }
  if sh[j].e != nil {
    return false
  }
  return sh[i].t.Before(sh[j].t)
}

func (sh streamHeap) Swap(i, j int) {
  sh[i], sh[j] = sh[j], sh[i]
}

func (sh *streamHeap) Push(x interface{}) {
  *sh = append(*sh, x.(*item))
}

func (sh *streamHeap) Pop() interface{} {
  old := *sh
  n := len(old)
  *sh = old[0:n - 1]
  return old[n - 1]
}

type mergeStream struct {
  orig []functional.Stream
  sh *streamHeap
  lastEmitted time.Time
  started bool
}

func (s *mergeStream) Next(ptr interface{}) error {
  for s.sh.Len() > 0 {
    aitem := heap.Pop(s.sh).(*item)
    if aitem.e == functional.Done {
      continue
    }
    if aitem.e != nil {
      return aitem.e
    }
    if s.started && aitem.t == s.lastEmitted {
      aitem.pop()
      heap.Push(s.sh, aitem)
      continue
    }
    p := ptr.(*time.Time)
    *p = aitem.t
    s.started = true
    s.lastEmitted = aitem.t
    aitem.pop()
    heap.Push(s.sh, aitem)
    return nil
  }
  return functional.Done
}
      
func (s *mergeStream) Close() error {
  var result error
  for _, stream := range s.orig {
    if err := stream.Close(); err != nil {
      result = err
    }
  }
  return result
}
