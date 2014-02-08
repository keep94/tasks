// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package recurring handles recurring times.
package recurring

import (
  "github.com/keep94/gofunctional3/functional"
  "time"
)

type DaysOfWeek int

const (
  Sunday DaysOfWeek = 1<<iota
  Saturday
  Friday
  Thursday
  Wednesday
  Tuesday
  Monday
)

const (
  Weekdays = Monday | Tuesday | Wednesday | Thursday | Friday
  Weekend = Saturday | Sunday
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

// Filter returns a new R instance that filters the time.Time Streams
// that r creates
func Filter(r R, f functional.Filterer) R {
  return modify(
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

// StartAt returns a new R that is the same as r but contains only the times
// on or after startTime.
func StartAt(r R, startTime time.Time) R {
  startTime = startTime.Add(-1 * time.Nanosecond)
  return RFunc(func(t time.Time) functional.Stream {
    if t.Before(startTime) {
      return r.ForTime(startTime)
    }
    return r.ForTime(t)
  })
}

// Until returns a new R that is the same as r but contains only times before t.
func Until(r R, t time.Time) R {
  return modify(
      r,
      func(s functional.Stream) functional.Stream {
        return functional.TakeWhile(
            functional.NewFilterer(func(ptr interface{}) error {
              p := ptr.(*time.Time)
              if p.Before(t) {
                return nil
              }
              return functional.Skipped
            }),
            s)
      })
}

// AtInterval returns a new R instance that represents starting at time
// start and repeating at d intervals.
func AtInterval(start time.Time, d time.Duration) R {
  return RFunc(func(t time.Time) functional.Stream {
    if t.Before(start) {
      return &intervalStream{t: start, d: d}
    }
    durationCount := t.Sub(start) / d + 1
    return &intervalStream{t: start.Add(durationCount * d), d: d}
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
func OnDays(dayMask DaysOfWeek) functional.Filterer {
  return functional.NewFilterer(func(ptr interface{}) error {
    p := ptr.(*time.Time)
    ourWeekday := uint((7 - p.Weekday()) % 7)
    if dayMask & (1 << ourWeekday) != 0 {
      return nil
    }
    return functional.Skipped
  })
}

func modify(r R, f func(s functional.Stream) functional.Stream) R {
  return RFunc(func(t time.Time) functional.Stream {
    return f(r.ForTime(t))
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
  allTimes := functional.Merge(
      func() interface{} { return new(time.Time) },
      nil,
      func(lhs, rhs interface{}) bool {
        l := lhs.(*time.Time)
        r := rhs.(*time.Time)
        return l.Before(*r)
      }, 
      streams...)
  return &uniqueStream{Stream: allTimes}
}

type uniqueStream struct {
  functional.Stream
  lastTime time.Time
  started bool
}

func (s *uniqueStream) Next(ptr interface{}) (err error) {
  for err = s.Stream.Next(ptr); err == nil; err = s.Stream.Next(ptr) {
    current := *ptr.(*time.Time)
    if s.started && current == s.lastTime {
      continue
    }
    s.started = true
    s.lastTime = current
    return
  }
  return
}

