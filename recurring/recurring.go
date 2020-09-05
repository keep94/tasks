// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package recurring handles recurring times.
package recurring

import (
	"github.com/keep94/common"
	"github.com/keep94/gofunctional3/functional"
	"time"
)

type DaysOfWeek int

const (
	Sunday DaysOfWeek = 1 << iota
	Saturday
	Friday
	Thursday
	Wednesday
	Tuesday
	Monday
)

const (
	Weekdays = Monday | Tuesday | Wednesday | Thursday | Friday
	Weekend  = Saturday | Sunday
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
	var aggregate combinedRecurring
	return common.Join(rs, aggregate, nilRecurring{}).(R)
}

// Filter returns a new R instance that filters the time.Time Streams
// that r creates
func Filter(r R, f functional.Filterer) R {
	if nested, ok := r.(*filterR); ok {
		return &filterR{
			recurring: nested.recurring, filter: functional.All(nested.filter, f)}
	}
	return &filterR{recurring: r, filter: f}
}

// After returns a new R instance that represents duration d after every time
// in r
func After(r R, d time.Duration) R {
	if nested, ok := r.(*afterR); ok {
		return &afterR{recurring: nested.recurring, after: nested.after + d}
	}
	return &afterR{recurring: r, after: d}
}

// StartAt returns a new R that is the same as r but contains only the times
// on or after startTime.
func StartAt(r R, startTime time.Time) R {
	if nested, ok := r.(*startUntilR); ok {
		return nested.AddStart(startTime)
	}
	return withStart(r, startTime)
}

// Until returns a new R that is the same as r but contains only times before t.
func Until(r R, t time.Time) R {
	if nested, ok := r.(*startUntilR); ok {
		return nested.AddUntil(t)
	}
	return withUntil(r, t)
}

// AtInterval returns a new R instance that represents starting at time
// start and repeating at d intervals.
func AtInterval(start time.Time, d time.Duration) R {
	return RFunc(func(t time.Time) functional.Stream {
		if t.Before(start) {
			return &intervalStream{t: start, d: d}
		}
		durationCount := t.Sub(start)/d + 1
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

// Nil returns a new R instance that represents never happening.
func Nil() R {
	return nilRecurring{}
}

// OnDays filters times by day of week. dayMask is the desired days of the
// week ored together e.g functional.Monday | functional.Tuesday
func OnDays(dayMask DaysOfWeek) functional.Filterer {
	return functional.NewFilterer(func(ptr interface{}) error {
		p := ptr.(*time.Time)
		ourWeekday := uint((7 - p.Weekday()) % 7)
		if dayMask&(1<<ourWeekday) != 0 {
			return nil
		}
		return functional.Skipped
	})
}

type afterR struct {
	recurring R
	after     time.Duration
}

func (r *afterR) Filter(ptr interface{}) error {
	p := ptr.(*time.Time)
	*p = (*p).Add(r.after)
	return nil
}

func (r *afterR) ForTime(t time.Time) functional.Stream {
	return functional.Filter(r, r.recurring.ForTime(t.Add(-1*r.after)))
}

type startUntilR struct {
	recurring R
	start     time.Time
	until     time.Time
	startSet  bool
	untilSet  bool
}

func withStart(r R, t time.Time) *startUntilR {
	return &startUntilR{
		recurring: r,
		start:     t.Add(-1 * time.Nanosecond),
		startSet:  true}
}

func (r *startUntilR) AddStart(t time.Time) *startUntilR {
	t = t.Add(-1 * time.Nanosecond)
	if r.startSet && !t.After(r.start) {
		return r
	}
	result := *r
	result.start = t
	result.startSet = true
	return &result
}

func withUntil(r R, t time.Time) *startUntilR {
	return &startUntilR{
		recurring: r,
		until:     t,
		untilSet:  true}
}

func (r *startUntilR) AddUntil(t time.Time) *startUntilR {
	if r.untilSet && !t.Before(r.until) {
		return r
	}
	result := *r
	result.until = t
	result.untilSet = true
	return &result
}

func (r *startUntilR) Filter(ptr interface{}) error {
	p := ptr.(*time.Time)
	if p.Before(r.until) {
		return nil
	}
	return functional.Skipped
}

func (r *startUntilR) ForTime(t time.Time) functional.Stream {
	if r.startSet && t.Before(r.start) {
		t = r.start
	}
	result := r.recurring.ForTime(t)
	if r.untilSet {
		return functional.TakeWhile(r, result)
	}
	return result
}

type filterR struct {
	recurring R
	filter    functional.Filterer
}

func (r *filterR) ForTime(t time.Time) functional.Stream {
	result := r.recurring.ForTime(t)
	return functional.Filter(r.filter, result)
}

type combinedRecurring []R

func (rs combinedRecurring) ForTime(t time.Time) functional.Stream {
	streams := make([]functional.Stream, len(rs))
	for i := range rs {
		streams[i] = rs[i].ForTime(t)
	}
	return combineStreams(streams)
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
	started  bool
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

type nilRecurring struct {
}

func (n nilRecurring) ForTime(t time.Time) functional.Stream {
	return functional.NilStream()
}
