// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package recurring_test

import (
	"github.com/keep94/gofunctional3/functional"
	"github.com/keep94/tasks/recurring"
	"testing"
	"time"
)

var (
	kNow = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
)

func TestAtTime(t *testing.T) {
	firstTime := time.Date(2013, 9, 13, 17, 21, 0, 0, time.Local)
	r := recurring.Until(recurring.AtTime(17, 21), firstTime.AddDate(0, 0, 3))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.AddDate(0, 0, 1),
		firstTime.AddDate(0, 0, 2))

	firstTime = time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
	r = recurring.Until(recurring.AtTime(17, 22), firstTime.AddDate(0, 0, 3))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.AddDate(0, 0, 1),
		firstTime.AddDate(0, 0, 2))
}

func TestOnDate(t *testing.T) {
	aDate := time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
	r := recurring.OnDate(aDate)
	verifyTimes(
		t,
		r.ForTime(kNow),
		aDate)

	aDate = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
	r = recurring.OnDate(aDate)
	verifyTimes(
		t,
		r.ForTime(kNow))
}

func TestOnDays(t *testing.T) {
	firstTime := time.Date(2013, 9, 12, 18, 0, 0, 0, time.Local)
	r := recurring.Until(
		recurring.Filter(
			recurring.AtTime(18, 0),
			recurring.OnDays(recurring.Weekdays)),
		firstTime.AddDate(0, 0, 8))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,                  // Thursday
		firstTime.AddDate(0, 0, 1), // Friday
		firstTime.AddDate(0, 0, 4), // Monday
		firstTime.AddDate(0, 0, 5), // Tuesday
		firstTime.AddDate(0, 0, 6), // Wednesday
		firstTime.AddDate(0, 0, 7)) // Thursday
}

func TestFilterNested(t *testing.T) {
	firstTime := time.Date(2013, 9, 12, 18, 0, 0, 0, time.Local)
	r := recurring.Until(
		recurring.Filter(
			recurring.Filter(
				recurring.AtTime(18, 0),
				recurring.OnDays(recurring.Weekdays)),
			functional.NewFilterer(func(ptr interface{}) error {
				p := ptr.(*time.Time)
				if p.Day()%2 != 0 {
					return functional.Skipped
				}
				return nil
			})),
		firstTime.AddDate(0, 0, 8))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,                  // Thursday
		firstTime.AddDate(0, 0, 4), // Monday
		firstTime.AddDate(0, 0, 6)) // Wednesday
}

func TestAfter(t *testing.T) {
	firstTime := time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
	r := recurring.Until(
		recurring.After(recurring.AtTime(14, 22), 3*time.Hour),
		firstTime.AddDate(0, 0, 2))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.AddDate(0, 0, 1))
	firstTime = time.Date(2013, 9, 13, 17, 21, 0, 0, time.Local)
	r = recurring.Until(
		recurring.After(recurring.AtTime(14, 21), 3*time.Hour),
		firstTime.AddDate(0, 0, 2))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.AddDate(0, 0, 1))
}

func TestAfterNested(t *testing.T) {
	firstTime := time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
	r := recurring.Until(
		recurring.After(
			recurring.After(recurring.AtTime(14, 22), 2*time.Hour),
			time.Hour),
		firstTime.AddDate(0, 0, 2))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.AddDate(0, 0, 1))
}

func TestAtInterval(t *testing.T) {
	r := recurring.Until(
		recurring.AtInterval(kNow, time.Hour), kNow.Add(4*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow),
		kNow.Add(time.Hour),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow.Add(-1*time.Second)),
		kNow,
		kNow.Add(time.Hour),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow.Add(-2*time.Hour)),
		kNow,
		kNow.Add(time.Hour),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow.Add(time.Hour)),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow.Add(59*time.Minute)),
		kNow.Add(time.Hour),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour))
}

func TestOnTheHour(t *testing.T) {
	firstTime := time.Date(2013, 9, 12, 18, 0, 0, 0, time.Local)
	r := recurring.Until(recurring.OnTheHour(), firstTime.Add(3*time.Hour))
	verifyTimes(
		t,
		r.ForTime(kNow),
		firstTime,
		firstTime.Add(time.Hour),
		firstTime.Add(2*time.Hour))
}

func TestCombine(t *testing.T) {
	r := recurring.Combine(
		recurring.Until(
			recurring.AtInterval(kNow, 2*time.Hour), kNow.Add(10*time.Hour)),
		recurring.Until(
			recurring.AtInterval(kNow, 3*time.Hour), kNow.Add(15*time.Hour)))
	verifyTimes(
		t,
		r.ForTime(kNow),
		kNow.Add(2*time.Hour),
		kNow.Add(3*time.Hour),
		kNow.Add(4*time.Hour),
		kNow.Add(6*time.Hour),
		kNow.Add(8*time.Hour),
		kNow.Add(9*time.Hour),
		kNow.Add(12*time.Hour))
}

func TestCombine2(t *testing.T) {
	r := recurring.Combine(recurring.Nil(), recurring.Nil())
	verifyTimes(
		t,
		r.ForTime(kNow))
}

func TestCombine3(t *testing.T) {
	r := recurring.Combine(
		recurring.Until(recurring.AtInterval(kNow, 2*time.Hour), kNow),
		recurring.Until(
			recurring.AtInterval(kNow, 3*time.Hour), kNow.Add(9*time.Hour)))
	verifyTimes(
		t,
		r.ForTime(kNow),
		kNow.Add(3*time.Hour),
		kNow.Add(6*time.Hour))
}

func TestCombineZeroOrOne(t *testing.T) {
	r := intRecurring(5)
	if recurring.Combine() != recurring.Nil() {
		t.Error("Expect combining no recurrings to be nil recurring.")
	}
	if recurring.Combine(r) != r {
		t.Error("Expected combine of single recurring to be that recurring.")
	}
}

func TestDrPepper(t *testing.T) {
	r := recurring.Until(
		recurring.Combine(
			recurring.AtTime(10, 0),
			recurring.AtTime(14, 0),
			recurring.AtTime(16, 0)),
		kNow.AddDate(0, 0, 2))
	verifyTimes(
		t,
		r.ForTime(kNow),
		time.Date(2013, 9, 13, 10, 0, 0, 0, time.Local),
		time.Date(2013, 9, 13, 14, 0, 0, 0, time.Local),
		time.Date(2013, 9, 13, 16, 0, 0, 0, time.Local),
		time.Date(2013, 9, 14, 10, 0, 0, 0, time.Local),
		time.Date(2013, 9, 14, 14, 0, 0, 0, time.Local),
		time.Date(2013, 9, 14, 16, 0, 0, 0, time.Local))
}

func TestStartAtUntil(t *testing.T) {
	hourly := recurring.OnTheHour()
	thanksgiving10 := time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local)
	thanksgiving10and11 := recurring.Until(
		recurring.StartAt(hourly, thanksgiving10),
		thanksgiving10.Add(time.Hour*2))
	verifyTimes(
		t,
		thanksgiving10and11.ForTime(kNow),
		time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local))
	verifyTimes(
		t,
		thanksgiving10and11.ForTime(thanksgiving10),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local))
	thanksgiving11and12 := recurring.Until(
		recurring.StartAt(hourly, thanksgiving10.Add(time.Nanosecond)),
		thanksgiving10.Add(time.Nanosecond).Add(time.Hour*2))
	verifyTimes(
		t,
		thanksgiving11and12.ForTime(kNow),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 12, 0, 0, 0, time.Local))
}

func TestStartAtUntilNested(t *testing.T) {
	hourly := recurring.OnTheHour()
	thanksgiving9 := time.Date(2013, 11, 28, 9, 0, 0, 0, time.Local)
	thanksgiving10 := time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local)
	thanksgiving12 := time.Date(2013, 11, 28, 12, 0, 0, 0, time.Local)
	thanksgiving13 := time.Date(2013, 11, 28, 13, 0, 0, 0, time.Local)
	thanksgiving10and11 := recurring.Until(
		recurring.StartAt(
			recurring.Until(
				recurring.StartAt(hourly, thanksgiving10),
				thanksgiving13),
			thanksgiving9),
		thanksgiving12)
	verifyTimes(
		t,
		thanksgiving10and11.ForTime(kNow),
		time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local))
	thanksgiving10and11again := recurring.Until(
		recurring.StartAt(
			recurring.Until(
				recurring.StartAt(hourly, thanksgiving9),
				thanksgiving12),
			thanksgiving10),
		thanksgiving13)
	verifyTimes(
		t,
		thanksgiving10and11again.ForTime(kNow),
		time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local))
	thanksgiving9On := recurring.StartAt(hourly, thanksgiving9)
	verifyTimes(
		t,
		functional.Slice(thanksgiving9On.ForTime(kNow), 0, 3),
		time.Date(2013, 11, 28, 9, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 10, 0, 0, 0, time.Local),
		time.Date(2013, 11, 28, 11, 0, 0, 0, time.Local))
}

func verifyTimes(t *testing.T, s functional.Stream, expectedTimes ...time.Time) {
	var actual time.Time
	for _, expected := range expectedTimes {
		if s.Next(&actual) != nil {
			t.Error("Stream too short.")
			return
		}
		if actual != expected {
			t.Errorf("Expected %v, got %v", expected, actual)
		}
	}
	if s.Next(&actual) != functional.Done {
		t.Error("Stream too long.")
	}
}

type intRecurring int

func (i intRecurring) ForTime(t time.Time) functional.Stream {
	return functional.NilStream()
}
